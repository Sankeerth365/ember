package ember_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"ember/ember"
	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/vfs"
	"ember/internal/wal"
)

func TestAcknowledgedWALSurvivesFault(t *testing.T) {
	for cut := range 3 {
		f := vfs.NewFaultFS()
		o := opts(f)
		o.MemtableBytes = 1 << 20
		d := open(t, "/db", o)
		if e := d.Put([]byte("durable"), []byte("yes")); e != nil {
			t.Fatal(e)
		}
		f.FailAfter(cut)
		e := d.Put([]byte("next"), []byte("maybe"))
		if cut < 2 && !errors.Is(e, vfs.ErrInjected) || cut == 2 && e != nil {
			t.Fatalf("cut %d: %v", cut, e)
		}
		_ = d.Close()
		f.Crash()
		d = open(t, "/db", o)
		get(t, d, "durable", "yes")
		if cut == 2 {
			get(t, d, "next", "maybe")
		} else {
			missing(t, d, "next")
		}
		if e := d.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestRotationAndFlushCrashCuts(t *testing.T) {
	for cut := range 26 {
		f := vfs.NewFaultFS()
		o := opts(f)
		o.MemtableBytes = 900
		d := open(t, "/db", o)
		if err := d.Put([]byte("old"), []byte("durable")); err != nil {
			t.Fatal(err)
		}
		f.FailAfter(cut)
		payload := bytes.Repeat([]byte{'x'}, 1200)
		putErr := d.Put([]byte("large"), payload)
		// Queue another write so rotation and backpressure can run before close.
		markerErr := d.Put([]byte("marker"), []byte("after"))
		_ = d.Close()
		f.Crash()
		d = open(t, "/db", o)
		get(t, d, "old", "durable")
		if putErr == nil {
			get(t, d, "large", string(payload))
		}
		if markerErr == nil {
			get(t, d, "marker", "after")
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReopenWithTornOrCorruptFinalBatch(t *testing.T) {
	entries := []skiplist.Entry{
		{Key: skiplist.Key{User: []byte("one"), Seq: 2, Kind: skiplist.Put}, Value: []byte("new")},
		{Key: skiplist.Key{User: []byte("two"), Seq: 3, Kind: skiplist.Put}, Value: []byte("value")},
	}
	record, err := wal.EncodeBatch(entries)
	if err != nil {
		t.Fatal(err)
	}
	var framed bytes.Buffer
	w, err := wal.NewWriter(&framed, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(record); err != nil {
		t.Fatal(err)
	}
	check := func(t *testing.T, tail []byte, complete bool) {
		t.Helper()
		fsys := vfs.NewFaultFS()
		o := opts(fsys)
		d := open(t, "/db", o)
		if err := d.Put([]byte("one"), []byte("old")); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := fsys.OpenFile("/db/000002.wal", os.O_APPEND|os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(tail); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		fsys.Crash()
		d = open(t, "/db", o)
		defer d.Close()
		if complete {
			get(t, d, "one", "new")
			get(t, d, "two", "value")
		} else {
			get(t, d, "one", "old")
			missing(t, d, "two")
		}
		if err := d.Put([]byte("after"), []byte("works")); err != nil {
			t.Fatal(err)
		}
	}
	for cut := range framed.Len() + 1 {
		t.Run(fmt.Sprintf("cut_%d", cut), func(t *testing.T) { check(t, framed.Bytes()[:cut], cut == framed.Len()) })
	}
	r := rand.New(rand.NewSource(712))
	for i := range 40 {
		t.Run(fmt.Sprintf("bit_flip_%d", i), func(t *testing.T) {
			p := bytes.Clone(framed.Bytes())
			p[r.Intn(len(p))] ^= 1 << r.Intn(8)
			check(t, p, false)
		})
	}
}

func TestPreviouslySyncedBatchSurvivesDroppedSync(t *testing.T) {
	fsys := vfs.NewFaultFS()
	d := open(t, "/db", opts(fsys))
	if err := d.Put([]byte("durable"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	fsys.DropSyncAfter(0)
	if err := d.Put([]byte("volatile"), []byte("no")); err != nil {
		t.Fatal(err)
	}
	// The fault model reported sync success without persisting the second
	// batch. Reject the clean-close sync to preserve that crash state.
	fsys.FailAfter(0)
	if err := d.Close(); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("close: %v", err)
	}
	fsys.Crash()
	d = open(t, "/db", opts(fsys))
	defer d.Close()
	get(t, d, "durable", "yes")
	missing(t, d, "volatile")
}

type syncObserver struct {
	vfs.FS
	done chan struct{}
}

func (o syncObserver) OpenFile(path string, flag int, mode fs.FileMode) (vfs.File, error) {
	f, err := o.FS.OpenFile(path, flag, mode)
	if err != nil || !strings.HasSuffix(path, ".wal") {
		return f, err
	}
	return observedFile{f, o.done}, nil
}

type observedFile struct {
	vfs.File
	done chan struct{}
}

func (f observedFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	select {
	case f.done <- struct{}{}:
	default:
	}
	return nil
}

func TestPeriodicSyncRunsWithIdleWriter(t *testing.T) {
	fsys := vfs.NewFaultFS()
	observer := syncObserver{fsys, make(chan struct{}, 1)}
	o := opts(observer)
	o.SyncPolicy = ember.SyncPeriodic
	o.SyncInterval = 2 * time.Millisecond
	d := open(t, "/db", o)
	if err := d.Put([]byte("periodic"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observer.done:
	case <-time.After(3 * time.Second):
		t.Fatal("no periodic sync while idle")
	}
	fsys.FailAfter(0)
	if err := d.Close(); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("close: %v", err)
	}
	fsys.Crash()
	d = open(t, "/db", o)
	defer d.Close()
	get(t, d, "periodic", "durable")
}

func TestReopenSyncsRecoveredMetadataBeforeAcknowledging(t *testing.T) {
	for _, stage := range []string{"current_directory_sync", "manifest_append_sync"} {
		t.Run(stage, func(t *testing.T) {
			fsys := vfs.NewFaultFS()
			o := opts(fsys)
			o.MemtableBytes = 1 << 20
			dir := "/db"
			install := func(name string) {
				t.Helper()
				if err := vfs.Install(fsys, dir+"/"+name, func(vfs.File) error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			switch stage {
			case "current_directory_sync":
				if err := fsys.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := fsys.SyncDir("/"); err != nil {
					t.Fatal(err)
				}
				install("000002.wal")
				fsys.FailAfter(9)
				_, err := manifest.Create(fsys, dir, 1, manifest.Edit{
					AddWALs: []uint64{2}, ActiveWAL: 2, NextFileNumber: 3,
				})
				if !errors.Is(err, vfs.ErrInjected) {
					t.Fatalf("initial CURRENT sync: %v", err)
				}
			case "manifest_append_sync":
				db := open(t, dir, o)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				install("000003.wal")
				log, _, err := manifest.Open(fsys, dir, nil)
				if err != nil {
					t.Fatal(err)
				}
				fsys.FailAfter(1)
				err = log.Append(manifest.Edit{AddWALs: []uint64{3}, ActiveWAL: 3, NextFileNumber: 4})
				if !errors.Is(err, vfs.ErrInjected) {
					t.Fatalf("WAL rotation edit sync: %v", err)
				}
				if err := log.Close(); err != nil {
					t.Fatal(err)
				}
			}
			fsys.FailAfter(-1)
			db := open(t, dir, o)
			if err := db.Put([]byte("acknowledged"), []byte("durable")); err != nil {
				t.Fatal(err)
			}
			// Prevent close from adding a persistence barrier to the test.
			fsys.FailAfter(0)
			if err := db.Close(); !errors.Is(err, vfs.ErrInjected) {
				t.Fatalf("close: %v", err)
			}
			fsys.Crash()
			db = open(t, dir, o)
			defer db.Close()
			get(t, db, "acknowledged", "durable")
		})
	}
}
