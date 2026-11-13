package ember_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"ember/ember"
	"ember/internal/vfs"
)

func opts(f vfs.FS) *ember.Options {
	return &ember.Options{FS: f, MemtableBytes: 2048, L0Trigger: 2, L0Slowdown: 3, L0Stop: 4, BaseLevelBytes: 4096, TargetTableBytes: 2048, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func open(t *testing.T, path string, opts *ember.Options) *ember.DB {
	t.Helper()
	db, e := ember.Open(path, opts)
	if e != nil {
		t.Fatal(e)
	}
	return db
}

func get(t *testing.T, db interface{ Get([]byte) ([]byte, error) }, k, want string) {
	t.Helper()
	got, e := db.Get([]byte(k))
	if e != nil || string(got) != want {
		t.Fatalf("%q: got %q %v want %q", k, got, e, want)
	}
}

func missing(t *testing.T, db interface{ Get([]byte) ([]byte, error) }, k string) {
	t.Helper()
	if _, e := db.Get([]byte(k)); !errors.Is(e, ember.ErrNotFound) {
		t.Fatalf("%q: %v", k, e)
	}
}

func TestSnapshotExpiryResetAndReopen(t *testing.T) {
	f := vfs.NewFaultFS()
	o := opts(f)
	var now atomic.Int64
	now.Store(1000)
	o.Clock = func() time.Time { return time.Unix(0, now.Load()) }
	d := open(t, "/db", o)
	if e := d.Put([]byte("aa"), []byte("old")); e != nil {
		t.Fatal(e)
	}
	var b ember.Batch
	b.PutWithExpiry([]byte("aa"), []byte("temporary"), time.Unix(0, 2000))
	b.Put([]byte("bb"), []byte("two"))
	b.Put([]byte("cc"), []byte("three"))
	if e := d.Write(&b); e != nil {
		t.Fatal(e)
	}
	s, e := d.NewSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	i, e := s.NewIterator(ember.IteratorOptions{Lower: []byte("aa"), Upper: []byte("cc")})
	if e != nil {
		t.Fatal(e)
	}
	now.Store(3000)
	missing(t, d, "aa")
	get(t, s, "aa", "temporary")
	if e := d.Reset(); e != nil {
		t.Fatal(e)
	}
	missing(t, d, "bb")
	get(t, s, "bb", "two")
	s.Close()
	for _, want := range []string{"aa", "bb"} {
		if !i.Next() || string(i.Key()) != want {
			t.Fatalf("iterator %q %v", i.Key(), i.Err())
		}
	}
	if i.Next() || i.Err() != nil {
		t.Fatalf("iterator end %v", i.Err())
	}
	i.Close()
	if e := d.Put(nil, []byte("empty-key")); e != nil {
		t.Fatal(e)
	}
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	f.Crash()
	d = open(t, "/db", o)
	defer d.Close()
	get(t, d, "", "empty-key")
	missing(t, d, "bb")
}

func TestPinnedDiskIteratorAndTombstone(t *testing.T) {
	d := open(t, t.TempDir(), opts(nil))
	defer d.Close()
	value := string(bytes.Repeat([]byte{'o'}, 3000))
	if err := d.Put([]byte("item"), []byte(value)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for d.Stats().Flushes < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d.Stats().Flushes == 0 {
		t.Fatal("expected first value on disk")
	}
	s, err := d.NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	it, err := s.NewIterator(ember.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := d.Delete([]byte("item")); err != nil {
		t.Fatal(err)
	}
	for n := range 8 {
		if err := d.Put([]byte(fmt.Sprintf("z%d", n)), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if d.Stats().Compactions == 0 {
		t.Fatal("expected compaction while iterator was pinned")
	}
	missing(t, d, "item")
	if !it.Next() || string(it.Key()) != "item" || string(it.Value()) != value {
		t.Fatalf("pinned iterator: %q %v", it.Key(), it.Err())
	}
	it.Close()
}

func TestAPIInvalidInputsAndClose(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ember.Options
	}{
		{"memtable", ember.Options{MemtableBytes: -1}},
		{"sync_policy", ember.Options{SyncPolicy: 9}},
		{"bloom", ember.Options{BloomBitsPerKey: 1 << 20}},
		{"levels", ember.Options{Levels: 1}},
		{"pressure", ember.Options{L0Trigger: 4, L0Slowdown: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if d, err := ember.Open(t.TempDir(), &tc.opts); err == nil {
				d.Close()
				t.Fatal("invalid options accepted")
			}
		})
	}
	if d, err := ember.Open("", nil); err == nil {
		d.Close()
		t.Fatal("empty path accepted")
	}
	d := open(t, t.TempDir(), opts(nil))
	defer d.Close()
	if err := d.Write(nil); err == nil {
		t.Fatal("nil batch accepted")
	}
	if err := d.Write(&ember.Batch{}); err != nil {
		t.Fatal(err)
	}
	if err := d.Update(nil); err == nil {
		t.Fatal("nil callback accepted")
	}
	if err := d.Put(make([]byte, 1<<20+1), nil); err == nil {
		t.Fatal("oversize key accepted")
	}
	abort := errors.New("cancel update")
	if err := d.Update(func(tx *ember.Tx) error {
		tx.Put([]byte("abort"), []byte("staged"), time.Time{})
		tx.Delete([]byte("abort"))
		if _, err := tx.Get([]byte("abort")); !errors.Is(err, ember.ErrNotFound) {
			return fmt.Errorf("staged delete: %v", err)
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	missing(t, d, "abort")
	if it, err := d.NewIterator(ember.IteratorOptions{Lower: []byte("z"), Upper: []byte("a")}); err == nil {
		it.Close()
		t.Fatal("inverted bounds accepted")
	}
	s, err := d.NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	it, err := s.NewIterator(ember.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	defer s.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(nil); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("get after close: %v", err)
	}
	if _, err := s.Get(nil); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("snapshot after close: %v", err)
	}
	if _, err := s.NewIterator(ember.IteratorOptions{}); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("snapshot iterator after close: %v", err)
	}
	if _, err := d.NewSnapshot(); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("snapshot after close: %v", err)
	}
	if _, err := d.NewIterator(ember.IteratorOptions{}); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("iterator after close: %v", err)
	}
	if it.Next() || !errors.Is(it.Err(), ember.ErrClosed) {
		t.Fatalf("next after close: %v", it.Err())
	}
	if err := d.Put(nil, nil); !errors.Is(err, ember.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestOutOfRangeExpiryRejectsWholeBatch(t *testing.T) {
	db := open(t, t.TempDir(), nil)
	defer db.Close()
	var b ember.Batch
	b.Put([]byte("first"), []byte("value"))
	b.PutWithExpiry([]byte("second"), []byte("value"), time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := db.Write(&b); err == nil {
		t.Fatal("out-of-range expiry accepted")
	}
	missing(t, db, "first")
	missing(t, db, "second")
}
