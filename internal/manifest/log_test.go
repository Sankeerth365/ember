package manifest_test

import (
	"errors"
	"io/fs"
	"os"
	"testing"

	"ember/internal/manifest"
	"ember/internal/version"
	"ember/internal/vfs"
)

var initial = manifest.Edit{AddWALs: []uint64{2}, ActiveWAL: 2, NextFileNumber: 3}

func create(t *testing.T, model *vfs.FaultFS) *manifest.Log {
	t.Helper()
	log, err := manifest.Create(model, "/", 1, initial)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

func TestManifestCreationCrashCuts(t *testing.T) {
	for cut := 0; cut <= 10; cut++ {
		model := vfs.NewFaultFS()
		if err := vfs.Install(model, "/000002.wal", func(vfs.File) error { return nil }); err != nil {
			t.Fatal(err)
		}
		model.FailAfter(cut)
		_, err := manifest.Create(model, "/", 1, initial)
		if cut < 10 && !errors.Is(err, vfs.ErrInjected) || cut == 10 && err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		model.Crash()
		set := version.NewSet()
		log, _, err := manifest.Open(model, "/", func(r manifest.Record) error { return set.Replay(r.Edit) })
		if cut < 10 {
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("cut %d: %v", cut, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		pin := set.Acquire()
		if pin.Version().ActiveWAL() != 2 {
			t.Fatal("missing replay WAL")
		}
		pin.Release()
		log.Close()
	}
}

func TestManifestTailCutsAndChecksum(t *testing.T) {
	model := vfs.NewFaultFS()
	log := create(t, model)
	path := "/" + manifest.Filename(1)
	file, err := model.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	initialInfo, _ := file.Stat()
	if err := log.Append(manifest.Edit{LastSequence: 17}); err != nil {
		t.Fatal(err)
	}
	info, _ := file.Stat()
	all := make([]byte, int(info.Size()))
	if _, err := file.ReadAt(all, 0); err != nil {
		t.Fatal(err)
	}
	for end := int(initialInfo.Size()); end <= len(all); end++ {
		copyFS := vfs.NewFaultFS()
		log := create(t, copyFS)
		log.Close()
		if err := vfs.Install(copyFS, path, func(f vfs.File) error { _, e := f.Write(all[:end]); return e }); err != nil {
			t.Fatal(err)
		}
		copyFS.Crash()
		edits := 0
		open, repaired, err := manifest.Open(copyFS, "/", func(manifest.Record) error { edits++; return nil })
		if err != nil {
			t.Fatal(err)
		}
		if repaired != (end > int(initialInfo.Size()) && end < len(all)) || edits != 1+btoi(end == len(all)) {
			t.Fatalf("cut %d: repaired=%v edits=%d", end, repaired, edits)
		}
		if err := open.Append(manifest.Edit{LastSequence: 18}); err != nil {
			t.Fatal(err)
		}
		open.Close()
		copyFS.Crash()
		open, repaired, err = manifest.Open(copyFS, "/", nil)
		if err != nil || repaired {
			t.Fatalf("cut %d: second reopen %v %v", end, repaired, err)
		}
		open.Close()
	}
	all[len(all)-1] ^= 1
	log.Close()
	if err := vfs.Install(model, path, func(f vfs.File) error { _, e := f.Write(all); return e }); err != nil {
		t.Fatal(err)
	}
	if _, repaired, err := manifest.Open(model, "/", nil); repaired || !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("bad CRC: %v %v", repaired, err)
	}
	after, err := model.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, _ := after.Stat()
	if afterInfo.Size() != int64(len(all)) {
		t.Fatal("corrupt complete record was truncated")
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestMissingManifestTarget(t *testing.T) {
	model := vfs.NewFaultFS()
	create(t, model).Close()
	if err := model.Remove("/" + manifest.Filename(1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manifest.Open(model, "/", nil); !errors.Is(err, manifest.ErrCorrupt) || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("dangling CURRENT: %v", err)
	}
}
