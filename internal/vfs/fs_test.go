package vfs_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"ember/internal/vfs"
)

func install(fsys vfs.FS, path, value string) error {
	return vfs.Install(fsys, path, func(file vfs.File) error {
		_, err := file.Write([]byte(value))
		return err
	})
}

func TestInstallCrashCuts(t *testing.T) {
	for cut := 0; cut <= 5; cut++ {
		fsys := vfs.NewFaultFS()
		if err := install(fsys, "/CURRENT", "old"); err != nil {
			t.Fatal(err)
		}
		fsys.FailAfter(cut)
		err := install(fsys, "/CURRENT", "new")
		if (cut < 5 && !errors.Is(err, vfs.ErrInjected)) || (cut == 5 && err != nil) {
			t.Fatalf("cut %d: %v", cut, err)
		}
		fsys.Crash()
		file, err := fsys.OpenFile("/CURRENT", os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 3)
		if _, err := file.ReadAt(buf, 0); err != nil {
			t.Fatal(err)
		}
		want := "old"
		if cut == 5 {
			want = "new"
		}
		if string(buf) != want {
			t.Fatalf("cut %d: got %q want %q", cut, buf, want)
		}
	}
}

func TestDroppedSyncs(t *testing.T) {
	fsys := vfs.NewFaultFS()
	fsys.DropSyncAfter(0)
	if err := install(fsys, "/table", "data"); err != nil {
		t.Fatal(err)
	}
	fsys.Crash()
	file, err := fsys.OpenFile("/table", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != 0 {
		t.Fatalf("dropped data sync: %v %v", info, err)
	}
	fsys.DropSyncAfter(1)
	if err := install(fsys, "/table2", "data"); err != nil {
		t.Fatal(err)
	}
	fsys.Crash()
	if _, err := fsys.OpenFile("/table2", os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("dropped directory sync: %v", err)
	}
	if _, err := file.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("stale handle: %v", err)
	}
}

func TestOSInstallAndLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "CURRENT")
	fsys := vfs.OS{}
	lock, err := fsys.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if second, err := fsys.Lock(filepath.Join(dir, "LOCK")); err == nil {
		second.Close()
		t.Fatal("a second opener acquired the lock")
	}
	if err := install(fsys, path, "manifest"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "manifest" {
		t.Fatalf("got %q: %v", data, err)
	}
}
