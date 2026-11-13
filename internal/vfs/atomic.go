package vfs

import (
	"errors"
	"fmt"
	"path/filepath"
)

// Install writes and syncs a sibling temporary file, closes it, renames it
// into place, then syncs its directory. Callers may publish a reference only
// after Install succeeds. The callback must not close the file.
func Install(fsys FS, path string, write func(File) error) error {
	dir := filepath.Dir(path)
	file, temp, err := fsys.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file for %q: %w", path, err)
	}
	cleanup := func(cause error) error {
		return fmt.Errorf("install %q: %w", path, errors.Join(cause, fsys.Remove(temp)))
	}

	if err := write(file); err != nil {
		return cleanup(errors.Join(err, file.Close()))
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return cleanup(err)
	}
	if err := fsys.Rename(temp, path); err != nil {
		return cleanup(err)
	}
	// The rename may already be durable when SyncDir returns an error.
	// Leave the destination for recovery, which has the authoritative references.
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("sync directory after installing %q: %w", path, err)
	}
	return nil
}
