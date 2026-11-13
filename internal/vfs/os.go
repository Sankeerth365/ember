package vfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
)

// OS implements FS using the Linux filesystem and advisory flock locks.
type OS struct{}

var _ FS = OS{}

// Mkdir creates a single directory.
func (OS) Mkdir(path string, perm fs.FileMode) error { return os.Mkdir(path, perm) }

// OpenFile opens a file with standard os flags.
func (OS) OpenFile(path string, flag int, perm fs.FileMode) (File, error) {
	file, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// CreateTemp creates an exclusive temporary file and returns its path.
func (OS) CreateTemp(dir, pattern string) (File, string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, "", err
	}
	return file, file.Name(), nil
}

// ReadDir returns directory entries in filename order.
func (OS) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }

// Rename atomically changes a directory entry on the same filesystem.
func (OS) Rename(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }

// Remove unlinks a path.
func (OS) Remove(path string) error { return os.Remove(path) }

// SyncDir makes directory-entry changes durable.
func (OS) SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Lock acquires a nonblocking exclusive lock. Leave the lock file in place
// after close so competing openers always lock the same inode.
func (OS) Lock(path string) (io.Closer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return nil, errors.Join(fmt.Errorf("lock %q: %w", path, err), file.Close())
		}
		return file, nil
	}
}
