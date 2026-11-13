// Package vfs defines the filesystem operations used by the storage engine.
package vfs

import (
	"io"
	"io/fs"
)

// File is a seekable storage file. ReadAt does not change the write offset.
type File interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Stat() (fs.FileInfo, error)
	Truncate(size int64) error
	Sync() error
}

// FS exposes file contents and directory entries as separate durability domains.
type FS interface {
	Mkdir(path string, perm fs.FileMode) error
	OpenFile(path string, flag int, perm fs.FileMode) (File, error)
	CreateTemp(dir, pattern string) (File, string, error)
	ReadDir(path string) ([]fs.DirEntry, error)
	Rename(oldPath, newPath string) error
	Remove(path string) error
	SyncDir(path string) error
	Lock(path string) (io.Closer, error)
}
