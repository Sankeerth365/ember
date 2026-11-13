package vfs

import (
	"io"
	"io/fs"
	"math"
	"os"
	"slices"
	"time"
)

type faultFile struct {
	fs         *FaultFS
	node       *inode
	name       string
	flag       int
	offset     int
	generation uint64
	closed     bool
}

var _ File = (*faultFile)(nil)

func (h *faultFile) alive() bool {
	return !h.closed && h.generation == h.fs.generation
}

func (h *faultFile) ReadAt(buf []byte, offset int64) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return 0, fs.ErrClosed
	}
	if offset < 0 || h.flag&os.O_WRONLY != 0 {
		return 0, fs.ErrInvalid
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if offset >= int64(len(h.node.data)) {
		return 0, io.EOF
	}
	n := copy(buf, h.node.data[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (h *faultFile) Write(buf []byte) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return 0, fs.ErrClosed
	}
	if h.flag&(os.O_RDWR|os.O_WRONLY) == 0 {
		return 0, fs.ErrPermission
	}
	if len(buf) == 0 {
		return 0, nil
	}
	offset := h.offset
	if h.flag&os.O_APPEND != 0 {
		offset = len(h.node.data)
	}
	if len(buf) > math.MaxInt-offset {
		return 0, fs.ErrInvalid
	}
	if err := h.fs.allowMutation(); err != nil {
		return 0, err
	}
	end := offset + len(buf)
	if end > len(h.node.data) {
		h.node.resize(end)
	}
	copy(h.node.data[offset:end], buf)
	h.offset = end
	return len(buf), nil
}

func (h *faultFile) Stat() (fs.FileInfo, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return nil, fs.ErrClosed
	}
	return h.node.info(h.name), nil
}

func (h *faultFile) Truncate(size int64) error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return fs.ErrClosed
	}
	if h.flag&(os.O_RDWR|os.O_WRONLY) == 0 {
		return fs.ErrPermission
	}
	if size < 0 || size > int64(math.MaxInt) {
		return fs.ErrInvalid
	}
	if err := h.fs.allowMutation(); err != nil {
		return err
	}
	h.node.resize(int(size))
	return nil
}

func (n *inode) resize(size int) {
	if extra := size - len(n.data); extra > 0 {
		n.data = append(n.data, make([]byte, extra)...)
	} else {
		n.data = n.data[:size]
	}
}

func (h *faultFile) Sync() error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return fs.ErrClosed
	}
	if persist, err := h.fs.allowSync(); err != nil {
		return err
	} else if persist {
		h.node.syncedData = slices.Clone(h.node.data)
	}
	return nil
}

func (h *faultFile) Close() error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if !h.alive() {
		return fs.ErrClosed
	}
	h.closed = true
	return nil
}

type fileInfo struct {
	name string
	mode fs.FileMode
	size int64
}

func (n *inode) info(name string) fileInfo {
	return fileInfo{name: name, mode: n.mode, size: int64(len(n.data))}
}

func (i fileInfo) Name() string               { return i.name }
func (i fileInfo) Mode() fs.FileMode          { return i.mode }
func (i fileInfo) Type() fs.FileMode          { return i.mode.Type() }
func (i fileInfo) Size() int64                { return i.size }
func (i fileInfo) ModTime() time.Time         { return time.Time{} }
func (i fileInfo) IsDir() bool                { return i.mode.IsDir() }
func (i fileInfo) Sys() any                   { return nil }
func (i fileInfo) Info() (fs.FileInfo, error) { return i, nil }
