package vfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// ErrInjected identifies a mutation rejected by the fault filesystem.
var ErrInjected = errors.New("injected filesystem failure")

// FaultFS models volatile file bytes and directory entries independently from
// their synced copies. Use NewFaultFS to construct it.
type FaultFS struct {
	mu         sync.Mutex
	root       *inode
	locks      map[string]bool
	generation uint64
	tempID     uint64
	failAfter  int
	dropAfter  int
}

type inode struct {
	mode        fs.FileMode
	data        []byte
	syncedData  []byte
	links       map[string]*inode
	syncedLinks map[string]*inode
}

var _ FS = (*FaultFS)(nil)

// NewFaultFS returns a filesystem with an empty, durable root directory.
func NewFaultFS() *FaultFS {
	return &FaultFS{
		root:      newDirectory(0755),
		locks:     make(map[string]bool),
		failAfter: -1,
		dropAfter: -1,
	}
}

func newDirectory(perm fs.FileMode) *inode {
	return &inode{mode: perm.Perm() | fs.ModeDir, links: make(map[string]*inode)}
}

// FailAfter lets n mutations through, then rejects subsequent mutations
// until Crash or reconfiguration. A negative value disarms the fault.
func (f *FaultFS) FailAfter(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter = n
}

// DropSyncAfter skips persistence on the sync after n successful sync calls.
// That call still returns nil. A negative value disarms the fault.
func (f *FaultFS) DropSyncAfter(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropAfter = n
}

// Crash discards unsynced state, releases locks, and invalidates open handles.
func (f *FaultFS) Crash() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.root.restore()
	clear(f.locks)
	f.generation++
	f.failAfter, f.dropAfter = -1, -1
}

func (n *inode) restore() {
	if !n.mode.IsDir() {
		n.data = slices.Clone(n.syncedData)
		return
	}
	n.links = copyLinks(n.syncedLinks)
	for _, child := range n.links {
		child.restore()
	}
}

func copyLinks(links map[string]*inode) map[string]*inode {
	copied := make(map[string]*inode, len(links))
	for name, node := range links {
		copied[name] = node
	}
	return copied
}

// allowMutation and allowSync run with mu held.
func (f *FaultFS) allowMutation() error {
	if f.failAfter == 0 {
		return ErrInjected
	}
	if f.failAfter > 0 {
		f.failAfter--
	}
	return nil
}

func (f *FaultFS) allowSync() (bool, error) {
	if err := f.allowMutation(); err != nil {
		return false, err
	}
	if f.dropAfter == 0 {
		f.dropAfter = -1
		return false, nil
	}
	if f.dropAfter > 0 {
		f.dropAfter--
	}
	return true, nil
}

func absolute(path string) string {
	return filepath.Clean(string(filepath.Separator) + path)
}

func (f *FaultFS) lookup(path string) (*inode, error) {
	node := f.root
	if path == "/" {
		return node, nil
	}
	for _, name := range strings.Split(path[1:], "/") {
		if !node.mode.IsDir() {
			return nil, fs.ErrInvalid
		}
		node = node.links[name]
		if node == nil {
			return nil, fs.ErrNotExist
		}
	}
	return node, nil
}

func (f *FaultFS) parent(path string) (*inode, string, error) {
	if path == "/" {
		return nil, "", fs.ErrInvalid
	}
	dir, err := f.lookup(filepath.Dir(path))
	if err == nil && !dir.mode.IsDir() {
		err = fs.ErrInvalid
	}
	return dir, filepath.Base(path), err
}

// Mkdir creates a directory. Sync its parent to persist its name.
func (f *FaultFS) Mkdir(path string, perm fs.FileMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name, err := f.parent(absolute(path))
	if err != nil {
		return err
	}
	if dir.links[name] != nil {
		return fs.ErrExist
	}
	if err := f.allowMutation(); err != nil {
		return err
	}
	dir.links[name] = newDirectory(perm)
	return nil
}

// OpenFile opens a regular file using the engine's os open flags.
func (f *FaultFS) OpenFile(path string, flag int, perm fs.FileMode) (File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	handle, err := f.open(absolute(path), flag, perm)
	if err != nil {
		return nil, err
	}
	return handle, nil
}

func (f *FaultFS) open(path string, flag int, perm fs.FileMode) (*faultFile, error) {
	dir, name, err := f.parent(path)
	if err != nil {
		return nil, err
	}
	node := dir.links[name]
	if node == nil {
		if flag&os.O_CREATE == 0 {
			return nil, fs.ErrNotExist
		}
		if err := f.allowMutation(); err != nil {
			return nil, err
		}
		node = &inode{mode: perm.Perm()}
		dir.links[name] = node
	} else {
		if flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL {
			return nil, fs.ErrExist
		}
		if node.mode.IsDir() {
			return nil, fs.ErrInvalid
		}
		if flag&os.O_TRUNC != 0 {
			if err := f.allowMutation(); err != nil {
				return nil, err
			}
			node.data = nil
		}
	}
	return &faultFile{fs: f, node: node, name: name, flag: flag, generation: f.generation}, nil
}

// CreateTemp creates an exclusive temporary regular file.
func (f *FaultFS) CreateTemp(dir, pattern string) (File, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.ContainsRune(pattern, filepath.Separator) {
		return nil, "", fs.ErrInvalid
	}
	for {
		f.tempID++
		suffix := fmt.Sprint(f.tempID)
		name := pattern + suffix
		if star := strings.LastIndexByte(pattern, '*'); star >= 0 {
			name = pattern[:star] + suffix + pattern[star+1:]
		}
		path := filepath.Join(dir, name)
		file, err := f.open(absolute(path), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return file, path, nil
	}
}

// ReadDir returns directory entries in filename order.
func (f *FaultFS) ReadDir(path string) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, err := f.lookup(absolute(path))
	if err != nil {
		return nil, err
	}
	if !dir.mode.IsDir() {
		return nil, fs.ErrInvalid
	}
	names := make([]string, 0, len(dir.links))
	for name := range dir.links {
		names = append(names, name)
	}
	slices.Sort(names)
	entries := make([]fs.DirEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, dir.links[name].info(name))
	}
	return entries, nil
}

// Rename changes a regular file's name within one directory.
func (f *FaultFS) Rename(oldPath, newPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	oldPath, newPath = absolute(oldPath), absolute(newPath)
	dir, oldName, err := f.parent(oldPath)
	if err != nil {
		return err
	}
	if filepath.Dir(oldPath) != filepath.Dir(newPath) {
		return fs.ErrInvalid
	}
	node := dir.links[oldName]
	if node == nil {
		return fs.ErrNotExist
	}
	newName := filepath.Base(newPath)
	existing := dir.links[newName]
	if node.mode.IsDir() || (existing != nil && existing.mode.IsDir()) {
		return fs.ErrInvalid
	}
	if oldName == newName {
		return nil
	}
	if err := f.allowMutation(); err != nil {
		return err
	}
	delete(dir.links, oldName)
	dir.links[newName] = node
	return nil
}

// Remove unlinks a regular file or an empty directory.
func (f *FaultFS) Remove(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name, err := f.parent(absolute(path))
	if err != nil {
		return err
	}
	node := dir.links[name]
	if node == nil {
		return fs.ErrNotExist
	}
	if len(node.links) != 0 {
		return fs.ErrInvalid
	}
	if err := f.allowMutation(); err != nil {
		return err
	}
	delete(dir.links, name)
	return nil
}

// SyncDir persists the directory's current names and inode references.
func (f *FaultFS) SyncDir(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, err := f.lookup(absolute(path))
	if err != nil {
		return err
	}
	if !dir.mode.IsDir() {
		return fs.ErrInvalid
	}
	if persist, err := f.allowSync(); err != nil {
		return err
	} else if persist {
		dir.syncedLinks = copyLinks(dir.links)
	}
	return nil
}

// Lock acquires an exclusive nonblocking lock for the engine's lock path.
func (f *FaultFS) Lock(path string) (io.Closer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path = absolute(path)
	if f.locks[path] {
		return nil, fmt.Errorf("lock %q is held", path)
	}
	if _, err := f.open(path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return nil, err
	}
	f.locks[path] = true
	return &faultLock{fs: f, path: path, generation: f.generation}, nil
}

type faultLock struct {
	fs         *FaultFS
	path       string
	generation uint64
	closed     bool
}

func (l *faultLock) Close() error {
	l.fs.mu.Lock()
	defer l.fs.mu.Unlock()
	if l.closed || l.generation != l.fs.generation {
		return fs.ErrClosed
	}
	delete(l.fs.locks, l.path)
	l.closed = true
	return nil
}
