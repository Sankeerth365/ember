// Package ember implements an embedded, ordered, durable key-value store.
package ember

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"ember/internal/cache"
	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/sstable"
	"ember/internal/version"
	"ember/internal/vfs"
	"ember/internal/wal"
)

// ErrNotFound indicates a missing, deleted, or expired key.
var ErrNotFound = errors.New("ember: key not found")

// ErrClosed indicates that the database or read handle has been closed.
var ErrClosed = errors.New("ember: closed")

type memtable struct {
	number uint64
	list   *skiplist.List
}
type tableFile struct {
	file  vfs.File
	table *sstable.Table
}

// DB is safe for concurrent use. Open takes an exclusive filesystem lock.
type DB struct {
	dir      string
	opts     Options
	lock     io.Closer
	log      *manifest.Log
	versions *version.Set
	cache    *cache.Cache

	// editMu serializes structural commits. mu publishes a version and
	// its memtables together. Disk I/O never runs while mu is held.
	editMu                         sync.Mutex
	mu                             sync.Mutex
	changed                        *sync.Cond
	current                        *version.Pin
	active                         memtable
	immutable                      []memtable // oldest first
	tables                         map[uint64]tableFile
	views                          map[*readView]struct{}
	sequence, nextFile, generation uint64
	fatal                          error
	clockHigh                      int64

	// The writer owns the active append handle. Admission closes before
	// joining the writer, and maintenance stops after the writer drains.
	walFile                                             vfs.File
	walWriter                                           *wal.Writer
	queue                                               chan request
	wake, writerWake, stop, writerDone, maintenanceDone chan struct{}
	admit                                               sync.RWMutex
	closing                                             bool

	life      sync.RWMutex
	dead      bool
	closeOnce sync.Once
	closeErr  error
	counters  counters
}

// Open opens or creates a database at path.
func Open(path string, opts *Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("empty Ember database path")
	}
	o, err := options(opts)
	if err != nil {
		return nil, err
	}
	d := &DB{
		dir: path, opts: o, versions: version.NewSet(), cache: cache.New(o.CacheBytes, 16),
		tables: make(map[uint64]tableFile), views: make(map[*readView]struct{}),
		queue: make(chan request, 128), wake: make(chan struct{}, 1), writerWake: make(chan struct{}, 1), stop: make(chan struct{}),
		clockHigh:  math.MinInt64,
		writerDone: make(chan struct{}), maintenanceDone: make(chan struct{}),
	}
	d.changed = sync.NewCond(&d.mu)
	if err := d.recover(); err != nil {
		return nil, errors.Join(fmt.Errorf("open Ember at %q: %w", path, err), d.closeFiles())
	}
	go d.maintain()
	go d.writeLoop()
	d.signal()
	d.signalWriter()
	return d, nil
}

func (d *DB) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DB) fail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fatal == nil {
		d.fatal = err
		d.changed.Broadcast()
	}
}

func (d *DB) begin() bool {
	d.life.RLock()
	if d.dead {
		d.life.RUnlock()
		return false
	}
	return true
}

// Close drains accepted writes and invalidates existing read handles.
// In-progress read calls finish before files are closed.
func (d *DB) Close() error {
	d.closeOnce.Do(func() {
		d.admit.Lock()
		d.closing = true
		close(d.queue)
		d.admit.Unlock()
		<-d.writerDone
		close(d.stop)
		<-d.maintenanceDone
		d.life.Lock()
		defer d.life.Unlock()
		d.dead = true
		d.closeErr = errors.Join(d.fatal, d.syncReplayWALs(), d.closeFiles())
	})
	return d.closeErr
}

func (d *DB) closeFiles() error {
	var err error
	for _, table := range d.tables {
		err = errors.Join(err, table.file.Close())
	}
	if d.current != nil {
		d.current.Release()
	}
	if d.walFile != nil {
		err = errors.Join(err, d.walFile.Close())
	}
	if d.log != nil {
		err = errors.Join(err, d.log.Close())
	}
	if d.lock != nil {
		err = errors.Join(err, d.lock.Close())
	}
	return err
}
