package ember

import (
	"sync"
	"time"
)

// Snapshot pins a sequence, expiry timestamp and file version. Close releases
// the pin. Iterators created from it own independent pins.
type Snapshot struct {
	mu   sync.RWMutex
	db   *DB
	view *readView
}

// NewSnapshot captures a point-in-time read view.
func (d *DB) NewSnapshot() (*Snapshot, error) {
	if !d.begin() {
		return nil, ErrClosed
	}
	defer d.life.RUnlock()
	return &Snapshot{db: d, view: d.capture()}, nil
}

// Get returns the value visible in the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	r, err := s.GetRecord(key)
	return r.Value, err
}

// GetRecord returns the value and expiry visible in the snapshot.
func (s *Snapshot) GetRecord(key []byte) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.view == nil || !s.db.begin() {
		return Record{}, ErrClosed
	}
	defer s.db.life.RUnlock()
	return s.db.get(s.view, key)
}

// NewIterator starts an independently pinned iterator over this snapshot.
func (s *Snapshot) NewIterator(opts IteratorOptions) (*Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.view == nil || !s.db.begin() {
		return nil, ErrClosed
	}
	defer s.db.life.RUnlock()
	return s.db.newIterator(s.db.cloneView(s.view), opts)
}

// Close releases the snapshot. Calling it again is safe.
func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.view != nil {
		s.db.release(s.view)
		s.view = nil
	}
	return nil
}

// Time returns the snapshot's fixed expiry timestamp. A closed snapshot
// returns zero.
func (s *Snapshot) Time() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.view == nil {
		return time.Time{}
	}
	return time.Unix(0, s.view.at)
}
