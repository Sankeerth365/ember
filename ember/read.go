package ember

import (
	"bytes"
	"errors"
	"time"

	"ember/internal/compaction"
	"ember/internal/skiplist"
	"ember/internal/version"
)

// Record is a copy of a visible value and its expiry, if present.
type Record struct {
	// Value holds the stored bytes.
	Value []byte
	// ExpiresAt is zero for a persistent key.
	ExpiresAt time.Time
}

type readView struct {
	pin  *version.Pin
	mems []memtable // newest first
	seq  uint64
	at   int64
}

func (d *DB) capture() *readView {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.clockHigh = max(d.clockHigh, d.opts.Clock().UnixNano())
	v := &readView{pin: d.current.Clone(), seq: d.sequence, at: d.clockHigh, mems: []memtable{d.active}}
	for i := len(d.immutable) - 1; i >= 0; i-- {
		v.mems = append(v.mems, d.immutable[i])
	}
	d.views[v] = struct{}{}
	return v
}

func (d *DB) cloneView(source *readView) *readView {
	d.mu.Lock()
	defer d.mu.Unlock()
	clone := *source
	clone.pin = source.pin.Clone()
	d.views[&clone] = struct{}{}
	return &clone
}

func (d *DB) release(v *readView) {
	d.mu.Lock()
	delete(d.views, v)
	d.mu.Unlock()
	v.pin.Release()
	d.signal()
}

func (d *DB) table(number uint64) (tableFile, error) {
	d.mu.Lock()
	t, ok := d.tables[number]
	d.mu.Unlock()
	if !ok {
		return tableFile{}, errors.New("ember: pinned SSTable is unavailable")
	}
	return t, nil
}

// Get returns a copy of the current value or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	r, err := d.GetRecord(key)
	return r.Value, err
}

// GetRecord returns a copy of the current value and its expiry.
func (d *DB) GetRecord(key []byte) (Record, error) {
	if !d.begin() {
		return Record{}, ErrClosed
	}
	defer d.life.RUnlock()
	v := d.capture()
	defer d.release(v)
	return d.get(v, key)
}

func (d *DB) get(v *readView, key []byte) (Record, error) {
	for _, m := range v.mems {
		if entry, ok := m.list.Get(key, v.seq); ok {
			return visible(entry, v.at)
		}
	}
	var result Record
	err := ErrNotFound
	v.pin.Version().VisitCandidates(key, func(n uint64) bool {
		t, e := d.table(n)
		if e != nil {
			err = e
			return false
		}
		if !t.table.MayContain(key) {
			d.counters.bloomSkips.Add(1)
			return true
		}
		it := t.table.NewIterator()
		if !it.Seek(skiplist.Key{User: key, Seq: v.seq, Kind: skiplist.Delete}) {
			if e := it.Err(); e != nil {
				err = e
				return false
			}
			return true
		}
		entry := it.Entry()
		if !bytes.Equal(entry.User, key) {
			return true
		}
		result, err = visible(entry, v.at)
		return false
	})
	return result, err
}

func visible(e skiplist.Entry, at int64) (Record, error) {
	if e.Kind == skiplist.Delete || e.ExpiresAt != 0 && e.ExpiresAt <= at {
		return Record{}, ErrNotFound
	}
	r := Record{Value: bytes.Clone(e.Value)}
	if e.ExpiresAt != 0 {
		r.ExpiresAt = time.Unix(0, e.ExpiresAt)
	}
	return r, nil
}

type memSource struct{ *skiplist.Iterator }

func (memSource) Err() error { return nil }

func (d *DB) sources(v *readView, opts IteratorOptions) ([]compaction.Source, error) {
	sources := make([]compaction.Source, 0, len(v.mems))
	for _, m := range v.mems {
		sources = append(sources, memSource{m.list.NewIterator()})
	}
	version := v.pin.Version()
	for level := 0; level < version.LevelCount(); level++ {
		for _, f := range version.Files(level) {
			if opts.Lower != nil && bytes.Compare(f.MaxKey, opts.Lower) < 0 ||
				opts.Upper != nil && bytes.Compare(f.MinKey, opts.Upper) >= 0 {
				continue
			}
			t, err := d.table(f.Number)
			if err != nil {
				return nil, err
			}
			sources = append(sources, t.table.NewIterator())
		}
	}
	return sources, nil
}
