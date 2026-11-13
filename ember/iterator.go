package ember

import (
	"bytes"
	"errors"
	"math"

	"ember/internal/compaction"
	"ember/internal/skiplist"
)

// IteratorOptions bounds a user-key range. Nil means unbounded.
type IteratorOptions struct {
	// Lower is inclusive.
	Lower []byte
	// Upper is exclusive.
	Upper []byte
}

// Iterator visits visible user keys in byte order. Use one goroutine per
// iterator, and close it to release its read view.
type Iterator struct {
	db             *DB
	view           *readView
	merge          *compaction.Merge
	bounds         IteratorOptions
	key, value     []byte
	started, valid bool
	err            error
}

// NewIterator captures a read view and returns an unpositioned iterator.
func (d *DB) NewIterator(opts IteratorOptions) (*Iterator, error) {
	if !d.begin() {
		return nil, ErrClosed
	}
	defer d.life.RUnlock()
	return d.newIterator(d.capture(), opts)
}

func (d *DB) newIterator(v *readView, opts IteratorOptions) (*Iterator, error) {
	if opts.Lower != nil && opts.Upper != nil && bytes.Compare(opts.Lower, opts.Upper) > 0 {
		d.release(v)
		return nil, errors.New("ember: iterator lower bound exceeds upper bound")
	}
	opts.Lower, opts.Upper = bytes.Clone(opts.Lower), bytes.Clone(opts.Upper)
	sources, err := d.sources(v, opts)
	if err != nil {
		d.release(v)
		return nil, err
	}
	return &Iterator{db: d, view: v, bounds: opts, merge: compaction.NewMerge(sources)}, nil
}

func (it *Iterator) enter() bool {
	if it.err != nil {
		return false
	}
	if it.view == nil || !it.db.begin() {
		it.err = ErrClosed
		it.valid = false
		return false
	}
	return true
}

// Seek positions at the first visible key at or after key and the lower bound.
func (it *Iterator) Seek(key []byte) bool {
	if !it.enter() {
		return false
	}
	defer it.db.life.RUnlock()
	return it.seek(key)
}

func (it *Iterator) seek(key []byte) bool {
	it.started = true
	if it.bounds.Lower != nil && bytes.Compare(key, it.bounds.Lower) < 0 {
		key = it.bounds.Lower
	}
	return it.find(it.merge.Seek(skiplist.Key{User: key, Seq: math.MaxUint64, Kind: skiplist.Delete}), nil, false)
}

// Next advances to the next visible key. Its first call starts at Lower.
func (it *Iterator) Next() bool {
	if !it.enter() {
		return false
	}
	defer it.db.life.RUnlock()
	if !it.started {
		return it.seek(it.bounds.Lower)
	}
	if !it.valid {
		return false
	}
	return it.find(it.merge.Next(), it.key, true)
}

func (it *Iterator) find(ok bool, skip []byte, skipSet bool) bool {
	it.valid = false
	for ok {
		e := it.merge.Entry()
		if it.bounds.Upper != nil && bytes.Compare(e.User, it.bounds.Upper) >= 0 {
			return false
		}
		if e.Seq <= it.view.seq && (!skipSet || !bytes.Equal(e.User, skip)) {
			if e.Kind == skiplist.Put && (e.ExpiresAt == 0 || e.ExpiresAt > it.view.at) {
				it.key = append(it.key[:0], e.User...)
				it.value = append(it.value[:0], e.Value...)
				it.valid = true
				return true
			}
			skip = append(it.key[:0], e.User...)
			it.key = skip
			skipSet = true
		}
		ok = it.merge.Next()
	}
	it.err = it.merge.Err()
	return false
}

// Key returns the current read-only key, valid until the next positioning call.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.key
}

// Value returns the current read-only value, valid until the next positioning call.
func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.value
}

// Err returns the first iteration error.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's pin. Calling it again is safe.
func (it *Iterator) Close() error {
	if it.view != nil {
		it.db.release(it.view)
		it.view = nil
	}
	it.valid = false
	it.merge = nil
	return nil
}
