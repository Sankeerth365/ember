package ember

import (
	"bytes"
	"errors"
	"time"

	"ember/internal/skiplist"
	"ember/internal/wal"
)

// Batch builds an atomic sequence of puts and deletes. Use one goroutine
// to build and submit a batch. It may be reused after Write returns.
type Batch struct {
	ops      []skiplist.Entry
	estimate int
	err      error
}

// Put adds a value and clears any previous expiry for key.
func (b *Batch) Put(key, value []byte) { b.add(key, value, 0, skiplist.Put) }

// PutWithExpiry adds a value with an absolute expiry. Zero time clears it.
// Write rejects a batch whose expiry is outside the Unix nanosecond range.
// The Unix epoch is encoded as one nanosecond after it because the format
// reserves zero for no expiry.
func (b *Batch) PutWithExpiry(key, value []byte, expiry time.Time) {
	if b.err != nil {
		return
	}
	var at int64
	if !expiry.IsZero() {
		at = expiry.UnixNano()
		if !time.Unix(0, at).Equal(expiry) {
			b.err = errors.New("ember: expiry outside Unix nanosecond range")
			return
		}
		if at == 0 {
			at = 1
		}
	}
	b.add(key, value, at, skiplist.Put)
}

// Delete adds a tombstone.
func (b *Batch) Delete(key []byte) { b.add(key, nil, 0, skiplist.Delete) }

func (b *Batch) add(key, value []byte, expiry int64, kind skiplist.Kind) {
	if b.err != nil {
		return
	}
	// Keep MANIFEST bounds and SSTable indexes small even with large values.
	if len(key) > 1<<20 || len(value) > wal.MaxRecordSize ||
		b.estimate+len(key)+len(value)+32 > wal.MaxRecordSize-13 || len(b.ops) == wal.MaxOperations {
		b.err = errors.New("ember: key or batch exceeds size limit")
		return
	}
	b.estimate += len(key) + len(value) + 32
	b.ops = append(b.ops, skiplist.Entry{
		Key:   skiplist.Key{User: bytes.Clone(key), Kind: kind},
		Value: bytes.Clone(value), ExpiresAt: expiry,
	})
}
