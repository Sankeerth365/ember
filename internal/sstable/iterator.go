package sstable

import (
	"fmt"
	"sort"

	"ember/internal/skiplist"
)

// Iterator traverses one table in internal-key order. Use one goroutine
// per iterator. Its first Next positions it at the first entry.
type Iterator struct {
	table          *Table
	cursor         blockCursor
	index          int
	started, valid bool
	bypassCache    bool
	err            error
}

// NewIterator returns an unpositioned iterator. The table's source must
// remain open for its lifetime.
func (t *Table) NewIterator() *Iterator { return &Iterator{table: t} }

// Seek positions at the first entry at or after target.
func (it *Iterator) Seek(target skiplist.Key) bool {
	if it.err != nil {
		return false
	}
	it.started, it.valid = true, false
	i := sort.Search(len(it.table.index), func(i int) bool {
		return skiplist.Compare(it.table.index[i].last, target) >= 0
	})
	if !it.load(i) {
		return false
	}
	it.valid, it.err = it.cursor.seek(target)
	if it.err == nil && !it.valid {
		it.err = fmt.Errorf("%w: seek exceeded index fence", ErrCorrupt)
	}
	return it.valid
}

// Next advances by one internal entry.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		if !it.load(0) {
			return false
		}
	} else if !it.valid {
		return false
	}
	for {
		ok, err := it.cursor.next()
		it.valid, it.err = ok, err
		if ok || err != nil {
			return ok
		}
		if skiplist.Compare(it.cursor.entry.Key, it.table.index[it.index].last) != 0 {
			it.err = fmt.Errorf("%w: data block does not match index fence", ErrCorrupt)
			return false
		}
		if !it.load(it.index + 1) {
			return false
		}
	}
}

func (it *Iterator) load(i int) bool {
	it.index = i
	if i >= len(it.table.index) {
		return false
	}
	block, err := it.table.loadDataBlock(i, !it.bypassCache)
	it.err = err
	it.cursor = blockCursor{block: block}
	return err == nil
}

// Entry returns the current entry. Its slices are read-only and valid until
// the next Seek or Next. Call Entry only after a successful Seek or Next.
func (it *Iterator) Entry() skiplist.Entry { return it.cursor.entry }

// Err returns the first error encountered by the iterator.
func (it *Iterator) Err() error { return it.err }

// Verify rereads all data blocks from disk and validates ordering, fences,
// entry count, and that every stored user key passes the Bloom filter.
func (t *Table) Verify() error {
	it := &Iterator{table: t, bypassCache: true}
	var previous skiplist.Key
	var count uint64
	for it.Next() {
		entry := it.Entry()
		if count > 0 && skiplist.Compare(previous, entry.Key) >= 0 {
			return fmt.Errorf("%w: out-of-order data key", ErrCorrupt)
		}
		if !t.MayContain(entry.User) {
			return fmt.Errorf("%w: Bloom false negative", ErrCorrupt)
		}
		previous.User = append(previous.User[:0], entry.User...)
		previous.Seq, previous.Kind = entry.Seq, entry.Kind
		count++
	}
	if err := it.Err(); err != nil {
		return err
	}
	if count != t.entries {
		return fmt.Errorf("%w: data entry count %d differs from footer %d", ErrCorrupt, count, t.entries)
	}
	return nil
}
