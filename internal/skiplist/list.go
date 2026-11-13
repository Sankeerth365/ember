// Package skiplist provides Ember's ordered, append-only memtable.
package skiplist

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"unsafe"
)

const maxHeight = 20

// Kind is the on-disk operation type.
type Kind uint8

const (
	// Delete marks a key as absent at the entry's sequence.
	Delete Kind = iota
	// Put stores a value, optionally with an expiry.
	Put
)

// Key orders by user bytes ascending, sequence descending, then kind ascending.
type Key struct {
	// User holds the original key bytes.
	User []byte
	// Seq determines visibility and sorts newer versions first.
	Seq uint64
	// Kind identifies a put or delete.
	Kind Kind
}

// Compare returns the internal-key ordering of a and b.
func Compare(a, b Key) int {
	if order := bytes.Compare(a.User, b.User); order != 0 {
		return order
	}
	switch {
	case a.Seq > b.Seq:
		return -1
	case a.Seq < b.Seq:
		return 1
	default:
		return int(a.Kind) - int(b.Kind)
	}
}

// Entry is a version of a user key. A zero expiry means it never expires.
type Entry struct {
	Key
	// Value holds the payload of a put.
	Value []byte
	// ExpiresAt is a Unix timestamp in nanoseconds, or zero for no expiry.
	ExpiresAt int64
}

type node struct {
	entry Entry
	next  []*node
}

// List permits concurrent readers and one engine writer. The zero value works.
// Callers must keep returned entry slices immutable.
type List struct {
	mu    sync.RWMutex
	head  [maxHeight]*node
	level int
	size  int64
}

// Insert copies entry and links it into the list. It returns false if the exact
// internal key already exists. Installed entries are never edited or removed.
func (l *List) Insert(entry Entry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	var previous [maxHeight]*node
	current := l.find(entry.Key, &previous)
	if current != nil && Compare(current.entry.Key, entry.Key) == 0 {
		return false
	}

	height := 1
	random := rand.Uint64()
	for height < maxHeight && random&3 == 0 {
		height++
		random >>= 2
	}

	entry.User = clone(entry.User)
	entry.Value = clone(entry.Value)
	added := &node{entry: entry, next: make([]*node, height)}
	for i := 0; i < height; i++ {
		if previous[i] == nil {
			added.next[i], l.head[i] = l.head[i], added
		} else {
			added.next[i], previous[i].next[i] = previous[i].next[i], added
		}
	}
	l.level = max(l.level, height)
	l.size += int64(len(entry.User)) + int64(len(entry.Value)) +
		int64(unsafe.Sizeof(node{})) + int64(height)*int64(unsafe.Sizeof((*node)(nil)))
	return true
}

func clone(data []byte) []byte {
	if data == nil {
		return nil
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied
}

// Bytes returns the estimated bytes charged to entries and their links.
func (l *List) Bytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

// Get returns the newest entry for user at or below seq, including deletes.
func (l *List) Get(user []byte, seq uint64) (Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	current := l.find(Key{User: user, Seq: seq, Kind: Delete}, nil)
	if current == nil || !bytes.Equal(current.entry.User, user) {
		return Entry{}, false
	}
	return current.entry, true
}

// NewIterator returns an unpositioned internal-key iterator. The first Next
// positions it at the first entry. A consumer can filter by sequence for a
// stable view while a writer adds entries.
func (l *List) NewIterator() *Iterator { return &Iterator{list: l} }

// find runs with the list lock held. It returns the first node >= target.
func (l *List) find(target Key, previous *[maxHeight]*node) *node {
	var current *node
	for level := l.level - 1; level >= 0; level-- {
		next := l.after(current, level)
		for next != nil && Compare(next.entry.Key, target) < 0 {
			current = next
			next = current.next[level]
		}
		if previous != nil {
			previous[level] = current
		}
	}
	return l.after(current, 0)
}

func (l *List) after(current *node, level int) *node {
	if current == nil {
		return l.head[level]
	}
	return current.next[level]
}

// Iterator walks a List in internal-key order. Use one goroutine per iterator.
type Iterator struct {
	list    *List
	current *node
	started bool
}

// Seek positions at the first key greater than or equal to target.
func (it *Iterator) Seek(target Key) bool {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	it.current = it.list.find(target, nil)
	it.started = true
	return it.current != nil
}

// Next advances the iterator and reports whether it has an entry.
func (it *Iterator) Next() bool {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	if !it.started {
		it.current = it.list.head[0]
		it.started = true
	} else if it.current != nil {
		it.current = it.current.next[0]
	}
	return it.current != nil
}

// Entry returns the current immutable entry. Call it after a successful
// Seek or Next.
func (it *Iterator) Entry() Entry { return it.current.entry }
