package compaction

import (
	"container/heap"

	"ember/internal/skiplist"
)

// Source is a forward iterator over sorted internal keys.
type Source interface {
	Seek(skiplist.Key) bool
	Next() bool
	Entry() skiplist.Entry
	Err() error
}

// Merge merges sorted sources in O(log k) per entry for k sources.
type Merge struct {
	sources []Source
	queue   sourceHeap
	started bool
	err     error
}

// NewMerge returns an unpositioned merging iterator.
func NewMerge(sources []Source) *Merge { return &Merge{sources: sources} }

// Seek positions all sources at the first entry at or after key.
func (m *Merge) Seek(key skiplist.Key) bool {
	return m.start(func(s Source) bool { return s.Seek(key) })
}

func (m *Merge) start(position func(Source) bool) bool {
	if m.err != nil {
		return false
	}
	m.started = true
	m.queue = m.queue[:0]
	for i, s := range m.sources {
		if position(s) {
			m.queue = append(m.queue, item{s, i})
		} else if m.err = s.Err(); m.err != nil {
			return false
		}
	}
	heap.Init(&m.queue)
	return len(m.queue) > 0
}

// Next positions at the first entry or advances to the next one.
func (m *Merge) Next() bool {
	if m.err != nil {
		return false
	}
	if !m.started {
		return m.start(func(s Source) bool { return s.Next() })
	}
	if len(m.queue) == 0 {
		return false
	}
	s := m.queue[0].source
	if s.Next() {
		heap.Fix(&m.queue, 0)
	} else {
		if m.err = s.Err(); m.err != nil {
			return false
		}
		heap.Pop(&m.queue)
	}
	return len(m.queue) > 0
}

// Entry returns the current entry, valid until the next positioning call.
func (m *Merge) Entry() skiplist.Entry { return m.queue[0].source.Entry() }

// Err returns the first source error.
func (m *Merge) Err() error { return m.err }

type item struct {
	source  Source
	ordinal int
}
type sourceHeap []item

func (h sourceHeap) Len() int { return len(h) }
func (h sourceHeap) Less(i, j int) bool {
	c := skiplist.Compare(h[i].source.Entry().Key, h[j].source.Entry().Key)
	return c < 0 || c == 0 && h[i].ordinal < h[j].ordinal
}
func (h sourceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sourceHeap) Push(x any)   { *h = append(*h, x.(item)) }
func (h *sourceHeap) Pop() any {
	last := len(*h) - 1
	x := (*h)[last]
	(*h)[last] = item{}
	*h = (*h)[:last]
	return x
}
