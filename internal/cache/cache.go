// Package cache implements a byte-bounded sharded LRU for decoded table blocks.
package cache

import (
	"container/list"
	"math/bits"
	"sync"
)

// Key identifies a block by immutable table number and file offset.
type Key struct {
	// File is the table's file number.
	File uint64
	// Offset is the byte offset of the block within the file.
	Offset uint64
}

// Stats records cache activity and current payload bytes.
type Stats struct {
	// Hits counts successful lookups.
	Hits uint64
	// Misses counts unsuccessful lookups in an enabled cache.
	Misses uint64
	// Evictions counts blocks removed to make room for another block.
	Evictions uint64
	// Bytes counts the payload bytes currently owned by the cache.
	Bytes int64
}

// Cache holds one LRU per shard. Its zero value is a disabled cache.
type Cache struct {
	shards []shard
}

type shard struct {
	mu        sync.Mutex
	entries   map[Key]*list.Element
	order     list.List
	used      int64
	capacity  int64
	hits      uint64
	misses    uint64
	evictions uint64
}

type block struct {
	key  Key
	data []byte
}

// New partitions capacity among shards. A nonpositive capacity disables the
// cache. A nonpositive shard count selects 16 shards.
func New(capacity int64, shards int) *Cache {
	if capacity <= 0 {
		return &Cache{}
	}
	if shards <= 0 {
		shards = 16
	}
	shards = int(min(int64(shards), capacity))
	c := &Cache{shards: make([]shard, shards)}
	for i := range c.shards {
		s := &c.shards[i]
		s.entries = make(map[Key]*list.Element)
		s.capacity = capacity / int64(shards)
		if int64(i) < capacity%int64(shards) {
			s.capacity++
		}
	}
	return c
}

func (c *Cache) shard(key Key) *shard {
	hash := key.File*0x9e3779b97f4a7c15 ^ bits.RotateLeft64(key.Offset*0xbf58476d1ce4e5b9, 31)
	hash ^= hash >> 30
	hash *= 0x94d049bb133111eb
	hash ^= hash >> 31
	return &c.shards[hash%uint64(len(c.shards))]
}

// Get returns immutable block bytes and marks the block most recently used.
// Returned bytes remain valid even if the cache evicts the entry.
func (c *Cache) Get(key Key) ([]byte, bool) {
	if len(c.shards) == 0 {
		return nil, false
	}
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	element := s.entries[key]
	if element == nil {
		s.misses++
		return nil, false
	}
	s.hits++
	s.order.MoveToFront(element)
	return element.Value.(block).data, true
}

// Set stores a private copy of a decoded block. Entries larger than their
// shard's budget bypass the cache. The byte budget counts block payloads.
func (c *Cache) Set(key Key, data []byte) {
	if len(c.shards) == 0 || len(data) == 0 {
		return
	}
	s := c.shard(key)
	size := int64(len(data))
	if size > s.capacity {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if element := s.entries[key]; element != nil {
		s.remove(element)
	}
	for s.used > s.capacity-size {
		s.remove(s.order.Back())
		s.evictions++
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	s.entries[key] = s.order.PushFront(block{key: key, data: copied})
	s.used += size
}

func (s *shard) remove(element *list.Element) {
	entry := element.Value.(block)
	s.used -= int64(len(entry.data))
	delete(s.entries, entry.key)
	s.order.Remove(element)
}

// Stats returns aggregate counters and a point-in-time estimate of bytes.
func (c *Cache) Stats() Stats {
	var stats Stats
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		stats.Bytes += s.used
		stats.Hits += s.hits
		stats.Misses += s.misses
		stats.Evictions += s.evictions
		s.mu.Unlock()
	}
	return stats
}
