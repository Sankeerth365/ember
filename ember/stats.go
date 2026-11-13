package ember

import "sync/atomic"

type counters struct {
	bloomSkips, stalls, slowdowns, compactions, flushes, groups, batches atomic.Uint64
}

// LevelStats describes the current tables in one level.
type LevelStats struct {
	// Files is the number of SSTables.
	Files int
	// Bytes is the combined physical size of those tables.
	Bytes uint64
}

// Stats describes the current database layout and cumulative counters.
type Stats struct {
	// Levels lists the table levels through the last populated level.
	Levels []LevelStats
	// Sequence is the latest published write sequence.
	Sequence uint64
	// ImmutableMemtables is the count awaiting flush.
	ImmutableMemtables int
	// CacheHitRate is the fraction of enabled-cache lookups that hit.
	CacheHitRate float64
	// CacheHits and CacheMisses count block cache lookups.
	CacheHits, CacheMisses uint64
	// CacheBytes is the payload memory retained by the cache.
	CacheBytes int64
	// BloomSkips counts candidate tables rejected by their filters.
	BloomSkips uint64
	// WriteStalls and WriteSlowdowns count writer backpressure events.
	WriteStalls, WriteSlowdowns uint64
	// Compactions and Flushes count completed maintenance operations.
	Compactions, Flushes uint64
	// WriteGroups and WriteBatches report group commit activity.
	WriteGroups, WriteBatches uint64
}

// Stats returns a point-in-time layout and recent cumulative counters.
func (d *DB) Stats() Stats {
	d.mu.Lock()
	v := d.current.Version()
	s := Stats{Sequence: d.sequence, ImmutableMemtables: len(d.immutable)}
	s.Levels = make([]LevelStats, v.LevelCount())
	d.mu.Unlock()
	for i := range s.Levels {
		s.Levels[i].Files, s.Levels[i].Bytes = v.LevelUsage(i)
	}
	cache := d.cache.Stats()
	s.CacheBytes, s.CacheHits, s.CacheMisses = cache.Bytes, cache.Hits, cache.Misses
	if total := cache.Hits + cache.Misses; total != 0 {
		s.CacheHitRate = float64(cache.Hits) / float64(total)
	}
	s.BloomSkips = d.counters.bloomSkips.Load()
	s.WriteStalls, s.WriteSlowdowns = d.counters.stalls.Load(), d.counters.slowdowns.Load()
	s.Compactions, s.Flushes = d.counters.compactions.Load(), d.counters.flushes.Load()
	s.WriteGroups, s.WriteBatches = d.counters.groups.Load(), d.counters.batches.Load()
	return s
}
