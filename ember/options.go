package ember

import (
	"errors"
	"log/slog"
	"time"

	"ember/internal/bloom"
	"ember/internal/vfs"
)

// SyncPolicy controls when the write-ahead log is synced.
type SyncPolicy uint8

const (
	// SyncEveryWrite syncs each write group before acknowledging it.
	SyncEveryWrite SyncPolicy = iota
	// SyncPeriodic acknowledges before syncing at SyncInterval.
	SyncPeriodic
	// SyncNever skips routine WAL syncs. Clean close still syncs the WAL.
	SyncNever
)

// Options configures an Ember database. Numeric zeroes select defaults.
type Options struct {
	// MemtableBytes is the approximate rotation threshold.
	MemtableBytes int64
	// MaxImmutable is the number of sealed memtables allowed in memory.
	MaxImmutable int
	// L0Trigger starts compaction at this many level-zero files.
	L0Trigger int
	// L0Slowdown inserts a short delay at this level-zero file count.
	L0Slowdown int
	// L0Stop stalls the writer at this level-zero file count.
	L0Stop int
	// Levels is the total number of compaction levels, including L0.
	Levels int
	// BaseLevelBytes sets the level-one size budget.
	BaseLevelBytes uint64
	// TargetTableBytes is the soft output-file target for compactions.
	TargetTableBytes int64
	// CacheBytes is the payload budget. A negative value disables caching.
	CacheBytes int64
	// BloomBitsPerKey controls SSTable Bloom density.
	BloomBitsPerKey int
	// Compression enables DEFLATE compression for new tables.
	Compression bool
	// SyncPolicy selects the acknowledgment policy.
	SyncPolicy SyncPolicy
	// SyncInterval defaults to 10 milliseconds in periodic mode.
	SyncInterval time.Duration
	// Logger receives recovery and maintenance diagnostics.
	Logger *slog.Logger
	// FS overrides the local filesystem, primarily for recovery tests.
	FS vfs.FS
	// Clock overrides wall time for deterministic expiry tests.
	Clock func() time.Time
}

func options(opts *Options) (Options, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	if o.MemtableBytes == 0 {
		o.MemtableBytes = 4 << 20
	}
	if o.MaxImmutable == 0 {
		o.MaxImmutable = 2
	}
	if o.L0Trigger == 0 {
		o.L0Trigger = 4
	}
	if o.L0Slowdown == 0 {
		o.L0Slowdown = max(8, o.L0Trigger)
	}
	if o.L0Stop == 0 {
		o.L0Stop = max(12, o.L0Slowdown)
	}
	if o.Levels == 0 {
		o.Levels = 7
	}
	if o.BaseLevelBytes == 0 {
		o.BaseLevelBytes = 32 << 20
	}
	if o.TargetTableBytes == 0 {
		o.TargetTableBytes = 8 << 20
	}
	if o.CacheBytes == 0 {
		o.CacheBytes = 32 << 20
	}
	if o.SyncInterval == 0 {
		o.SyncInterval = 10 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.FS == nil {
		o.FS = vfs.OS{}
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.MemtableBytes < 1 || o.MaxImmutable < 1 || o.L0Trigger < 1 || o.L0Slowdown < o.L0Trigger ||
		o.L0Stop < o.L0Slowdown || o.Levels < 2 || o.Levels > 32 || o.TargetTableBytes < 1 ||
		o.SyncPolicy > SyncNever || o.SyncInterval < 1 {
		return Options{}, errors.New("invalid Ember options")
	}
	if _, err := bloom.New(0, o.BloomBitsPerKey); err != nil {
		return Options{}, err
	}
	return o, nil
}
