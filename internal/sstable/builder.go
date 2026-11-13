package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"ember/internal/bloom"
	"ember/internal/skiplist"
)

const maxEntrySize = 64 << 20

// Options configures table construction. Zero selects a 4 KiB block target,
// a restart interval of 16 and the default Bloom density.
type Options struct {
	// BlockSize is a soft target in bytes.
	BlockSize int
	// RestartInterval is the number of entries between full keys.
	RestartInterval int
	// BitsPerKey sets the Bloom filter density.
	BitsPerKey int
	// Codec enables compression when it makes a block smaller.
	Codec Codec
}

// Meta describes a completed table for the MANIFEST and inspection tools.
type Meta struct {
	// Size is the physical table size in bytes.
	Size uint64
	// Entries counts internal keys.
	Entries uint64
	// Blocks counts data blocks.
	Blocks uint64
	// MinKey is the smallest user key.
	MinKey []byte
	// MaxKey is the largest user key.
	MaxKey []byte
	// MinSeq is the smallest sequence.
	MinSeq uint64
	// MaxSeq is the largest sequence.
	MaxSeq uint64
}

// Builder writes an SSTable in strictly increasing internal-key order.
type Builder struct {
	dst        io.Writer
	opts       Options
	block      blockBuilder
	index      []byte
	filterKeys [][]byte
	lastKey    skiplist.Key
	meta       Meta
	done       bool
	err        error
}

// NewBuilder prepares a builder at the beginning of dst. The caller owns
// closing, syncing and publishing the resulting file.
func NewBuilder(dst io.Writer, opts Options) (*Builder, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4 << 10
	}
	if opts.RestartInterval == 0 {
		opts.RestartInterval = 16
	}
	if dst == nil || opts.BlockSize < 1 || opts.BlockSize > 1<<20 ||
		opts.RestartInterval < 1 || opts.Codec != nil && opts.Codec.ID() == 0 {
		return nil, errors.New("invalid SSTable builder options")
	}
	if opts.Codec != nil && opts.Codec.ID() == 1 {
		switch opts.Codec.(type) {
		case Deflate, *Deflate:
		default:
			return nil, errors.New("SSTable codec ID 1 is reserved for Deflate")
		}
	}
	if _, err := bloom.New(0, opts.BitsPerKey); err != nil {
		return nil, err
	}
	return &Builder{dst: dst, opts: opts, index: make([]byte, 4)}, nil
}

// Add copies an entry into the builder. It rejects out-of-order entries.
func (b *Builder) Add(entry skiplist.Entry) error {
	if b.done || b.err != nil {
		return b.stateError()
	}
	if entry.Seq == 0 || entry.Kind > skiplist.Put ||
		entry.Kind == skiplist.Delete && (len(entry.Value) != 0 || entry.ExpiresAt != 0) ||
		len(entry.User) > maxEntrySize || len(entry.Value) > maxEntrySize ||
		len(entry.User)+len(entry.Value) > maxEntrySize {
		return errors.New("invalid SSTable entry")
	}
	if b.meta.Entries != 0 && skiplist.Compare(b.lastKey, entry.Key) >= 0 {
		return errors.New("SSTable keys must be strictly increasing")
	}
	if len(b.block.data) >= b.opts.BlockSize {
		if err := b.flush(); err != nil {
			b.err = err
			return err
		}
	}
	key := encodeKey(entry.Key)
	b.block.add(key, entry, b.opts.RestartInterval)
	if b.meta.Entries == 0 || !bytes.Equal(b.lastKey.User, entry.User) {
		b.filterKeys = append(b.filterKeys, bytes.Clone(entry.User))
	}
	b.lastKey, _ = decodeKey(key)
	if b.meta.Entries == 0 {
		b.meta.MinKey = bytes.Clone(entry.User)
		b.meta.MinSeq = entry.Seq
	}
	b.meta.MinSeq = min(b.meta.MinSeq, entry.Seq)
	b.meta.MaxSeq = max(b.meta.MaxSeq, entry.Seq)
	b.meta.Entries++
	return nil
}

func (b *Builder) stateError() error {
	if b.err != nil {
		return b.err
	}
	return errors.New("SSTable builder already finished")
}

func (b *Builder) flush() error {
	if b.block.count == 0 {
		return nil
	}
	key := b.block.previous
	if len(key) > maxBlockSize-len(b.index)-binary.MaxVarintLen64-handleSize {
		return errors.New("SSTable index exceeds block size limit")
	}
	h, err := b.writeBlock(b.block.finish(), b.opts.Codec)
	if err != nil {
		return err
	}
	b.index = binary.AppendUvarint(b.index, uint64(len(key)))
	b.index = append(b.index, key...)
	b.index = h.appendTo(b.index)
	b.meta.Blocks++
	// A single large entry should not keep a large scratch block alive.
	scratch := b.block.data[:0]
	if cap(scratch) > 4*b.opts.BlockSize {
		scratch = nil
	}
	b.block = blockBuilder{data: scratch, restarts: b.block.restarts[:0]}
	return nil
}

func (b *Builder) writeBlock(payload []byte, codec Codec) (handle, error) {
	if len(payload) == 0 || len(payload) > maxBlockSize {
		return handle{}, errors.New("SSTable block exceeds size limit")
	}
	var id byte
	if codec != nil {
		encoded, err := codec.Encode(payload)
		if err != nil {
			return handle{}, fmt.Errorf("compress SSTable block: %w", err)
		}
		if len(encoded) != 0 && len(encoded) < len(payload) {
			payload, id = encoded, codec.ID()
		}
	}
	h := handle{offset: b.meta.Size, length: uint64(len(payload) + trailerSize)}
	trailer := [trailerSize]byte{id}
	binary.LittleEndian.PutUint32(trailer[1:], footerChecksum(payload, trailer[:1]))
	if h.offset > math.MaxInt64-h.length {
		return handle{}, errors.New("SSTable exceeds file size limit")
	}
	if err := writeFull(b.dst, payload); err != nil {
		return handle{}, fmt.Errorf("write SSTable block: %w", err)
	}
	if err := writeFull(b.dst, trailer[:]); err != nil {
		return handle{}, fmt.Errorf("write SSTable block trailer: %w", err)
	}
	b.meta.Size += h.length
	return h, nil
}

// Finish writes the remaining blocks and footer and returns table metadata.
func (b *Builder) Finish() (Meta, error) {
	if b.done || b.err != nil {
		return Meta{}, b.stateError()
	}
	b.done = true
	if b.meta.Entries == 0 {
		return Meta{}, errors.New("cannot finish an empty SSTable")
	}
	if err := b.flush(); err != nil {
		return Meta{}, err
	}
	filter, err := bloom.New(len(b.filterKeys), b.opts.BitsPerKey)
	if err != nil {
		return Meta{}, err
	}
	for _, user := range b.filterKeys {
		filter.Add(user)
	}
	b.filterKeys = nil
	binary.LittleEndian.PutUint32(b.index, uint32(b.meta.Blocks))
	indexHandle, err := b.writeBlock(b.index, nil)
	if err != nil {
		return Meta{}, err
	}
	filterHandle, err := b.writeBlock(filter.Encode(), nil)
	if err != nil {
		return Meta{}, err
	}
	f := footer{indexHandle, filterHandle, b.meta.Blocks, b.meta.Entries}
	if b.meta.Size > math.MaxInt64-footerSize {
		return Meta{}, errors.New("SSTable exceeds file size limit")
	}
	if err := writeFull(b.dst, f.encode()); err != nil {
		return Meta{}, fmt.Errorf("write SSTable footer: %w", err)
	}
	b.meta.Size += footerSize
	b.meta.MaxKey = bytes.Clone(b.lastKey.User)
	return b.meta, nil
}
