package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"ember/internal/bloom"
	"ember/internal/cache"
	"ember/internal/skiplist"
)

type indexEntry struct {
	last   skiplist.Key
	handle handle
}

// Info describes the table's physical blocks and Bloom filter.
type Info struct {
	// Size is the physical table size in bytes.
	Size uint64
	// Entries is the footer's internal-key count.
	Entries uint64
	// Blocks is the data block count.
	Blocks int
	// BloomBits is the number of bits in the filter.
	BloomBits uint64
	// BloomProbes is the number of probes per filter query.
	BloomProbes int
}

// Table is an immutable, concurrently readable view of an SSTable.
type Table struct {
	src     io.ReaderAt
	size    uint64
	fileNo  uint64
	cache   *cache.Cache
	codecs  map[byte]Codec
	index   []indexEntry
	filter  *bloom.Filter
	entries uint64
}

// Open validates a table's footer, index and Bloom block. File numbers
// must be unique within any shared cache. The caller owns src.
func Open(src io.ReaderAt, size int64, fileNo uint64, blocks *cache.Cache, codecs ...Codec) (*Table, error) {
	if src == nil {
		return nil, errors.New("missing SSTable source")
	}
	if size < footerSize {
		return nil, fmt.Errorf("%w: table shorter than footer", ErrCorrupt)
	}
	t := &Table{src: src, size: uint64(size), fileNo: fileNo, cache: blocks}
	t.codecs = map[byte]Codec{1: Deflate{}}
	for _, codec := range codecs {
		if codec == nil || codec.ID() < 2 || t.codecs[codec.ID()] != nil {
			return nil, errors.New("invalid or duplicate SSTable codec ID")
		}
		t.codecs[codec.ID()] = codec
	}
	var p [footerSize]byte
	if err := readAt(src, p[:], size-footerSize); err != nil {
		return nil, err
	}
	f, err := parseFooter(p[:], uint64(size-footerSize))
	if err != nil {
		return nil, err
	}
	raw, err := t.readBlock(f.index, true)
	if err != nil {
		return nil, err
	}
	t.index, err = parseIndex(raw, f)
	if err != nil {
		return nil, err
	}
	raw, err = t.readBlock(f.filter, true)
	if err != nil {
		return nil, err
	}
	t.filter, err = bloom.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	t.entries = f.entries
	return t, nil
}

func parseIndex(p []byte, f footer) ([]indexEntry, error) {
	if len(p) < 4 || uint64(binary.LittleEndian.Uint32(p)) != f.blocks ||
		f.blocks > uint64((len(p)-4)/(1+keyTrailer+handleSize)) {
		return nil, fmt.Errorf("%w: invalid index count", ErrCorrupt)
	}
	p = p[4:]
	count := int(f.blocks)
	index := make([]indexEntry, 0, count)
	var end uint64
	for i := 0; i < count; i++ {
		length, n := binary.Uvarint(p)
		if n <= 0 || len(p)-n < handleSize || length > uint64(len(p)-n-handleSize) {
			return nil, fmt.Errorf("%w: invalid index key length", ErrCorrupt)
		}
		at := n + int(length)
		key, valid := decodeKey(p[n:at])
		h := parseHandle(p[at:])
		if !valid || !h.valid(f.index.offset) || h.offset != end ||
			i > 0 && skiplist.Compare(index[i-1].last, key) >= 0 {
			return nil, fmt.Errorf("%w: invalid index key or data handle", ErrCorrupt)
		}
		index = append(index, indexEntry{key, h})
		end = h.offset + h.length
		p = p[at+handleSize:]
	}
	if len(p) != 0 || end != f.index.offset {
		return nil, fmt.Errorf("%w: invalid index termination", ErrCorrupt)
	}
	return index, nil
}

func (t *Table) readBlock(h handle, rawOnly bool) ([]byte, error) {
	p := make([]byte, int(h.length))
	if err := readAt(t.src, p, int64(h.offset)); err != nil {
		return nil, err
	}
	end := len(p) - trailerSize
	id := p[end]
	if footerChecksum(p[:end], p[end:end+1]) != binary.LittleEndian.Uint32(p[end+1:]) {
		return nil, fmt.Errorf("%w: block checksum at offset %d", ErrCorrupt, h.offset)
	}
	if id == 0 {
		return p[:end:end], nil
	}
	codec := t.codecs[id]
	if rawOnly || codec == nil {
		return nil, fmt.Errorf("%w: unsupported block codec %d at offset %d", ErrCorrupt, id, h.offset)
	}
	out, err := codec.Decode(p[:end], maxBlockSize)
	if err != nil || len(out) == 0 || len(out) > maxBlockSize {
		return nil, fmt.Errorf("%w: invalid compressed block at offset %d: %v", ErrCorrupt, h.offset, err)
	}
	return out, nil
}

func (t *Table) loadDataBlock(i int, useCache bool) (dataBlock, error) {
	h := t.index[i].handle
	key := cache.Key{File: t.fileNo, Offset: h.offset}
	if useCache && t.cache != nil {
		if raw, ok := t.cache.Get(key); ok {
			return parseBlock(raw)
		}
	}
	raw, err := t.readBlock(h, false)
	if err != nil {
		return dataBlock{}, err
	}
	block, err := parseBlock(raw)
	if err == nil && useCache && t.cache != nil {
		t.cache.Set(key, raw)
	}
	return block, err
}

// Info reports footer and Bloom filter statistics.
func (t *Table) Info() Info {
	return Info{t.size, t.entries, len(t.index), t.filter.BitCount(), t.filter.Probes()}
}

// MayContain reports whether the Bloom filter allows a user key.
func (t *Table) MayContain(user []byte) bool { return t.filter.MayContain(user) }

// Get returns the newest entry at or below seq, including tombstones. A
// caller may check MayContain first to count Bloom skips separately.
func (t *Table) Get(user []byte, seq uint64) (skiplist.Entry, bool, error) {
	if !t.MayContain(user) {
		return skiplist.Entry{}, false, nil
	}
	it := t.NewIterator()
	if !it.Seek(skiplist.Key{User: user, Seq: seq, Kind: skiplist.Delete}) {
		return skiplist.Entry{}, false, it.Err()
	}
	entry := it.Entry()
	if !bytes.Equal(entry.User, user) {
		return skiplist.Entry{}, false, nil
	}
	entry.User, entry.Value = bytes.Clone(entry.User), bytes.Clone(entry.Value)
	return entry, true, nil
}
