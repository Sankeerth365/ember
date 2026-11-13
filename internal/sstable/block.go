package sstable

import (
	"encoding/binary"
	"fmt"
	"sort"

	"ember/internal/skiplist"
)

type blockBuilder struct {
	data     []byte
	restarts []uint32
	previous []byte
	count    int
}

func (b *blockBuilder) add(key []byte, entry skiplist.Entry, interval int) {
	shared := 0
	if b.count%interval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.data)))
	} else {
		limit := min(len(key), len(b.previous))
		for shared < limit && key[shared] == b.previous[shared] {
			shared++
		}
	}
	valueSize := 0
	if entry.Kind == skiplist.Put {
		valueSize = 8 + len(entry.Value)
	}
	b.data = binary.AppendUvarint(b.data, uint64(shared))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)-shared))
	b.data = binary.AppendUvarint(b.data, uint64(valueSize))
	b.data = append(b.data, key[shared:]...)
	if entry.Kind == skiplist.Put {
		b.data = binary.LittleEndian.AppendUint64(b.data, uint64(entry.ExpiresAt))
		b.data = append(b.data, entry.Value...)
	}
	b.previous = key
	b.count++
}

func (b *blockBuilder) finish() []byte {
	for _, off := range b.restarts {
		b.data = binary.LittleEndian.AppendUint32(b.data, off)
	}
	return binary.LittleEndian.AppendUint32(b.data, uint32(len(b.restarts)))
}

type dataBlock struct {
	body, restarts []byte
}

func parseBlock(p []byte) (dataBlock, error) {
	if len(p) < 9 {
		return dataBlock{}, fmt.Errorf("%w: short data block", ErrCorrupt)
	}
	count := int(binary.LittleEndian.Uint32(p[len(p)-4:]))
	if count < 1 || count > (len(p)-5)/4 {
		return dataBlock{}, fmt.Errorf("%w: invalid restart count", ErrCorrupt)
	}
	end := len(p) - 4 - 4*count
	b := dataBlock{body: p[:end], restarts: p[end : len(p)-4]}
	prev := -1
	for i := 0; i < count; i++ {
		offset := b.restart(i)
		if offset <= prev || offset >= end || i == 0 && offset != 0 || b.body[offset] != 0 {
			return dataBlock{}, fmt.Errorf("%w: invalid restart offset", ErrCorrupt)
		}
		prev = offset
	}
	return b, nil
}

func (b dataBlock) restart(i int) int {
	return int(binary.LittleEndian.Uint32(b.restarts[i*4:]))
}

type blockCursor struct {
	block   dataBlock
	key     []byte
	entry   skiplist.Entry
	offset  int
	restart int
}

func (c *blockCursor) seek(target skiplist.Key) (bool, error) {
	var failure error
	i := sort.Search(len(c.block.restarts)/4, func(i int) bool {
		offset := c.block.restart(i)
		shared, suffix, _, _, ok := parseFields(c.block.body[offset:])
		key, valid := decodeKey(suffix)
		if !ok || !valid || shared != 0 {
			failure = fmt.Errorf("%w: invalid restart entry", ErrCorrupt)
			return true
		}
		return skiplist.Compare(key, target) >= 0
	})
	if failure != nil {
		return false, failure
	}
	c.restart = max(0, i-1)
	c.offset = c.block.restart(c.restart)
	c.key = c.key[:0]
	for {
		ok, err := c.next()
		if err != nil || !ok || skiplist.Compare(c.entry.Key, target) >= 0 {
			return ok, err
		}
	}
}

func (c *blockCursor) next() (bool, error) {
	if c.offset == len(c.block.body) {
		return false, nil
	}
	shared, suffix, value, consumed, ok := parseFields(c.block.body[c.offset:])
	if !ok || shared > uint64(len(c.key)) {
		return false, fmt.Errorf("%w: malformed data entry", ErrCorrupt)
	}
	if c.restart < len(c.block.restarts)/4 {
		restart := c.block.restart(c.restart)
		switch {
		case restart == c.offset && shared == 0:
			c.restart++
		case restart < c.offset+consumed:
			return false, fmt.Errorf("%w: restart inside data entry", ErrCorrupt)
		}
	}
	if shared+uint64(len(suffix)) > maxBlockSize {
		return false, fmt.Errorf("%w: reconstructed key exceeds limit", ErrCorrupt)
	}
	c.key = append(c.key[:int(shared)], suffix...)
	key, valid := decodeKey(c.key)
	if !valid || key.Kind == skiplist.Put && len(value) < 8 ||
		key.Kind == skiplist.Delete && len(value) != 0 {
		return false, fmt.Errorf("%w: invalid internal key or value", ErrCorrupt)
	}
	c.entry = skiplist.Entry{Key: key}
	if key.Kind == skiplist.Put {
		c.entry.ExpiresAt = int64(binary.LittleEndian.Uint64(value))
		c.entry.Value = value[8:len(value):len(value)]
	}
	c.offset += consumed
	return true, nil
}

func parseFields(p []byte) (shared uint64, suffix, value []byte, consumed int, ok bool) {
	shared, a := binary.Uvarint(p)
	if a <= 0 {
		return 0, nil, nil, 0, false
	}
	unshared, b := binary.Uvarint(p[a:])
	if b <= 0 {
		return 0, nil, nil, 0, false
	}
	size, c := binary.Uvarint(p[a+b:])
	if c <= 0 {
		return 0, nil, nil, 0, false
	}
	start := a + b + c
	if unshared > uint64(len(p)-start) || size > uint64(len(p)-start)-unshared {
		return 0, nil, nil, 0, false
	}
	mid := start + int(unshared)
	end := mid + int(size)
	return shared, p[start:mid], p[mid:end], end, true
}
