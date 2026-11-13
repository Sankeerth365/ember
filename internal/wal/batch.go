// Package wal encodes atomic batches and frames them in checksummed blocks.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"ember/internal/skiplist"
)

const (
	// MaxRecordSize bounds the size of one encoded batch.
	MaxRecordSize = 64 << 20
	// MaxOperations bounds the number of operations in one batch.
	MaxOperations = 1 << 18
	batchVersion  = 1
	batchHeader   = 13
)

// EncodeBatch encodes nonempty entries with contiguous, nonzero sequences.
func EncodeBatch(entries []skiplist.Entry) ([]byte, error) {
	if len(entries) == 0 || len(entries) > MaxOperations {
		return nil, errors.New("invalid WAL batch operation count")
	}
	first := entries[0].Seq
	if first == 0 || first > math.MaxUint64-uint64(len(entries)-1) {
		return nil, errors.New("invalid WAL batch sequence range")
	}
	size := batchHeader
	for i, entry := range entries {
		if entry.Seq != first+uint64(i) || entry.Kind > skiplist.Put {
			return nil, fmt.Errorf("invalid WAL operation %d", i)
		}
		if entry.Kind == skiplist.Delete && (len(entry.Value) != 0 || entry.ExpiresAt != 0) {
			return nil, fmt.Errorf("WAL delete %d carries a value or expiry", i)
		}
		if len(entry.User) > MaxRecordSize || len(entry.Value) > MaxRecordSize {
			return nil, errors.New("WAL operation exceeds size limit")
		}
		size += 1 + varintSize(len(entry.User)) + len(entry.User)
		if entry.Kind == skiplist.Put {
			size += 8 + varintSize(len(entry.Value)) + len(entry.Value)
		}
		if size > MaxRecordSize {
			return nil, errors.New("WAL batch exceeds size limit")
		}
	}
	out := make([]byte, 0, size)
	out = append(out, batchVersion)
	out = binary.LittleEndian.AppendUint64(out, first)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(entries)))
	for _, entry := range entries {
		out = append(out, byte(entry.Kind))
		out = appendBytes(out, entry.User)
		if entry.Kind == skiplist.Put {
			out = binary.LittleEndian.AppendUint64(out, uint64(entry.ExpiresAt))
			out = appendBytes(out, entry.Value)
		}
	}
	return out, nil
}

func varintSize(n int) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], uint64(n))
}

func appendBytes(dst, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

// DecodeBatch validates a complete record. Returned key and value slices
// reference record and must be treated as immutable.
func DecodeBatch(record []byte) ([]skiplist.Entry, error) {
	if len(record) < batchHeader || len(record) > MaxRecordSize || record[0] != batchVersion {
		return nil, errors.New("invalid WAL batch header")
	}
	first := binary.LittleEndian.Uint64(record[1:])
	count := int(binary.LittleEndian.Uint32(record[9:]))
	p := record[batchHeader:]
	if count <= 0 || count > MaxOperations || count > len(p)/2 ||
		first == 0 || first > math.MaxUint64-uint64(count-1) {
		return nil, errors.New("invalid WAL batch count or sequence range")
	}
	entries := make([]skiplist.Entry, 0, count)
	for i := 0; i < count; i++ {
		if len(p) == 0 || p[0] > byte(skiplist.Put) {
			return nil, fmt.Errorf("invalid WAL operation %d", i)
		}
		entry := skiplist.Entry{Key: skiplist.Key{Seq: first + uint64(i), Kind: skiplist.Kind(p[0])}}
		var ok bool
		entry.User, p, ok = takeBytes(p[1:])
		if !ok {
			return nil, fmt.Errorf("invalid WAL key in operation %d", i)
		}
		if entry.Kind == skiplist.Put {
			if len(p) < 8 {
				return nil, fmt.Errorf("missing WAL expiry in operation %d", i)
			}
			entry.ExpiresAt = int64(binary.LittleEndian.Uint64(p))
			entry.Value, p, ok = takeBytes(p[8:])
			if !ok {
				return nil, fmt.Errorf("invalid WAL value in operation %d", i)
			}
		}
		entries = append(entries, entry)
	}
	if len(p) != 0 {
		return nil, errors.New("trailing bytes in WAL batch")
	}
	return entries, nil
}

func takeBytes(p []byte) (value, rest []byte, ok bool) {
	length, prefix := binary.Uvarint(p)
	if prefix <= 0 || length > uint64(len(p)-prefix) {
		return nil, nil, false
	}
	end := prefix + int(length)
	return p[prefix:end:end], p[end:], true
}
