package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

const (
	// BlockSize is the physical WAL block size.
	BlockSize         = 32 << 10
	headerSize        = 7
	maxGroupSize      = 128 << 20
	full         byte = 1
	first        byte = 2
	middle       byte = 3
	last         byte = 4
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// Writer frames batches. One goroutine owns it and controls when the file
// is synced. An I/O error poisons the writer until recovery.
type Writer struct {
	dst    io.Writer
	offset int64
	err    error
}

// NewWriter starts at offset. Position dst at that offset before calling it.
func NewWriter(dst io.Writer, offset int64) (*Writer, error) {
	if dst == nil || offset < 0 {
		return nil, errors.New("invalid WAL writer destination or offset")
	}
	return &Writer{dst: dst, offset: offset}, nil
}

// Append frames encoded batches and writes the group in one Write call.
// Each batch remains a separate logical record during recovery.
func (w *Writer) Append(records ...[]byte) error {
	if w.err != nil {
		return w.err
	}
	total := 0
	for _, record := range records {
		if len(record) < batchHeader || len(record) > MaxRecordSize ||
			len(record) > maxGroupSize-total {
			return errors.New("invalid WAL record or group size")
		}
		total += len(record)
	}
	if total == 0 {
		return nil
	}
	if w.offset > math.MaxInt64-2*maxGroupSize {
		return errors.New("WAL offset exceeds size limit")
	}
	framed := make([]byte, 0, total)
	for _, record := range records {
		framed = frame(framed, record, w.offset)
	}
	n, err := w.dst.Write(framed)
	if err == nil && n != len(framed) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = fmt.Errorf("append WAL at offset %d: %w", w.offset, err)
		return w.err
	}
	w.offset += int64(n)
	return nil
}

func frame(dst, record []byte, base int64) []byte {
	started := false
	for len(record) > 0 {
		remaining := BlockSize - int((base+int64(len(dst)))%BlockSize)
		if remaining <= headerSize {
			dst = append(dst, make([]byte, remaining)...)
			remaining = BlockSize
		}
		size := min(len(record), remaining-headerSize)
		var kind byte
		switch {
		case !started && size == len(record):
			kind = full
		case !started:
			kind = first
		case size == len(record):
			kind = last
		default:
			kind = middle
		}
		payload := record[:size]
		dst = binary.LittleEndian.AppendUint32(dst, checksum(kind, payload))
		dst = binary.LittleEndian.AppendUint16(dst, uint16(size))
		dst = append(dst, kind)
		dst = append(dst, payload...)
		record = record[size:]
		started = true
	}
	return dst
}

func checksum(kind byte, payload []byte) uint32 {
	crc := crc32.Update(0, checksumTable, []byte{kind})
	return crc32.Update(crc, checksumTable, payload)
}
