package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"ember/internal/skiplist"
	"ember/internal/vfs"
)

// ErrCorrupt means the WAL contains an invalid or torn logical record.
var ErrCorrupt = errors.New("corrupt WAL")

// Reader scans a fixed-size view of a WAL from its beginning.
type Reader struct {
	src        io.ReaderAt
	size       int64
	next       int64
	good       int64
	block      [BlockSize]byte
	blockStart int64
	blockLen   int
	err        error
}

// NewReader constructs a reader over size bytes. The caller owns src.
func NewReader(src io.ReaderAt, size int64) (*Reader, error) {
	if src == nil || size < 0 {
		return nil, errors.New("invalid WAL reader source or size")
	}
	return &Reader{src: src, size: size, blockStart: -1}, nil
}

// GoodOffset is the end of the last fully validated batch. After ErrCorrupt,
// a recovery caller may truncate to this offset. I/O errors are separate.
func (r *Reader) GoodOffset() int64 { return r.good }

// Next returns a complete batch, io.EOF, or a sticky error. The batch owns
// its record bytes independently of subsequent reads.
func (r *Reader) Next() ([]skiplist.Entry, error) {
	if r.err != nil {
		return nil, r.err
	}
	entries, err := r.readBatch()
	if err != nil {
		r.err = err
	}
	return entries, err
}

func (r *Reader) readBatch() ([]skiplist.Entry, error) {
	var record []byte
	start := r.next
	for r.next < r.size {
		if err := r.loadBlock(); err != nil {
			return nil, err
		}
		pos := int(r.next - r.blockStart)
		p := r.block[pos:r.blockLen]
		if BlockSize-pos <= headerSize {
			for _, b := range p {
				if b != 0 {
					return nil, corrupt(r.next, "invalid block padding")
				}
			}
			r.next += int64(len(p))
			if record == nil {
				start = r.next
			}
			continue
		}
		if len(p) < headerSize {
			return nil, corrupt(r.next, "short fragment header")
		}
		kind := p[6]
		length := int(binary.LittleEndian.Uint16(p[4:6]))
		if length == 0 || length > len(p)-headerSize {
			return nil, corrupt(r.next, "invalid fragment length")
		}
		payload := p[headerSize : headerSize+length]
		if binary.LittleEndian.Uint32(p) != checksum(kind, payload) {
			return nil, corrupt(r.next, "fragment checksum mismatch")
		}
		if record == nil && kind != full && kind != first ||
			record != nil && kind != middle && kind != last {
			return nil, corrupt(r.next, "invalid fragment transition")
		}
		if (kind == first || kind == middle) && pos+headerSize+length != BlockSize ||
			(kind == middle || kind == last) && pos != 0 {
			return nil, corrupt(r.next, "fragment at an invalid block position")
		}
		if length > MaxRecordSize-len(record) {
			return nil, corrupt(start, "record exceeds size limit")
		}
		record = append(record, payload...)
		r.next += int64(headerSize + length)
		if kind == full || kind == last {
			entries, err := DecodeBatch(record)
			if err != nil {
				return nil, corrupt(start, err.Error())
			}
			r.good = r.next
			return entries, nil
		}
	}
	if record != nil || r.next != r.good {
		return nil, corrupt(r.good, "unfinished record or padding")
	}
	return nil, io.EOF
}

func (r *Reader) loadBlock() error {
	start := r.next - r.next%BlockSize
	if start == r.blockStart {
		return nil
	}
	length := int(min(int64(BlockSize), r.size-start))
	n, err := r.src.ReadAt(r.block[:length], start)
	if n != length || err != nil && !errors.Is(err, io.EOF) {
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("read WAL block at %d: %w", start, err)
	}
	r.blockStart, r.blockLen = start, length
	return nil
}

func corrupt(offset int64, detail string) error {
	return fmt.Errorf("%w at offset %d: %s", ErrCorrupt, offset, detail)
}

// Recover replays complete batches and truncates and syncs a damaged tail.
// Call it with exclusive access and a writable file. The returned damage
// describes a successfully repaired tail, while err reports a failed recovery.
// A callback failure or underlying read error never triggers truncation.
func Recover(file vfs.File, apply func([]skiplist.Entry) error) (end int64, damage, err error) {
	info, err := file.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat WAL: %w", err)
	}
	reader, err := NewReader(file, info.Size())
	if err != nil {
		return 0, nil, err
	}
	for {
		entries, err := reader.Next()
		end = reader.GoodOffset()
		switch {
		case errors.Is(err, io.EOF):
			return end, nil, nil
		case errors.Is(err, ErrCorrupt):
			if truncateErr := file.Truncate(end); truncateErr != nil {
				return end, err, fmt.Errorf("truncate WAL to %d: %w", end, truncateErr)
			}
			if syncErr := file.Sync(); syncErr != nil {
				return end, err, fmt.Errorf("sync truncated WAL: %w", syncErr)
			}
			return end, err, nil
		case err != nil:
			return end, nil, err
		}
		if err := apply(entries); err != nil {
			return end, nil, fmt.Errorf("apply WAL batch ending at %d: %w", end, err)
		}
	}
}
