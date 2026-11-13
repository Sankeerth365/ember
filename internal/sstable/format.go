// Package sstable implements immutable, checksummed sorted tables.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"ember/internal/skiplist"
)

const (
	footerSize    = 64
	trailerSize   = 5
	handleSize    = 16
	formatVersion = 1
	magic         = "EMBERSST"
	maxBlockSize  = 80 << 20
	keyTrailer    = 9
)

// ErrCorrupt identifies invalid table data or a checksum mismatch.
var ErrCorrupt = errors.New("corrupt SSTable")

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

type handle struct{ offset, length uint64 }

func (h handle) appendTo(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, h.offset)
	return binary.LittleEndian.AppendUint64(dst, h.length)
}

func parseHandle(p []byte) handle {
	return handle{binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint64(p[8:])}
}

type footer struct {
	index, filter   handle
	blocks, entries uint64
}

func (f footer) encode() []byte {
	buf := f.index.appendTo(make([]byte, 0, footerSize))
	buf = f.filter.appendTo(buf)
	buf = binary.LittleEndian.AppendUint64(buf, f.blocks)
	buf = binary.LittleEndian.AppendUint64(buf, f.entries)
	buf = binary.LittleEndian.AppendUint32(buf, formatVersion)
	buf = binary.LittleEndian.AppendUint32(buf, footerChecksum(buf, []byte(magic)))
	return append(buf, magic...)
}

func footerChecksum(prefix, suffix []byte) uint32 {
	return crc32.Update(crc32.Checksum(prefix, checksumTable), checksumTable, suffix)
}

func parseFooter(p []byte, end uint64) (footer, error) {
	if string(p[56:]) != magic || binary.LittleEndian.Uint32(p[48:]) != formatVersion ||
		binary.LittleEndian.Uint32(p[52:]) != footerChecksum(p[:52], p[56:]) {
		return footer{}, fmt.Errorf("%w: invalid footer", ErrCorrupt)
	}
	f := footer{
		index: parseHandle(p), filter: parseHandle(p[16:]),
		blocks: binary.LittleEndian.Uint64(p[32:]), entries: binary.LittleEndian.Uint64(p[40:]),
	}
	if f.blocks == 0 || f.entries < f.blocks ||
		!f.index.valid(end) || !f.filter.valid(end) ||
		f.index.offset+f.index.length != f.filter.offset ||
		f.filter.offset+f.filter.length != end {
		return footer{}, fmt.Errorf("%w: invalid footer handles or counts", ErrCorrupt)
	}
	return f, nil
}

func (h handle) valid(end uint64) bool {
	return h.offset <= end && h.length > trailerSize &&
		h.length <= maxBlockSize+trailerSize && h.length <= end-h.offset
}

func encodeKey(key skiplist.Key) []byte {
	buf := make([]byte, 0, len(key.User)+keyTrailer)
	buf = append(buf, key.User...)
	buf = binary.LittleEndian.AppendUint64(buf, key.Seq)
	return append(buf, byte(key.Kind))
}

func decodeKey(buf []byte) (skiplist.Key, bool) {
	if len(buf) < keyTrailer || buf[len(buf)-1] > byte(skiplist.Put) {
		return skiplist.Key{}, false
	}
	end := len(buf) - keyTrailer
	seq := binary.LittleEndian.Uint64(buf[end:])
	return skiplist.Key{User: buf[:end:end], Seq: seq, Kind: skiplist.Kind(buf[len(buf)-1])}, seq != 0
}

func readAt(src io.ReaderAt, p []byte, offset int64) error {
	n, err := src.ReadAt(p, offset)
	if n == len(p) && (err == nil || errors.Is(err, io.EOF)) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("read SSTable at %d: %w", offset, err)
}

func writeFull(dst io.Writer, p []byte) error {
	n, err := dst.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}
