// Package manifest persists atomic changes to the database's live file set.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	editVersion          = 1
	maxFields            = 1 << 18
	maxPayload           = 128 << 20
	addTable        byte = 1
	deleteTable     byte = 2
	addWAL          byte = 3
	removeWAL       byte = 4
	setActiveWAL    byte = 5
	setNextFile     byte = 6
	setLastSequence byte = 7
)

// FileMeta describes an immutable SSTable and its inclusive user-key range.
type FileMeta struct {
	// Level is zero for overlapping tables and positive for sorted levels.
	Level int
	// Number uniquely identifies the file within the database.
	Number uint64
	// Size is the physical file size in bytes.
	Size uint64
	// MinSeq and MaxSeq bound the stored sequence numbers.
	MinSeq, MaxSeq uint64
	// MinKey and MaxKey are the inclusive user-key bounds.
	MinKey, MaxKey []byte
}

// FileRef identifies a table to remove from the live version.
type FileRef struct {
	// Level is the file's current level.
	Level int
	// Number is the file number.
	Number uint64
}

// Edit records one atomic change. Zero scalar fields mean no change.
// A new database implicitly has sequence zero.
type Edit struct {
	// Added and Deleted describe the changes to the live table set.
	Added   []FileMeta
	Deleted []FileRef
	// AddWALs and RemoveWALs change the set of WALs replayed at startup.
	AddWALs    []uint64
	RemoveWALs []uint64
	// ActiveWAL identifies the writable WAL, which must also be replayable.
	ActiveWAL uint64
	// NextFileNumber is the persisted allocation high-water mark.
	NextFileNumber uint64
	// LastSequence is the persisted sequence high-water mark.
	LastSequence uint64
}

func encode(edit Edit) ([]byte, error) {
	count := len(edit.Added) + len(edit.Deleted) + len(edit.AddWALs) + len(edit.RemoveWALs)
	for _, value := range []uint64{edit.ActiveWAL, edit.NextFileNumber, edit.LastSequence} {
		if value != 0 {
			count++
		}
	}
	if count == 0 || count > maxFields {
		return nil, errors.New("invalid MANIFEST field count")
	}
	p := binary.AppendUvarint([]byte{editVersion}, uint64(count))
	for _, f := range edit.Added {
		if f.Level < 0 || len(f.MinKey) > maxPayload || len(f.MaxKey) > maxPayload {
			return nil, errors.New("invalid MANIFEST table metadata")
		}
		size := varintSize(uint64(f.Level)) + 32 + varintSize(uint64(len(f.MinKey))) + len(f.MinKey) +
			varintSize(uint64(len(f.MaxKey))) + len(f.MaxKey)
		if size > maxPayload-len(p)-1-varintSize(uint64(size)) {
			return nil, errors.New("MANIFEST edit exceeds size limit")
		}
		p = field(p, addTable, size)
		p = binary.AppendUvarint(p, uint64(f.Level))
		for _, value := range []uint64{f.Number, f.Size, f.MinSeq, f.MaxSeq} {
			p = binary.LittleEndian.AppendUint64(p, value)
		}
		p = appendKey(p, f.MinKey)
		p = appendKey(p, f.MaxKey)
	}
	for _, f := range edit.Deleted {
		if f.Level < 0 {
			return nil, errors.New("invalid MANIFEST deletion level")
		}
		p = field(p, deleteTable, varintSize(uint64(f.Level))+8)
		p = binary.AppendUvarint(p, uint64(f.Level))
		p = binary.LittleEndian.AppendUint64(p, f.Number)
	}
	for _, number := range edit.AddWALs {
		if number == 0 {
			return nil, errors.New("zero WAL number")
		}
		p = scalar(p, addWAL, number)
	}
	for _, number := range edit.RemoveWALs {
		if number == 0 {
			return nil, errors.New("zero WAL number")
		}
		p = scalar(p, removeWAL, number)
	}
	if edit.ActiveWAL != 0 {
		p = scalar(p, setActiveWAL, edit.ActiveWAL)
	}
	if edit.NextFileNumber != 0 {
		p = scalar(p, setNextFile, edit.NextFileNumber)
	}
	if edit.LastSequence != 0 {
		p = scalar(p, setLastSequence, edit.LastSequence)
	}
	if len(p) > maxPayload {
		return nil, errors.New("MANIFEST edit exceeds size limit")
	}
	return p, nil
}

func varintSize(n uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], n)
}

func field(p []byte, tag byte, size int) []byte {
	return binary.AppendUvarint(append(p, tag), uint64(size))
}

func scalar(p []byte, tag byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(field(p, tag, 8), value)
}

func appendKey(p, key []byte) []byte {
	p = binary.AppendUvarint(p, uint64(len(key)))
	return append(p, key...)
}

func decode(p []byte) (Edit, error) {
	if len(p) < 2 || p[0] != editVersion {
		return Edit{}, errors.New("unsupported MANIFEST edit version")
	}
	count, n := binary.Uvarint(p[1:])
	if n <= 0 || count == 0 || count > maxFields || count > uint64(len(p)-1-n)/2 {
		return Edit{}, errors.New("invalid MANIFEST field count")
	}
	p = p[1+n:]
	var edit Edit
	for range count {
		if len(p) == 0 {
			return Edit{}, errors.New("missing MANIFEST field")
		}
		tag := p[0]
		value, rest, ok := take(p[1:])
		if !ok {
			return Edit{}, errors.New("invalid MANIFEST field length")
		}
		if err := decodeField(&edit, tag, value); err != nil {
			return Edit{}, fmt.Errorf("MANIFEST tag %d: %w", tag, err)
		}
		p = rest
	}
	if len(p) != 0 {
		return Edit{}, errors.New("trailing bytes in MANIFEST edit")
	}
	return edit, nil
}

func decodeField(e *Edit, tag byte, p []byte) error {
	switch tag {
	case addTable:
		f, err := decodeTable(p)
		if err != nil {
			return err
		}
		e.Added = append(e.Added, f)
		return nil
	case deleteTable:
		level, n := binary.Uvarint(p)
		if n <= 0 || level > uint64(^uint(0)>>1) || len(p)-n != 8 {
			return errors.New("invalid table deletion")
		}
		e.Deleted = append(e.Deleted, FileRef{int(level), binary.LittleEndian.Uint64(p[n:])})
		return nil
	}
	if len(p) != 8 {
		return errors.New("invalid scalar size or unknown tag")
	}
	value := binary.LittleEndian.Uint64(p)
	if value == 0 {
		return errors.New("zero file number or sequence")
	}
	var target *uint64
	switch tag {
	case addWAL:
		e.AddWALs = append(e.AddWALs, value)
	case removeWAL:
		e.RemoveWALs = append(e.RemoveWALs, value)
	case setActiveWAL:
		target = &e.ActiveWAL
	case setNextFile:
		target = &e.NextFileNumber
	case setLastSequence:
		target = &e.LastSequence
	default:
		return errors.New("unknown MANIFEST tag")
	}
	if target != nil {
		if *target != 0 {
			return errors.New("duplicate scalar")
		}
		*target = value
	}
	return nil
}

func decodeTable(p []byte) (FileMeta, error) {
	level, n := binary.Uvarint(p)
	if n <= 0 || level > uint64(^uint(0)>>1) || len(p)-n < 32 {
		return FileMeta{}, errors.New("invalid table metadata")
	}
	p = p[n:]
	f := FileMeta{
		Level:  int(level),
		Number: binary.LittleEndian.Uint64(p),
		Size:   binary.LittleEndian.Uint64(p[8:]),
		MinSeq: binary.LittleEndian.Uint64(p[16:]),
		MaxSeq: binary.LittleEndian.Uint64(p[24:]),
	}
	var ok bool
	f.MinKey, p, ok = take(p[32:])
	if !ok {
		return FileMeta{}, errors.New("invalid table minimum key")
	}
	f.MaxKey, p, ok = take(p)
	if !ok || len(p) != 0 {
		return FileMeta{}, errors.New("invalid table maximum key")
	}
	return f, nil
}

func take(p []byte) (value, rest []byte, ok bool) {
	length, n := binary.Uvarint(p)
	if n <= 0 || length > uint64(len(p)-n) {
		return nil, nil, false
	}
	end := n + int(length)
	return p[n:end:end], p[end:], true
}
