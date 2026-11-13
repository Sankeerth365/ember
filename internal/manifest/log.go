package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"ember/internal/vfs"
)

const (
	headerSize     = 8
	currentName    = "CURRENT"
	manifestPrefix = "MANIFEST-"
	maxCurrentSize = len(manifestPrefix) + 20 + 1
)

// ErrCorrupt identifies a complete invalid MANIFEST record or CURRENT file.
var ErrCorrupt = errors.New("corrupt MANIFEST")

// ErrIncomplete identifies a short final record that recovery can truncate.
var ErrIncomplete = errors.New("incomplete MANIFEST record")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Filename returns the canonical basename of a numbered MANIFEST.
func Filename(number uint64) string { return fmt.Sprintf("MANIFEST-%06d", number) }

// ReadCurrent resolves the canonical MANIFEST number in CURRENT.
func ReadCurrent(fsys vfs.FS, dir string) (uint64, error) {
	f, err := fsys.OpenFile(filepath.Join(dir, currentName), os.O_RDONLY, 0)
	if err != nil {
		return 0, fmt.Errorf("open CURRENT: %w", err)
	}
	number, readErr := readCurrent(f)
	return number, errors.Join(readErr, f.Close())
}

func readCurrent(f vfs.File) (uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat CURRENT: %w", err)
	}
	if info.Size() < int64(len(manifestPrefix)+2) || info.Size() > int64(maxCurrentSize) {
		return 0, fmt.Errorf("%w: invalid CURRENT size", ErrCorrupt)
	}
	var buf [maxCurrentSize]byte
	p := buf[:int(info.Size())]
	if err := readAt(f, p, 0); err != nil {
		return 0, fmt.Errorf("read CURRENT: %w", err)
	}
	name, ok := strings.CutSuffix(string(p), "\n")
	digits, hasPrefix := strings.CutPrefix(name, manifestPrefix)
	number, parseErr := strconv.ParseUint(digits, 10, 64)
	if !ok || !hasPrefix || parseErr != nil || number == 0 || Filename(number) != name {
		return 0, fmt.Errorf("%w: invalid CURRENT target", ErrCorrupt)
	}
	return number, nil
}

// Record is a complete edit and its byte offset in the MANIFEST.
type Record struct {
	// Offset is the beginning of the record header.
	Offset int64
	// Edit is the decoded version edit.
	Edit Edit
}

// Scan validates a fixed-size view of a MANIFEST. It returns the last good
// offset. A nil callback performs validation without applying edits.
func Scan(src io.ReaderAt, size int64, visit func(Record) error) (int64, error) {
	if src == nil || size < 0 {
		return 0, errors.New("invalid MANIFEST source or size")
	}
	var offset int64
	var header [headerSize]byte
	for offset < size {
		if size-offset < headerSize {
			return offset, fmt.Errorf("%w at offset %d: short header", ErrIncomplete, offset)
		}
		if err := readAt(src, header[:], offset); err != nil {
			return offset, fmt.Errorf("read MANIFEST header at %d: %w", offset, err)
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length == 0 || length > maxPayload {
			return offset, fmt.Errorf("%w at offset %d: invalid length", ErrCorrupt, offset)
		}
		if int64(length) > size-offset-headerSize {
			return offset, fmt.Errorf("%w at offset %d: short payload", ErrIncomplete, offset)
		}
		payload := make([]byte, int(length))
		if err := readAt(src, payload, offset+headerSize); err != nil {
			return offset, fmt.Errorf("read MANIFEST payload at %d: %w", offset, err)
		}
		if binary.LittleEndian.Uint32(header[4:]) != recordChecksum(header[:4], payload) {
			return offset, fmt.Errorf("%w at offset %d: checksum mismatch", ErrCorrupt, offset)
		}
		edit, err := decode(payload)
		if err != nil {
			return offset, fmt.Errorf("%w at offset %d: %v", ErrCorrupt, offset, err)
		}
		if visit != nil {
			if err := visit(Record{offset, edit}); err != nil {
				return offset, fmt.Errorf("apply MANIFEST record at %d: %w", offset, err)
			}
		}
		offset += headerSize + int64(length)
	}
	return offset, nil
}

func recordChecksum(length, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(length, crcTable), crcTable, payload)
}

func readAt(src io.ReaderAt, buf []byte, offset int64) error {
	n, err := src.ReadAt(buf, offset)
	if n == len(buf) && (err == nil || errors.Is(err, io.EOF)) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func writeRecord(dst io.Writer, payload []byte) error {
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf, uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:], recordChecksum(buf[:4], payload))
	copy(buf[headerSize:], payload)
	n, err := dst.Write(buf)
	if err == nil && n != len(buf) {
		return io.ErrShortWrite
	}
	return err
}

// Log owns the append handle for the active MANIFEST. A failed write or
// sync poisons the handle so a later append cannot hide a damaged tail.
type Log struct {
	mu   sync.Mutex
	file vfs.File
	err  error
}

// Create installs an initial MANIFEST and then installs CURRENT. The caller
// must hold the database lock and make referenced file names durable first.
func Create(fsys vfs.FS, dir string, number uint64, initial Edit) (*Log, error) {
	if number == 0 {
		return nil, errors.New("invalid MANIFEST number")
	}
	payload, err := encode(initial)
	if err != nil {
		return nil, err
	}
	name := Filename(number)
	path := filepath.Join(dir, name)
	current := filepath.Join(dir, currentName)
	for _, dest := range []string{current, path} {
		file, err := fsys.OpenFile(dest, os.O_RDONLY, 0)
		if err == nil {
			return nil, fmt.Errorf("create MANIFEST at %q: %w", dest, errors.Join(fs.ErrExist, file.Close()))
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("check MANIFEST destination %q: %w", dest, err)
		}
	}
	if err := vfs.Install(fsys, path, func(f vfs.File) error {
		return writeRecord(f, payload)
	}); err != nil {
		return nil, err
	}
	if err := vfs.Install(fsys, current, func(f vfs.File) error {
		p := []byte(name + "\n")
		n, err := f.Write(p)
		if err == nil && n != len(p) {
			return io.ErrShortWrite
		}
		return err
	}); err != nil {
		return nil, err
	}
	return openAppend(fsys, path)
}

func openAppend(fsys vfs.FS, path string) (*Log, error) {
	file, err := fsys.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, fmt.Errorf("open MANIFEST %q: %w", path, err)
	}
	return &Log{file: file}, nil
}

// Open replays CURRENT's MANIFEST and opens it for append. It repairs a
// short final record and reports whether it did so. Replayed metadata is
// synced before returning. The caller must hold the database lock and
// discard replayed state on error.
func Open(fsys vfs.FS, dir string, visit func(Record) error) (*Log, bool, error) {
	number, err := ReadCurrent(fsys, dir)
	if err != nil {
		return nil, false, err
	}
	log, err := openAppend(fsys, filepath.Join(dir, Filename(number)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, fmt.Errorf("%w: CURRENT points to missing %s", ErrCorrupt, Filename(number))
		}
		return nil, false, err
	}
	logError := func(err error) (*Log, bool, error) {
		return nil, false, errors.Join(err, log.Close())
	}
	info, err := log.file.Stat()
	if err != nil {
		return logError(fmt.Errorf("stat MANIFEST: %w", err))
	}
	// A visitor error must never be mistaken for a torn on-disk record.
	var visitErr error
	end, err := Scan(log.file, info.Size(), func(r Record) error {
		if visit == nil {
			return nil
		}
		visitErr = visit(r)
		return visitErr
	})
	if visitErr != nil || err != nil && !errors.Is(err, ErrIncomplete) {
		return logError(err)
	}
	if end == 0 {
		return logError(fmt.Errorf("%w: no complete initial edit", ErrCorrupt))
	}
	repaired := err != nil
	if repaired {
		if err := log.file.Truncate(end); err != nil {
			return logError(fmt.Errorf("truncate MANIFEST to %d: %w", end, err))
		}
	}
	// A failed sync can leave a complete edit visible to a second opener.
	// Make the adopted WAL references durable before accepting new writes.
	if err := log.file.Sync(); err != nil {
		return logError(fmt.Errorf("sync recovered MANIFEST: %w", err))
	}
	// A previous CURRENT rename may also be visible but unsynced.
	if err := fsys.SyncDir(dir); err != nil {
		return logError(fmt.Errorf("sync directory for recovered CURRENT: %w", err))
	}
	return log, repaired, nil
}

// Append appends and syncs one edit. Sync success is the commit boundary.
func (l *Log) Append(edit Edit) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return fs.ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	payload, err := encode(edit)
	if err != nil {
		return err
	}
	if err := writeRecord(l.file, payload); err != nil {
		l.err = fmt.Errorf("append MANIFEST: %w", err)
		return l.err
	}
	if err := l.file.Sync(); err != nil {
		l.err = fmt.Errorf("sync MANIFEST: %w", err)
		return l.err
	}
	return nil
}

// Close closes the append handle. It is safe to call more than once.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
