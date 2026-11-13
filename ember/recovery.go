package ember

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/sstable"
	"ember/internal/vfs"
	"ember/internal/wal"
)

func walName(n uint64) string   { return fmt.Sprintf("%06d.wal", n) }
func tableName(n uint64) string { return fmt.Sprintf("%06d.sst", n) }

func fileNumber(name string) (uint64, string) {
	if digits, ok := strings.CutPrefix(name, "MANIFEST-"); ok {
		n, err := strconv.ParseUint(digits, 10, 64)
		if err == nil && n != 0 && manifest.Filename(n) == name {
			return n, "manifest"
		}
	}
	for _, suffix := range []string{".wal", ".sst"} {
		if digits, ok := strings.CutSuffix(name, suffix); ok {
			n, err := strconv.ParseUint(digits, 10, 64)
			if err == nil && n != 0 && fmt.Sprintf("%06d%s", n, suffix) == name {
				return n, suffix
			}
		}
	}
	return 0, ""
}

func ensureDir(fsys vfs.FS, path string) error {
	parent := filepath.Dir(path)
	if _, err := fsys.ReadDir(path); err == nil {
		// A prior mkdir might have succeeded before its parent sync failed.
		return fsys.SyncDir(parent)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	err := fsys.Mkdir(path, 0700)
	if errors.Is(err, fs.ErrNotExist) && parent != path {
		if err := ensureDir(fsys, parent); err != nil {
			return err
		}
		return ensureDir(fsys, path)
	}
	if err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
		if _, err := fsys.ReadDir(path); err != nil {
			return err
		}
	}
	return fsys.SyncDir(parent)
}

func (d *DB) recover() error {
	fsys := d.opts.FS
	if err := ensureDir(fsys, d.dir); err != nil {
		return err
	}
	lock, err := fsys.Lock(filepath.Join(d.dir, "LOCK"))
	if err != nil {
		return err
	}
	d.lock = lock
	names, err := fsys.ReadDir(d.dir)
	if err != nil {
		return err
	}
	d.nextFile = 1
	for _, file := range names {
		n, _ := fileNumber(file.Name())
		if n == math.MaxUint64 {
			return errors.New("file numbers exhausted")
		}
		d.nextFile = max(d.nextFile, n+1)
	}
	log, repaired, err := manifest.Open(fsys, d.dir, func(r manifest.Record) error { return d.versions.Replay(r.Edit) })
	if errors.Is(err, fs.ErrNotExist) {
		log, err = d.bootstrap(names)
	}
	if err != nil {
		return err
	}
	d.log = log
	if repaired {
		d.opts.Logger.Warn("repaired incomplete MANIFEST tail", "path", d.dir)
	}
	d.current = d.versions.Acquire()
	v := d.current.Version()
	d.nextFile = max(d.nextFile, v.NextFileNumber())
	d.sequence = v.LastSequence()
	var replaySequence uint64
	for level := 0; level < v.LevelCount(); level++ {
		for _, meta := range v.Files(level) {
			table, err := d.openTable(meta)
			if err != nil {
				return err
			}
			d.tables[meta.Number] = table
			replaySequence = max(replaySequence, meta.MaxSeq)
		}
	}
	for _, number := range v.ReplayWALs() {
		if err := d.recoverWAL(number, number == v.ActiveWAL(), &replaySequence); err != nil {
			return err
		}
	}
	d.sequence = max(d.sequence, replaySequence)
	return d.cleanStartup(names, v.ReplayWALs())
}

func (d *DB) bootstrap(names []fs.DirEntry) (*manifest.Log, error) {
	// A failed initial install can leave an empty WAL and MANIFEST.
	// Existing table data or WAL records require explicit recovery.
	for _, entry := range names {
		_, kind := fileNumber(entry.Name())
		if kind == ".sst" {
			return nil, errors.New("CURRENT missing in directory containing SSTables")
		}
		if kind == ".wal" {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if info.Size() != 0 {
				return nil, errors.New("CURRENT missing in directory containing WAL data")
			}
		}
	}
	id, err := d.reserveFile()
	if err != nil {
		return nil, err
	}
	number, err := d.reserveFile()
	if err != nil {
		return nil, err
	}
	if err := d.installWAL(number); err != nil {
		return nil, err
	}
	edit := manifest.Edit{AddWALs: []uint64{number}, ActiveWAL: number, NextFileNumber: d.nextFile}
	if err := d.versions.Replay(edit); err != nil {
		return nil, err
	}
	return manifest.Create(d.opts.FS, d.dir, id, edit)
}

func (d *DB) reserveFile() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.nextFile == math.MaxUint64 {
		return 0, errors.New("file numbers exhausted")
	}
	n := d.nextFile
	d.nextFile++
	return n, nil
}

func (d *DB) installWAL(number uint64) error {
	return vfs.Install(d.opts.FS, filepath.Join(d.dir, walName(number)), func(vfs.File) error { return nil })
}

func (d *DB) openWAL(number uint64) (vfs.File, error) {
	return d.opts.FS.OpenFile(filepath.Join(d.dir, walName(number)), os.O_RDWR|os.O_APPEND, 0)
}

func (d *DB) recoverWAL(number uint64, active bool, seq *uint64) error {
	f, err := d.openWAL(number)
	if err != nil {
		return fmt.Errorf("open WAL %d: %w", number, err)
	}
	m := memtable{number, &skiplist.List{}}
	end, damage, err := wal.Recover(f, func(entries []skiplist.Entry) error {
		if entries[0].Seq <= *seq {
			return errors.New("non-increasing WAL sequence")
		}
		for _, entry := range entries {
			m.list.Insert(entry)
		}
		*seq = entries[len(entries)-1].Seq
		return nil
	})
	if err != nil {
		return errors.Join(fmt.Errorf("recover WAL %d: %w", number, err), f.Close())
	}
	if damage != nil {
		d.opts.Logger.Warn("repaired WAL tail", "file", number, "offset", end, "error", damage)
	}
	if !active {
		d.immutable = append(d.immutable, m)
		return f.Close()
	}
	w, err := wal.NewWriter(f, end)
	if err != nil {
		return errors.Join(err, f.Close())
	}
	d.active, d.walFile, d.walWriter = m, f, w
	return nil
}

func (d *DB) openTable(meta manifest.FileMeta) (tableFile, error) {
	f, err := d.opts.FS.OpenFile(filepath.Join(d.dir, tableName(meta.Number)), os.O_RDONLY, 0)
	if err != nil {
		return tableFile{}, fmt.Errorf("open SSTable %d: %w", meta.Number, err)
	}
	info, err := f.Stat()
	if err == nil && uint64(info.Size()) != meta.Size {
		err = errors.New("size differs from MANIFEST")
	}
	if err != nil {
		return tableFile{}, errors.Join(fmt.Errorf("stat SSTable %d: %w", meta.Number, err), f.Close())
	}
	t, err := sstable.Open(f, info.Size(), meta.Number, d.cache)
	if err != nil {
		return tableFile{}, errors.Join(fmt.Errorf("open SSTable %d: %w", meta.Number, err), f.Close())
	}
	return tableFile{f, t}, nil
}

func (d *DB) cleanStartup(names []fs.DirEntry, wals []uint64) error {
	current, err := manifest.ReadCurrent(d.opts.FS, d.dir)
	if err != nil {
		return err
	}
	liveWAL := make(map[uint64]bool, len(wals))
	for _, n := range wals {
		liveWAL[n] = true
	}
	for _, file := range names {
		n, kind := fileNumber(file.Name())
		_, table := d.tables[n]
		if kind == ".sst" && !table || kind == ".wal" && !liveWAL[n] || kind == "manifest" && n != current ||
			strings.HasPrefix(file.Name(), ".") && strings.HasSuffix(file.Name(), ".tmp") && isInstallTemp(file.Name()) {
			if err := d.remove(file.Name()); err != nil {
				return err
			}
		}
	}
	d.versions.TakeObsolete()
	return nil
}

func isInstallTemp(name string) bool {
	body := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".tmp")
	split := strings.LastIndexByte(body, '.')
	if split < 0 {
		return false
	}
	if _, err := strconv.ParseUint(body[split+1:], 10, 64); err != nil {
		return false
	}
	dest := body[:split]
	if dest == "CURRENT" {
		return true
	}
	_, kind := fileNumber(dest)
	return kind != ""
}

func (d *DB) remove(name string) error {
	err := d.opts.FS.Remove(filepath.Join(d.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove %q: %w", name, err)
	}
	return nil
}

func (d *DB) syncReplayWALs() error {
	var err error
	for _, number := range d.current.Version().ReplayWALs() {
		if number == d.active.number {
			err = errors.Join(err, d.walFile.Sync())
			continue
		}
		f, openErr := d.openWAL(number)
		if openErr != nil {
			err = errors.Join(err, openErr)
			continue
		}
		err = errors.Join(err, f.Sync(), f.Close())
	}
	return err
}
