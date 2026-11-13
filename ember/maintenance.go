package ember

import (
	"bytes"
	"errors"
	"path/filepath"

	"ember/internal/compaction"
	"ember/internal/manifest"
	"ember/internal/sstable"
	"ember/internal/version"
	"ember/internal/vfs"
)

type work struct {
	pin                  *version.Pin
	generation, sequence uint64
	at                   int64
	mem                  memtable
	l0                   int
}

func (d *DB) maintain() {
	defer close(d.maintenanceDone)
	for {
		select {
		case <-d.stop:
			return
		case <-d.wake:
		}
		for {
			select {
			case <-d.stop:
				return
			default:
			}
			d.reclaim()
			w, ok := d.chooseWork()
			if !ok {
				break
			}
			did, err := d.runWork(w)
			w.pin.Release()
			if err != nil {
				d.fail(err)
				d.opts.Logger.Error("Ember maintenance failed", "error", err)
				break
			}
			if !did {
				break
			}
		}
	}
}

func (d *DB) chooseWork() (work, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fatal != nil {
		return work{}, false
	}
	d.clockHigh = max(d.clockHigh, d.opts.Clock().UnixNano())
	w := work{
		pin: d.current.Clone(), generation: d.generation, sequence: d.sequence, at: d.clockHigh,
		l0: d.current.Version().FileCount(0),
	}
	if len(d.immutable) > 0 {
		w.mem = d.immutable[0]
	}
	for v := range d.views {
		w.sequence = min(w.sequence, v.seq)
		w.at = min(w.at, v.at)
	}
	return w, true
}

func (d *DB) runWork(w work) (bool, error) {
	if w.mem.list != nil && w.l0 < d.opts.L0Stop {
		return true, d.flush(w)
	}
	plan, ok := compaction.Pick(w.pin.Version(), d.opts.L0Trigger, d.opts.BaseLevelBytes, d.opts.Levels)
	if ok {
		return true, d.compact(w, plan)
	}
	if w.mem.list != nil {
		return true, d.flush(w)
	}
	return false, nil
}

type output struct {
	meta  manifest.FileMeta
	table tableFile
}

func (d *DB) buildTable(level int, fill func(*sstable.Builder) error) (output, error) {
	n, err := d.reserveFile()
	if err != nil {
		return output{}, err
	}
	path := filepath.Join(d.dir, tableName(n))
	opts := sstable.Options{BitsPerKey: d.opts.BloomBitsPerKey}
	if d.opts.Compression {
		opts.Codec = sstable.Deflate{}
	}
	var meta sstable.Meta
	err = vfs.Install(d.opts.FS, path, func(file vfs.File) error {
		b, err := sstable.NewBuilder(file, opts)
		if err != nil {
			return err
		}
		if err := fill(b); err != nil {
			return err
		}
		meta, err = b.Finish()
		return err
	})
	if err != nil {
		return output{}, err
	}
	m := manifest.FileMeta{Level: level, Number: n, Size: meta.Size, MinKey: meta.MinKey, MaxKey: meta.MaxKey, MinSeq: meta.MinSeq, MaxSeq: meta.MaxSeq}
	t, err := d.openTable(m)
	return output{m, t}, err
}

func (d *DB) flush(w work) error {
	var outputs []output
	if w.mem.list.Bytes() > 0 {
		out, err := d.buildTable(0, func(b *sstable.Builder) error {
			it := w.mem.list.NewIterator()
			for it.Next() {
				if err := b.Add(it.Entry()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		outputs = append(outputs, out)
	}
	installed, err := d.installOutputs(w, outputs, manifest.Edit{RemoveWALs: []uint64{w.mem.number}}, func() {
		d.immutable[0] = memtable{}
		d.immutable = d.immutable[1:]
	})
	if installed {
		d.removeLogged(walName(w.mem.number))
		d.counters.flushes.Add(1)
		d.signalWriter()
	}
	return err
}

func (d *DB) compact(w work, plan compaction.Plan) error {
	sources := make([]compaction.Source, 0, len(plan.Inputs))
	var edit manifest.Edit
	for _, f := range plan.Inputs {
		t, err := d.table(f.Number)
		if err != nil {
			return err
		}
		sources = append(sources, t.table.NewIterator())
		edit.Deleted = append(edit.Deleted, manifest.FileRef{Level: f.Level, Number: f.Number})
	}
	p := compaction.NewPruner(plan, compaction.NewMerge(sources), w.sequence, w.at)
	var outputs []output
	ok := p.Next()
	for ok {
		var size int64
		var last []byte
		out, err := d.buildTable(plan.OutputLevel, func(b *sstable.Builder) error {
			for ok {
				e := p.Entry()
				if size >= d.opts.TargetTableBytes && !bytes.Equal(last, e.User) {
					break
				}
				if err := b.Add(e); err != nil {
					return err
				}
				last = append(last[:0], e.User...)
				size += int64(len(e.User)) + int64(len(e.Value)) + 32
				ok = p.Next()
			}
			return p.Err()
		})
		if err != nil {
			d.discard(outputs, true)
			return err
		}
		outputs = append(outputs, out)
	}
	if err := p.Err(); err != nil {
		d.discard(outputs, true)
		return err
	}
	installed, err := d.installOutputs(w, outputs, edit, func() {})
	if installed {
		d.counters.compactions.Add(1)
	}
	return err
}

func (d *DB) installOutputs(w work, outputs []output, edit manifest.Edit, update func()) (bool, error) {
	d.editMu.Lock()
	defer d.editMu.Unlock()
	d.mu.Lock()
	stale := w.generation != d.generation
	d.mu.Unlock()
	if stale {
		d.discard(outputs, true)
		return false, nil
	}
	for _, out := range outputs {
		edit.Added = append(edit.Added, out.meta)
	}
	err := d.commitEdit(edit, func() {
		for _, out := range outputs {
			d.tables[out.meta.Number] = out.table
		}
		update()
	})
	if err != nil {
		// An ambiguous MANIFEST sync may have published these names.
		// Close our handles and leave the files for recovery to classify.
		d.discard(outputs, false)
		return false, err
	}
	return true, nil
}

func (d *DB) discard(outputs []output, unlink bool) {
	for _, out := range outputs {
		if err := out.table.file.Close(); err != nil {
			d.opts.Logger.Warn("close unused SSTable", "error", err)
		}
		if unlink {
			d.removeLogged(tableName(out.meta.Number))
		}
	}
}

func (d *DB) reclaim() {
	for _, number := range d.versions.TakeObsolete() {
		d.mu.Lock()
		t, ok := d.tables[number]
		delete(d.tables, number)
		d.mu.Unlock()
		if ok {
			if err := errors.Join(t.file.Close(), d.remove(tableName(number))); err != nil {
				d.opts.Logger.Warn("retire obsolete SSTable", "file", number, "error", err)
			}
		}
	}
}

func (d *DB) removeLogged(name string) {
	if err := d.remove(name); err != nil {
		d.opts.Logger.Warn("remove obsolete file", "error", err)
	}
}
