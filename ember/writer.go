package ember

import (
	"errors"
	"fmt"
	"math"
	"time"

	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/version"
	"ember/internal/wal"
)

type request struct {
	batch  *Batch
	update func(*Tx) error
	reset  bool
	done   chan error
}

// Put writes a value and clears its prior expiry.
func (d *DB) Put(key, value []byte) error {
	var batch Batch
	batch.Put(key, value)
	return d.Write(&batch)
}

// Delete writes a tombstone for key.
func (d *DB) Delete(key []byte) error {
	var batch Batch
	batch.Delete(key)
	return d.Write(&batch)
}

// Write commits an atomic batch. Concurrent callers may share a WAL sync.
func (d *DB) Write(batch *Batch) error {
	if batch == nil {
		return errors.New("ember: nil batch")
	}
	if batch.err != nil {
		return batch.err
	}
	return d.submit(request{batch: batch})
}

func (d *DB) submit(r request) error {
	r.done = make(chan error, 1)
	d.admit.RLock()
	if d.closing {
		d.admit.RUnlock()
		return ErrClosed
	}
	d.queue <- r
	d.admit.RUnlock()
	return <-r.done
}

func (d *DB) writeLoop() {
	defer close(d.writerDone)
	var ticks <-chan time.Time
	if d.opts.SyncPolicy == SyncPeriodic {
		ticker := time.NewTicker(d.opts.SyncInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	dirty := false
	var carry *request
	for {
		select {
		case <-ticks:
			d.syncPeriodic(&dirty)
		default:
		}
		var first request
		if carry != nil {
			first = *carry
			carry = nil
		} else {
			select {
			case <-ticks:
				d.syncPeriodic(&dirty)
				continue
			case <-d.writerWake:
				if err := d.rotateIfNeeded(&dirty); err != nil {
					d.fail(err)
				}
				continue
			case r, ok := <-d.queue:
				if !ok {
					return
				}
				first = r
			}
		}
		group := []request{first}
		if first.batch != nil {
			group, carry = d.gather(group, first.batch.estimate)
		}
		if first.reset {
			first.done <- d.reset()
			dirty = false
			continue
		}
		err := d.waitWritable(&dirty)
		if err == nil {
			if first.update != nil {
				err = d.runUpdate(first.update, &dirty)
			} else {
				err = d.commitGroup(group, &dirty)
			}
		}
		for _, r := range group {
			r.done <- err
		}
		if err == nil {
			if err := d.rotateIfNeeded(&dirty); err != nil {
				d.fail(err)
			}
		}
	}
}

func (d *DB) gather(group []request, total int) ([]request, *request) {
	for len(group) < 32 {
		select {
		case r, ok := <-d.queue:
			if !ok {
				return group, nil
			}
			if r.batch == nil || r.batch.estimate > wal.MaxRecordSize-total {
				return group, &r
			}
			group = append(group, r)
			total += r.batch.estimate
		default:
			return group, nil
		}
	}
	return group, nil
}

func (d *DB) waitWritable(dirty *bool) error {
	slowed := false
	for {
		d.mu.Lock()
		fatal := d.fatal
		l0 := d.current.Version().FileCount(0)
		stall := l0 >= d.opts.L0Stop || len(d.immutable) >= d.opts.MaxImmutable && d.active.list.Bytes() >= d.opts.MemtableBytes
		if fatal != nil {
			d.mu.Unlock()
			return fatal
		}
		if !stall {
			d.mu.Unlock()
			if !slowed && l0 >= d.opts.L0Slowdown {
				d.counters.slowdowns.Add(1)
				time.Sleep(time.Millisecond)
				slowed = true
				continue
			}
			return d.rotateIfNeeded(dirty)
		}
		if *dirty && d.opts.SyncPolicy == SyncPeriodic {
			d.mu.Unlock()
			d.syncPeriodic(dirty)
			continue
		}
		d.counters.stalls.Add(1)
		d.changed.Wait()
		d.mu.Unlock()
	}
}

func (d *DB) syncPeriodic(dirty *bool) {
	if !*dirty {
		return
	}
	*dirty = false
	if err := d.walFile.Sync(); err != nil {
		d.fail(fmt.Errorf("sync periodic WAL: %w", err))
	}
}

func (d *DB) commitGroup(group []request, dirty *bool) error {
	d.mu.Lock()
	seq, fatal := d.sequence, d.fatal
	d.mu.Unlock()
	if fatal != nil {
		return fatal
	}
	records := make([][]byte, 0, len(group))
	entries := make([][]skiplist.Entry, 0, len(group))
	for _, r := range group {
		if len(r.batch.ops) == 0 {
			continue
		}
		if uint64(len(r.batch.ops)) > math.MaxUint64-seq {
			return errors.New("ember: sequences exhausted")
		}
		batch := append([]skiplist.Entry(nil), r.batch.ops...)
		for i := range batch {
			seq++
			batch[i].Seq = seq
		}
		record, err := wal.EncodeBatch(batch)
		if err != nil {
			return err
		}
		records = append(records, record)
		entries = append(entries, batch)
	}
	if len(records) == 0 {
		return nil
	}
	if err := d.walWriter.Append(records...); err != nil {
		d.fail(err)
		return err
	}
	*dirty = true
	if d.opts.SyncPolicy == SyncEveryWrite {
		if err := d.walFile.Sync(); err != nil {
			err = fmt.Errorf("sync WAL group: %w", err)
			d.fail(err)
			return err
		}
		*dirty = false
	}
	for _, batch := range entries {
		for _, entry := range batch {
			d.active.list.Insert(entry)
		}
	}
	// A reader capturing the old sequence skips the newly installed nodes.
	d.mu.Lock()
	d.sequence = seq
	d.mu.Unlock()
	d.counters.groups.Add(1)
	d.counters.batches.Add(uint64(len(records)))
	return nil
}

func (d *DB) rotateIfNeeded(dirty *bool) error {
	d.mu.Lock()
	ready := d.fatal == nil && len(d.immutable) < d.opts.MaxImmutable && d.active.list.Bytes() >= d.opts.MemtableBytes
	d.mu.Unlock()
	if !ready {
		return nil
	}
	if *dirty && d.opts.SyncPolicy == SyncPeriodic {
		d.syncPeriodic(dirty)
	}
	d.editMu.Lock()
	defer d.editMu.Unlock()
	old := d.active
	n, f, w, err := d.newWAL()
	if err != nil {
		return err
	}
	edit := manifest.Edit{AddWALs: []uint64{n}, ActiveWAL: n}
	err = d.commitEdit(edit, func() {
		d.immutable = append(d.immutable, old)
		d.active = memtable{n, &skiplist.List{}}
	})
	if err != nil {
		return errors.Join(err, f.Close())
	}
	d.swapWAL(f, w)
	*dirty = false
	return nil
}

// commitEdit requires editMu. The durable edit precedes publication. The
// old pin keeps its files alive until the published read view is replaced.
func (d *DB) commitEdit(edit manifest.Edit, update func()) error {
	d.mu.Lock()
	edit.NextFileNumber = d.nextFile
	edit.LastSequence = d.sequence
	fatal := d.fatal
	d.mu.Unlock()
	if fatal != nil {
		return fatal
	}
	if err := d.versions.Commit(edit, d.log.Append); err != nil {
		d.fail(err)
		return err
	}
	next := d.versions.Acquire()
	d.mu.Lock()
	old := d.current
	d.current = next
	update()
	d.changed.Broadcast()
	d.mu.Unlock()
	old.Release()
	d.signal()
	return nil
}

func (d *DB) currentPin() *version.Pin {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.current.Clone()
}

func (d *DB) signalWriter() {
	select {
	case d.writerWake <- struct{}{}:
	default:
	}
}
