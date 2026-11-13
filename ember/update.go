package ember

import (
	"errors"
	"time"

	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/vfs"
	"ember/internal/wal"
)

// Tx stages a serialized read-modify-write operation. It is valid only
// during the Update callback and is not safe for concurrent use.
type Tx struct {
	db     *DB
	view   *readView
	batch  Batch
	latest map[string]int
}

// Time is the fixed read and expiry timestamp for this update.
func (t *Tx) Time() time.Time { return time.Unix(0, t.view.at) }

// Get returns a visible record, including staged writes in this update.
func (t *Tx) Get(key []byte) (Record, error) {
	if i, ok := t.latest[string(key)]; ok {
		return visible(t.batch.ops[i], t.view.at)
	}
	return t.db.get(t.view, key)
}

// Put stages a value with an optional absolute expiry.
func (t *Tx) Put(key, value []byte, expiry time.Time) {
	t.batch.PutWithExpiry(key, value, expiry)
	if t.batch.err == nil {
		t.latest[string(key)] = len(t.batch.ops) - 1
	}
}

// Delete stages a tombstone.
func (t *Tx) Delete(key []byte) {
	t.batch.Delete(key)
	if t.batch.err == nil {
		t.latest[string(key)] = len(t.batch.ops) - 1
	}
}

// Update serializes a read-modify-write with other writers and commits its
// staged operations atomically. The callback must use Tx for reads and
// writes and must not call a database method that waits for this writer.
func (d *DB) Update(fn func(*Tx) error) error {
	if fn == nil {
		return errors.New("ember: missing update callback")
	}
	return d.submit(request{update: fn})
}

func (d *DB) runUpdate(fn func(*Tx) error, dirty *bool) error {
	v := d.capture()
	defer d.release(v)
	tx := &Tx{db: d, view: v, latest: make(map[string]int)}
	if err := fn(tx); err != nil {
		return err
	}
	if tx.batch.err != nil {
		return tx.batch.err
	}
	return d.commitGroup([]request{{batch: &tx.batch}}, dirty)
}

// Reset durably empties the current database in one version edit. Existing
// snapshots retain their pinned read views.
func (d *DB) Reset() error { return d.submit(request{reset: true}) }

func (d *DB) reset() error {
	d.editMu.Lock()
	defer d.editMu.Unlock()
	pin := d.currentPin()
	defer pin.Release()
	v := pin.Version()
	edit := manifest.Edit{RemoveWALs: v.ReplayWALs()}
	for level := 0; level < v.LevelCount(); level++ {
		for _, f := range v.Files(level) {
			edit.Deleted = append(edit.Deleted, manifest.FileRef{Level: level, Number: f.Number})
		}
	}
	n, f, w, err := d.newWAL()
	if err != nil {
		d.fail(err)
		return err
	}
	edit.AddWALs = []uint64{n}
	edit.ActiveWAL = n
	err = d.commitEdit(edit, func() {
		d.generation++
		d.active = memtable{n, &skiplist.List{}}
		d.immutable = nil
	})
	if err != nil {
		return errors.Join(err, f.Close())
	}
	d.swapWAL(f, w)
	for _, old := range edit.RemoveWALs {
		d.removeLogged(walName(old))
	}
	return nil
}

func (d *DB) newWAL() (uint64, vfs.File, *wal.Writer, error) {
	n, err := d.reserveFile()
	if err != nil {
		return 0, nil, nil, err
	}
	if err := d.installWAL(n); err != nil {
		return 0, nil, nil, err
	}
	f, err := d.openWAL(n)
	if err != nil {
		return 0, nil, nil, err
	}
	w, err := wal.NewWriter(f, 0)
	if err != nil {
		return 0, nil, nil, errors.Join(err, f.Close())
	}
	return n, f, w, nil
}

func (d *DB) swapWAL(f vfs.File, w *wal.Writer) {
	old := d.walFile
	d.walFile, d.walWriter = f, w
	if err := old.Close(); err != nil {
		d.opts.Logger.Warn("close sealed WAL", "error", err)
	}
}
