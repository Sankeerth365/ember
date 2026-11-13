// Command emberctl reads and verifies Ember's on-disk structures.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/sstable"
	"ember/internal/version"
	"ember/internal/vfs"
	"ember/internal/wal"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: emberctl wal FILE | sst FILE | manifest DIR | verify DIR")
		os.Exit(2)
	}
	out := bufio.NewWriter(os.Stdout)
	err := run(out, os.Args[1], os.Args[2])
	if err := errors.Join(err, out.Flush()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(out io.Writer, command, path string) error {
	switch command {
	case "wal":
		_, err := scanWAL(path, func(entries []skiplist.Entry) error {
			fmt.Fprintf(out, "batch first=%d operations=%d\n", entries[0].Seq, len(entries))
			for _, e := range entries {
				printEntry(out, e)
			}
			return nil
		})
		return err
	case "sst":
		return withTable(path, 0, func(t *sstable.Table, size int64) error {
			info := t.Info()
			fmt.Fprintf(out, "size=%d entries=%d blocks=%d bloom_bits=%d bloom_probes=%d\n",
				size, info.Entries, info.Blocks, info.BloomBits, info.BloomProbes)
			it := t.NewIterator()
			for it.Next() {
				e := it.Entry()
				fmt.Fprintf(out, "  seq=%d kind=%d key=%q expires_ns=%d value_bytes=%d\n", e.Seq, e.Kind, e.User, e.ExpiresAt, len(e.Value))
			}
			return it.Err()
		})
	case "manifest":
		number, err := manifest.ReadCurrent(vfs.OS{}, path)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, manifest.Filename(number))
		return scanManifest(path, number, func(r manifest.Record) error {
			e := r.Edit
			fmt.Fprintf(out, "offset=%d active_wal=%d next_file=%d last_sequence=%d add_wals=%v remove_wals=%v\n",
				r.Offset, e.ActiveWAL, e.NextFileNumber, e.LastSequence, e.AddWALs, e.RemoveWALs)
			for _, f := range e.Added {
				fmt.Fprintf(out, "  add level=%d file=%d bytes=%d seq=%d..%d keys=%q..%q\n",
					f.Level, f.Number, f.Size, f.MinSeq, f.MaxSeq, f.MinKey, f.MaxKey)
			}
			for _, f := range e.Deleted {
				fmt.Fprintf(out, "  delete level=%d file=%d\n", f.Level, f.Number)
			}
			return nil
		})
	case "verify":
		return verify(out, path)
	default:
		return fmt.Errorf("unknown emberctl command %q", command)
	}
}

func printEntry(out io.Writer, e skiplist.Entry) {
	if e.Kind == skiplist.Delete {
		fmt.Fprintf(out, "  seq=%d delete key=%q\n", e.Seq, e.User)
		return
	}
	fmt.Fprintf(out, "  seq=%d put key=%q expires_ns=%d value=%q\n", e.Seq, e.User, e.ExpiresAt, e.Value)
}

func withFile(path string, fn func(*os.File, int64) error) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if err := fn(f, stat.Size()); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func withTable(path string, number uint64, fn func(*sstable.Table, int64) error) error {
	return withFile(path, func(f *os.File, size int64) error {
		table, err := sstable.Open(f, size, number, nil)
		if err != nil {
			return err
		}
		return fn(table, size)
	})
}

func scanManifest(dir string, number uint64, visit func(manifest.Record) error) error {
	return withFile(filepath.Join(dir, manifest.Filename(number)), func(f *os.File, size int64) error {
		end, err := manifest.Scan(f, size, visit)
		if err == nil && end == 0 {
			return errors.New("empty MANIFEST")
		}
		return err
	})
}

func scanWAL(path string, visit func([]skiplist.Entry) error) (int, error) {
	batches := 0
	err := withFile(path, func(f *os.File, size int64) error {
		reader, err := wal.NewReader(f, size)
		if err != nil {
			return err
		}
		for {
			entries, err := reader.Next()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := visit(entries); err != nil {
				return err
			}
			batches++
		}
	})
	return batches, err
}

func verify(out io.Writer, dir string) (err error) {
	fsys := vfs.OS{}
	lock, err := fsys.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return fmt.Errorf("verify requires an offline database: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	number, err := manifest.ReadCurrent(fsys, dir)
	if err != nil {
		return err
	}
	set := version.NewSet()
	if err := scanManifest(dir, number, func(r manifest.Record) error { return set.Replay(r.Edit) }); err != nil {
		return err
	}
	pin := set.Acquire()
	defer pin.Release()
	v := pin.Version()
	var sequence uint64
	tables := 0
	for level := 0; level < v.LevelCount(); level++ {
		for _, meta := range v.Files(level) {
			path := filepath.Join(dir, fmt.Sprintf("%06d.sst", meta.Number))
			if err := withTable(path, meta.Number, func(t *sstable.Table, size int64) error {
				if uint64(size) != meta.Size {
					return errors.New("size differs from MANIFEST")
				}
				return t.Verify()
			}); err != nil {
				return err
			}
			sequence = max(sequence, meta.MaxSeq)
			tables++
		}
	}
	wals := v.ReplayWALs()
	total := 0
	for _, number := range wals {
		path := filepath.Join(dir, fmt.Sprintf("%06d.wal", number))
		batches, err := scanWAL(path, func(entries []skiplist.Entry) error {
			if entries[0].Seq <= sequence {
				return errors.New("non-increasing WAL sequence")
			}
			sequence = entries[len(entries)-1].Seq
			return nil
		})
		if err != nil {
			return err
		}
		total += batches
	}
	_, err = fmt.Fprintf(out, "OK manifest=%s tables=%d wals=%d wal_batches=%d\n", manifest.Filename(number), tables, len(wals), total)
	return err
}
