package wal_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"ember/internal/skiplist"
	"ember/internal/vfs"
	"ember/internal/wal"
)

func put(user []byte, seq uint64, value []byte) skiplist.Entry {
	return skiplist.Entry{Key: skiplist.Key{User: user, Seq: seq, Kind: skiplist.Put}, Value: value, ExpiresAt: 100}
}

func mustBatch(t *testing.T, entries ...skiplist.Entry) []byte {
	t.Helper()
	raw, err := wal.EncodeBatch(entries)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestWALCutsAndRepair(t *testing.T) {
	a := mustBatch(t, put([]byte{0, 'a'}, 1, []byte("old")))
	b := mustBatch(t, put([]byte("b"), 2, []byte("new")), put([]byte("c"), 3, []byte("batch")))
	var out bytes.Buffer
	w, err := wal.NewWriter(&out, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(a); err != nil {
		t.Fatal(err)
	}
	good := out.Len()
	if err := w.Append(b); err != nil {
		t.Fatal(err)
	}
	for end := good; end <= out.Len(); end++ {
		model := vfs.NewFaultFS()
		f, err := model.OpenFile("/1.wal", os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(out.Bytes()[:end]); err != nil {
			t.Fatal(err)
		}
		var last uint64
		offset, damage, err := wal.Recover(f, func(entries []skiplist.Entry) error {
			last = entries[len(entries)-1].Seq
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		partial := end > good && end < out.Len()
		if errors.Is(damage, wal.ErrCorrupt) != partial {
			t.Fatalf("cut %d: damage %v", end, damage)
		}
		wantLast, wantOffset := uint64(1), good
		if end == out.Len() {
			wantLast, wantOffset = 3, end
		}
		info, err := f.Stat()
		if err != nil || last != wantLast || offset != int64(wantOffset) || info.Size() != offset {
			t.Fatalf("cut %d: last=%d offset=%d info=%v err=%v", end, last, offset, info, err)
		}
	}
}

func TestWALGroupFragmentation(t *testing.T) {
	a := mustBatch(t, put([]byte("a"), 1, bytes.Repeat([]byte("a"), wal.BlockSize-7-3-13-1-1-1-8-3)))
	if len(a) != wal.BlockSize-10 {
		t.Fatalf("bad fixture len %d", len(a))
	}
	b := mustBatch(t, put([]byte("b"), 2, bytes.Repeat([]byte{0xfe}, 3*wal.BlockSize)))
	c := mustBatch(t, skiplist.Entry{Key: skiplist.Key{User: []byte("b"), Seq: 3, Kind: skiplist.Delete}})
	var out bytes.Buffer
	w, _ := wal.NewWriter(&out, 0)
	if err := w.Append(a, b, c); err != nil {
		t.Fatal(err)
	}
	r, _ := wal.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	for i, data := range [][]byte{a, b, c} {
		want, _ := wal.DecodeBatch(data)
		got, err := r.Next()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("EOF: %v", err)
	}
	broken := bytes.Clone(out.Bytes())
	broken[2*wal.BlockSize+9] ^= 1
	r, _ = wal.NewReader(bytes.NewReader(broken), int64(len(broken)))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, wal.ErrCorrupt) || r.GoodOffset() != wal.BlockSize-3 {
		t.Fatalf("offset=%d err=%v", r.GoodOffset(), err)
	}
}

func TestWALResumeAfterRepair(t *testing.T) {
	model := vfs.NewFaultFS()
	f, err := model.OpenFile("/log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := wal.NewWriter(f, 0)
	a := mustBatch(t, put([]byte("x"), 1, []byte("first")))
	b := mustBatch(t, put([]byte("x"), 2, []byte("second")))
	if err := w.Append(a, b); err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(stat.Size() - 1); err != nil {
		t.Fatal(err)
	}
	end, damage, err := wal.Recover(f, func([]skiplist.Entry) error { return nil })
	if err != nil || !errors.Is(damage, wal.ErrCorrupt) {
		t.Fatalf("repair %v %v", damage, err)
	}
	w, _ = wal.NewWriter(f, end)
	if err := w.Append(b); err != nil {
		t.Fatal(err)
	}
	var batches int
	_, damage, err = wal.Recover(f, func([]skiplist.Entry) error { batches++; return nil })
	if err != nil || damage != nil || batches != 2 {
		t.Fatalf("replay %d %v %v", batches, damage, err)
	}
}
