package sstable_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"ember/internal/cache"
	"ember/internal/skiplist"
	"ember/internal/sstable"
)

func put(user []byte, seq uint64, value []byte) skiplist.Entry {
	return skiplist.Entry{Key: skiplist.Key{User: user, Seq: seq, Kind: skiplist.Put}, Value: value, ExpiresAt: 100}
}

func TestTableRoundTrip(t *testing.T) {
	for _, codec := range []sstable.Codec{nil, sstable.Deflate{}} {
		var out bytes.Buffer
		b, err := sstable.NewBuilder(&out, sstable.Options{BlockSize: 220, RestartInterval: 2, Codec: codec})
		if err != nil {
			t.Fatal(err)
		}
		var entries []skiplist.Entry
		for i := 0; i < 250; i++ {
			user := []byte(fmt.Sprintf("shared-prefix-%04d", i))
			entries = append(entries, skiplist.Entry{Key: skiplist.Key{User: user, Seq: 9, Kind: skiplist.Delete}})
			for seq := uint64(8); seq > 0; seq-- {
				entries = append(entries, put(user, seq, bytes.Repeat([]byte{'x'}, 19)))
			}
		}
		for _, e := range entries {
			if err := b.Add(e); err != nil {
				t.Fatal(err)
			}
		}
		meta, err := b.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if int(meta.Size) != out.Len() || meta.Entries != uint64(len(entries)) || meta.Blocks < 2 ||
			meta.MinSeq != 1 || meta.MaxSeq != 9 {
			t.Fatalf("bad metadata %+v", meta)
		}
		table, err := sstable.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), 1, cache.New(1<<20, 8))
		if err != nil {
			t.Fatal(err)
		}
		if err := table.Verify(); err != nil {
			t.Fatal(err)
		}
		it := table.NewIterator()
		for i, want := range entries {
			if !it.Next() || !reflect.DeepEqual(it.Entry(), want) {
				t.Fatalf("next %d: %v", i, it.Err())
			}
			got, ok, err := table.Get(want.User, want.Seq)
			if err != nil || !ok || !reflect.DeepEqual(got, want) {
				t.Fatalf("get %d: %v", i, err)
			}
		}
		if it.Next() || it.Err() != nil {
			t.Fatalf("EOF %v", it.Err())
		}
		if _, ok, err := table.Get([]byte("missing"), math.MaxUint64); ok || err != nil {
			t.Fatalf("missing %v", err)
		}
		raw := bytes.Clone(out.Bytes())
		raw[0] ^= 1
		corrupt, err := sstable.Open(bytes.NewReader(raw), int64(len(raw)), 2, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := corrupt.Verify(); !errors.Is(err, sstable.ErrCorrupt) {
			t.Fatalf("block corruption %v", err)
		}
		raw[len(raw)-1] ^= 1
		if _, err := sstable.Open(bytes.NewReader(raw), int64(len(raw)), 2, nil); !errors.Is(err, sstable.ErrCorrupt) {
			t.Fatalf("footer corruption %v", err)
		}
	}
}
