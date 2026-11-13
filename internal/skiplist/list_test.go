package skiplist_test

import (
	"encoding/binary"
	"sync"
	"testing"

	"ember/internal/skiplist"
)

func key(i uint64) []byte { return binary.LittleEndian.AppendUint64(nil, i) }

func TestSkiplistConcurrentView(t *testing.T) {
	var list skiplist.List
	const count = 1000
	for i := range uint64(count) {
		list.Insert(skiplist.Entry{Key: skiplist.Key{User: key(i), Seq: i + 1, Kind: skiplist.Put}})
	}
	var workers sync.WaitGroup
	for range 6 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			it := list.NewIterator()
			visible := 0
			for it.Next() {
				if it.Entry().Seq <= count {
					visible++
				}
			}
			if visible != count {
				t.Errorf("snapshot iteration saw %d entries", visible)
			}
			for i := range uint64(count) {
				entry, ok := list.Get(key(i), count)
				if !ok || entry.Seq != i+1 {
					t.Errorf("snapshot lookup returned %+v for %d", entry, i)
				}
			}
		}()
	}
	for i := range uint64(count) {
		list.Insert(skiplist.Entry{Key: skiplist.Key{User: key(i), Seq: i + count + 1, Kind: skiplist.Delete}})
	}
	workers.Wait()
}

func TestInternalKeyOrderAndOwnedBytes(t *testing.T) {
	var list skiplist.List
	user, value := []byte("a"), []byte("value")
	entry := skiplist.Entry{Key: skiplist.Key{User: user, Seq: 4, Kind: skiplist.Put}, Value: value}
	if !list.Insert(entry) || list.Insert(entry) {
		t.Fatal("duplicate insert semantics")
	}
	user[0], value[0] = 'z', 'z'
	if !list.Insert(skiplist.Entry{Key: skiplist.Key{User: []byte("a"), Seq: 9, Kind: skiplist.Delete}}) {
		t.Fatal("insert tombstone")
	}
	if got, ok := list.Get([]byte("a"), 8); !ok || got.Seq != 4 || string(got.Value) != "value" {
		t.Fatalf("old version %+v %t", got, ok)
	}
	if _, ok := list.Get([]byte("a"), 3); ok {
		t.Fatal("future entry returned")
	}
	it := list.NewIterator()
	if !it.Seek(skiplist.Key{User: []byte("a"), Seq: 9, Kind: skiplist.Delete}) || it.Entry().Kind != skiplist.Delete {
		t.Fatal("missing tombstone")
	}
	if !it.Next() || it.Entry().Seq != 4 || it.Next() {
		t.Fatal("internal versions out of order")
	}
}
