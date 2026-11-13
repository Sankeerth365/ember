package ember_test

import (
	"fmt"
	"maps"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"

	"ember/ember"
)

type snapshotModel struct {
	db      *ember.Snapshot
	entries map[string]string
}

func TestRandomizedModel(t *testing.T) {
	const operations = 60_000
	for _, seed := range []int64{3, 42, 101, 1201} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			path := filepath.Join(t.TempDir(), "nested", "db")
			o := opts(nil)
			o.SyncPolicy = ember.SyncNever
			o.MemtableBytes = 24 << 10
			o.BaseLevelBytes = 128 << 10
			o.TargetTableBytes = 16 << 10
			db := open(t, path, o)
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			model := make(map[string]string)
			keys := make([]string, 512)
			for i := 1; i < len(keys); i++ {
				keys[i] = string([]byte{byte(i >> 8), byte(i), 255})
			}
			var snapshots []snapshotModel
			for step := range operations {
				k := keys[rng.Intn(len(keys))]
				switch roll := rng.Intn(1000); {
				case roll < 370:
					value := fmt.Sprintf("%d:%d", seed, step)
					if err := db.Put([]byte(k), []byte(value)); err != nil {
						t.Fatalf("step %d: %v", step, err)
					}
					model[k] = value
				case roll < 580:
					if err := db.Delete([]byte(k)); err != nil {
						t.Fatalf("step %d: %v", step, err)
					}
					delete(model, k)
				case roll < 680:
					other := keys[rng.Intn(len(keys))]
					value := fmt.Sprint(step)
					var b ember.Batch
					b.Put([]byte(k), []byte(value))
					b.Delete([]byte(other))
					if err := db.Write(&b); err != nil {
						t.Fatalf("step %d: %v", step, err)
					}
					model[k] = value
					delete(model, other)
				case roll < 955:
					checkPoint(t, db, model, k)
				case roll < 977:
					checkRange(t, db, model, keys, rng)
				case roll < 988:
					if len(snapshots) < 4 {
						s, err := db.NewSnapshot()
						if err != nil {
							t.Fatal(err)
						}
						snapshots = append(snapshots, snapshotModel{s, maps.Clone(model)})
					}
				default:
					if len(snapshots) > 0 {
						s := snapshots[rng.Intn(len(snapshots))]
						checkPoint(t, s.db, s.entries, k)
						checkRange(t, s.db, s.entries, keys, rng)
					}
				}
				if step%4000 == 3999 {
					for _, s := range snapshots {
						checkScan(t, s.db, s.entries, ember.IteratorOptions{}, nil)
						if err := s.db.Close(); err != nil {
							t.Fatal(err)
						}
					}
					snapshots = nil
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db = open(t, path, o)
					checkScan(t, db, model, ember.IteratorOptions{}, nil)
				}
			}
		})
	}
}

type pointReader interface{ Get([]byte) ([]byte, error) }
type scanReader interface {
	NewIterator(ember.IteratorOptions) (*ember.Iterator, error)
}

func checkPoint(t *testing.T, r pointReader, model map[string]string, key string) {
	t.Helper()
	if want, ok := model[key]; ok {
		get(t, r, key, want)
	} else {
		missing(t, r, key)
	}
}

func checkRange(t *testing.T, r scanReader, model map[string]string, keys []string, rng *rand.Rand) {
	t.Helper()
	a, b := rng.Intn(len(keys)), rng.Intn(len(keys))
	var opts ember.IteratorOptions
	opts.Lower = []byte(keys[min(a, b)])
	opts.Upper = []byte(keys[max(a, b)])
	var seek []byte
	if rng.Intn(2) == 0 {
		seek = []byte(keys[rng.Intn(len(keys))])
	}
	checkScan(t, r, model, opts, seek)
}

func checkScan(t *testing.T, r scanReader, model map[string]string, opts ember.IteratorOptions, seek []byte) {
	t.Helper()
	it, err := r.NewIterator(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	want := make([]string, 0, len(model))
	for key := range model {
		if (opts.Lower == nil || key >= string(opts.Lower)) && (opts.Upper == nil || key < string(opts.Upper)) &&
			(seek == nil || key >= string(seek)) {
			want = append(want, key)
		}
	}
	slices.Sort(want)
	var ok bool
	if seek != nil {
		ok = it.Seek(seek)
	} else {
		ok = it.Next()
	}
	for _, key := range want {
		if !ok || string(it.Key()) != key || string(it.Value()) != model[key] {
			t.Fatalf("want %q=%q, got %t %q=%q (%v)", key, model[key], ok, it.Key(), it.Value(), it.Err())
		}
		ok = it.Next()
	}
	if ok || it.Err() != nil {
		t.Fatalf("expected iterator end, got %t %q (%v)", ok, it.Key(), it.Err())
	}
}
