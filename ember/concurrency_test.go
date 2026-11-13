package ember_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"ember/ember"
)

func TestConcurrentGroupsAndUpdates(t *testing.T) {
	d := open(t, t.TempDir(), opts(nil))
	defer d.Close()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 100 {
				var b ember.Batch
				key := []byte(fmt.Sprintf("%d:%d", worker, n))
				b.Put(key, key)
				b.Put(append(bytes.Clone(key), 'x'), key)
				if e := d.Write(&b); e != nil {
					t.Error(e)
					return
				}
				if e := d.Update(func(tx *ember.Tx) error {
					r, e := tx.Get([]byte("count"))
					if e != nil && !errors.Is(e, ember.ErrNotFound) {
						return e
					}
					val, _ := strconv.Atoi(string(r.Value))
					tx.Put([]byte("count"), []byte(strconv.Itoa(val+1)), time.Time{})
					return nil
				}); e != nil {
					t.Error(e)
					return
				}
				if n%15 == 0 {
					s, e := d.NewSnapshot()
					if e != nil {
						t.Error(e)
						return
					}
					i, e := s.NewIterator(ember.IteratorOptions{Lower: key})
					if e == nil && !i.Next() {
						e = fmt.Errorf("missing iterator key: %v", i.Err())
					}
					if i != nil {
						i.Close()
					}
					s.Close()
					if e != nil {
						t.Error(e)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	get(t, d, "count", "800")
	s := d.Stats()
	t.Logf("groups=%d batches=%d flushes=%d compactions=%d", s.WriteGroups, s.WriteBatches, s.Flushes, s.Compactions)
	if s.WriteBatches != 1600 || s.WriteGroups >= s.WriteBatches || s.Compactions == 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestConcurrentSnapshotsNeverSplitBatch(t *testing.T) {
	d := open(t, t.TempDir(), opts(nil))
	defer d.Close()
	left, right := []byte("left"), []byte("right")
	write := func(value []byte) error {
		var batch ember.Batch
		batch.Put(left, value)
		batch.Put(right, value)
		return d.Write(&batch)
	}
	if err := write([]byte("initial")); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for step := range 250 {
				if worker < 2 {
					if err := write([]byte(fmt.Sprintf("%d:%d", worker, step))); err != nil {
						t.Error(err)
						return
					}
					continue
				}
				s, err := d.NewSnapshot()
				if err != nil {
					t.Error(err)
					return
				}
				a, aerr := s.Get(left)
				b, berr := s.Get(right)
				s.Close()
				if aerr != nil || berr != nil || !bytes.Equal(a, b) {
					t.Errorf("snapshot observed a split batch: left=%q (%v), right=%q (%v)", a, aerr, b, berr)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}
