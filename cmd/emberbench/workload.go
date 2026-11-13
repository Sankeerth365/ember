package main

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"ember/ember"
)

type results struct {
	elapsed time.Duration
	latency []time.Duration
}

func (r results) percentile(p int) time.Duration {
	return r.latency[(p*len(r.latency)+99)/100-1]
}

func benchmark(ctx context.Context, path, name string, c config) (r results, err error) {
	db, err := ember.Open(path, &ember.Options{SyncPolicy: c.policy})
	if err != nil {
		return results{}, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	fill := name == "fillseq" || name == "fillrandom"
	if !fill {
		if err := prefill(ctx, db, c); err != nil {
			return results{}, err
		}
	}
	warm := newWorker(c, c.seed^0x1234)
	for i := 0; i < c.warmup; i++ {
		if err := ctx.Err(); err != nil {
			return results{}, err
		}
		index := i
		if fill {
			index += c.ops
		}
		if err := warm.step(db, name, index, nil); err != nil {
			return results{}, err
		}
	}
	var permutation []int
	if name == "fillrandom" {
		permutation = rand.New(rand.NewPCG(c.seed, 1)).Perm(c.ops)
	}
	return measure(ctx, db, name, c, permutation)
}

type worker struct {
	cfg        config
	key, value []byte
	random     *rand.Rand
}

func newWorker(c config, seed uint64) *worker {
	return &worker{c, make([]byte, c.keySize), make([]byte, c.valueSize), rand.New(rand.NewPCG(seed, 0x656d626572))}
}

func (w *worker) step(db *ember.DB, name string, index int, permutation []int) error {
	id := index
	write := false
	switch name {
	case "fillrandom":
		if permutation != nil {
			id = permutation[index]
		}
		write = true
	case "fillseq":
		write = true
	case "readseq":
		id = index % w.cfg.keys
	case "readrandom":
		id = w.random.IntN(w.cfg.keys)
	case "overwrite":
		id = w.random.IntN(w.cfg.keys)
		write = true
	case "mixed":
		id = w.random.IntN(w.cfg.keys)
		write = index%10 == 0
	}
	binary.BigEndian.PutUint64(w.key, uint64(id))
	if write {
		if len(w.value) > 0 {
			w.value[0] = byte(index)
		}
		return db.Put(w.key, w.value)
	}
	_, err := db.Get(w.key)
	return err
}

func prefill(ctx context.Context, db *ember.DB, c config) error {
	w := newWorker(c, c.seed)
	batchSize := min(256, max(1, (4<<20)/(c.keySize+c.valueSize+32)))
	for i := 0; i < c.keys; i += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		var b ember.Batch
		for j := i; j < min(i+batchSize, c.keys); j++ {
			binary.BigEndian.PutUint64(w.key, uint64(j))
			b.Put(w.key, w.value)
		}
		if err := db.Write(&b); err != nil {
			return err
		}
	}
	return nil
}

func measure(ctx context.Context, db *ember.DB, name string, c config, permutation []int) (results, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	latencies := make([]time.Duration, c.ops)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	for id := 0; id < c.workers; id++ {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			w := newWorker(c, c.seed+uint64(id)+1)
			ready.Done()
			<-start
			for i := id; i < c.ops; i += c.workers {
				if ctx.Err() != nil {
					return
				}
				then := time.Now()
				err := w.step(db, name, i, permutation)
				latencies[i] = time.Since(then)
				if err != nil {
					cancel(err)
					return
				}
			}
		}()
	}
	ready.Wait()
	then := time.Now()
	close(start)
	done.Wait()
	elapsed := time.Since(then)
	if err := context.Cause(ctx); err != nil {
		return results{}, err
	}
	slices.Sort(latencies)
	return results{elapsed, latencies}, nil
}
