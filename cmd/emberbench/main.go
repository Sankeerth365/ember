// Command emberbench times the embedded Ember API on repeatable workloads.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"ember/ember"
)

var allWorkloads = []string{"fillseq", "fillrandom", "readrandom", "readseq", "overwrite", "mixed"}

type config struct {
	ops, keys, warmup, workers, keySize, valueSize int
	seed                                           uint64
	policy                                         ember.SyncPolicy
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	var c config
	workloads := flag.String("workload", "all", "comma-separated workload names or all")
	parent := flag.String("dir", "", "parent for fresh benchmark databases (system temp by default)")
	keep := flag.Bool("keep", false, "retain the benchmark directories after the run")
	mode := flag.String("sync", "always", "WAL sync policy (always, periodic, never)")
	flag.IntVar(&c.ops, "ops", 10000, "timed operations per workload")
	flag.IntVar(&c.keys, "keys", 0, "prefilled keys for read and update workloads (default -ops)")
	flag.IntVar(&c.warmup, "warmup", 1000, "untimed operations per workload")
	flag.IntVar(&c.workers, "workers", 1, "concurrent timed workers")
	flag.IntVar(&c.keySize, "key-size", 16, "key size in bytes (minimum 8)")
	flag.IntVar(&c.valueSize, "value-size", 100, "value size in bytes")
	flag.Uint64Var(&c.seed, "seed", 42, "deterministic workload seed")
	flag.Parse()
	if c.keys == 0 {
		c.keys = c.ops
	}
	if flag.NArg() != 0 || c.ops < 1 || c.ops > 10_000_000 || c.keys < 1 || c.keys > 10_000_000 ||
		c.warmup < 0 || c.warmup > 10_000_000 || c.workers < 1 || c.workers > 256 ||
		c.keySize < 8 || c.keySize > 1<<20 || c.valueSize < 0 || c.valueSize > 1<<20 {
		return errors.New("invalid benchmark options")
	}
	switch *mode {
	case "always":
		c.policy = ember.SyncEveryWrite
	case "periodic":
		c.policy = ember.SyncPeriodic
	case "never":
		c.policy = ember.SyncNever
	default:
		return fmt.Errorf("invalid WAL sync policy %q", *mode)
	}
	selected, err := selectWorkloads(*workloads)
	if err != nil {
		return err
	}
	root, err := os.MkdirTemp(*parent, "ember-bench-")
	if err != nil {
		return err
	}
	if *keep {
		fmt.Fprintln(os.Stderr, "benchmark databases", root)
	} else {
		defer func() { err = errors.Join(err, os.RemoveAll(root)) }()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("sync=%s workers=%d warmup=%d keys=%d key_bytes=%d value_bytes=%d seed=%d\n",
		*mode, c.workers, c.warmup, c.keys, c.keySize, c.valueSize, c.seed)
	out := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	defer func() { err = errors.Join(err, out.Flush()) }()
	fmt.Fprintln(out, "workload\tops\tops/sec\tp50\tp95\tp99")
	for _, name := range selected {
		result, err := benchmark(ctx, filepath.Join(root, name), name, c)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Fprintf(out, "%s\t%d\t%.0f\t%s\t%s\t%s\n",
			name, c.ops, float64(c.ops)/result.elapsed.Seconds(),
			result.percentile(50).Round(time.Nanosecond),
			result.percentile(95).Round(time.Nanosecond),
			result.percentile(99).Round(time.Nanosecond))
	}
	return nil
}

func selectWorkloads(arg string) ([]string, error) {
	if arg == "all" {
		return allWorkloads, nil
	}
	selected := strings.Split(arg, ",")
	seen := make(map[string]bool, len(selected))
	for _, name := range selected {
		switch name {
		case "fillseq", "fillrandom", "readrandom", "readseq", "overwrite", "mixed":
		default:
			return nil, fmt.Errorf("unknown workload %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("repeated workload %q", name)
		}
		seen[name] = true
	}
	return selected, nil
}
