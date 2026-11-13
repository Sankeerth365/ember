// Command emberd serves a single Ember database over RESP2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ember/ember"
	"ember/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("emberd stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := flag.String("addr", "127.0.0.1:6380", "TCP listen address")
	dir := flag.String("dir", "./ember-data", "database directory")
	mode := flag.String("sync", "always", "WAL sync policy (always, periodic, never)")
	interval := flag.Duration("sync-interval", 10*time.Millisecond, "periodic WAL sync interval")
	maxConn := flag.Int("max-connections", 128, "maximum concurrent connections")
	readTimeout := flag.Duration("read-timeout", 30*time.Second, "per-command read deadline")
	writeTimeout := flag.Duration("write-timeout", 10*time.Second, "per-reply write deadline")
	shutdown := flag.Duration("shutdown-timeout", 15*time.Second, "connection drain deadline")
	compress := flag.Bool("compress", false, "enable DEFLATE for new SSTables")
	flag.Parse()
	if flag.NArg() != 0 || *shutdown <= 0 {
		return errors.New("invalid emberd arguments")
	}
	var policy ember.SyncPolicy
	switch *mode {
	case "always":
		policy = ember.SyncEveryWrite
	case "periodic":
		policy = ember.SyncPeriodic
	case "never":
		policy = ember.SyncNever
	default:
		return fmt.Errorf("invalid WAL sync policy %q", *mode)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := ember.Open(*dir, &ember.Options{SyncPolicy: policy, SyncInterval: *interval, Compression: *compress, Logger: logger})
	if err != nil {
		return err
	}
	s, err := server.New(db, server.Options{MaxConnections: *maxConn, ReadTimeout: *readTimeout, WriteTimeout: *writeTimeout, Logger: logger})
	if err != nil {
		return errors.Join(err, db.Close())
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return errors.Join(err, db.Close())
	}
	logger.Info("emberd listening", "address", listener.Addr().String(), "dir", *dir, "sync", *mode)
	served := make(chan error, 1)
	go func() { served <- s.Serve(listener) }()
	select {
	case err = <-served:
	case <-ctx.Done():
	}
	deadline, cancel := context.WithTimeout(context.Background(), *shutdown)
	defer cancel()
	shutdownErr := s.Shutdown(deadline)
	// Close also handles a signal received before Serve claimed the listener.
	_ = listener.Close()
	return errors.Join(err, shutdownErr, db.Close())
}
