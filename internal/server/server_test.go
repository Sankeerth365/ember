package server_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ember/ember"
	"ember/internal/server"
	"ember/internal/vfs"
)

type client struct {
	net.Conn
	r *bufio.Reader
}

type replyError string

func connect(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return &client{c, bufio.NewReader(c)}
}

func wire(args []string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&out, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return out.String()
}

func (c *client) send(t *testing.T, commands ...[]string) {
	t.Helper()
	var all strings.Builder
	for _, command := range commands {
		all.WriteString(wire(command))
	}
	if _, err := io.WriteString(c.Conn, all.String()); err != nil {
		t.Fatal(err)
	}
}

func (c *client) check(t *testing.T, want any) {
	t.Helper()
	got, err := readReply(c.r)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("reply=%#v err=%v want=%#v", got, err, want)
	}
}

func readReply(r *bufio.Reader) (any, error) {
	raw, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(raw) < 3 || !strings.HasSuffix(raw, "\r\n") {
		return nil, fmt.Errorf("invalid reply header %q", raw)
	}
	line := raw[1 : len(raw)-2]
	switch raw[0] {
	case '+':
		return line, nil
	case '-':
		return replyError(line), nil
	case ':':
		return strconv.ParseInt(line, 10, 64)
	case '$', '*':
		n, err := strconv.Atoi(line)
		if err != nil || n < -1 || n > 1<<20 {
			return nil, fmt.Errorf("invalid reply length %q", line)
		}
		if n == -1 {
			return nil, nil
		}
		if raw[0] == '$' {
			buf := make([]byte, n+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			if string(buf[n:]) != "\r\n" {
				return nil, errors.New("missing bulk terminator")
			}
			return string(buf[:n]), nil
		}
		list := make([]any, n)
		for i := range list {
			list[i], err = readReply(r)
			if err != nil {
				return nil, err
			}
		}
		return list, nil
	default:
		return nil, fmt.Errorf("unexpected reply %q", raw)
	}
}

func launch(t *testing.T, dbOpts ember.Options, serverOpts server.Options) (*server.Server, string) {
	t.Helper()
	dbOpts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	serverOpts.Logger = dbOpts.Logger
	db, err := ember.Open(t.TempDir(), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := server.New(db, serverOpts)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		_ = ln.Close()
		if err := <-served; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	return s, ln.Addr().String()
}

func TestTCPCommandsAndTransactions(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	_, addr := launch(t, ember.Options{Clock: func() time.Time { return time.Unix(0, clock.Load()) }}, server.Options{})
	c := connect(t, addr)
	c.send(t, []string{"PING"}, []string{"ECHO", "x\x00\r\ny"}, []string{"MSET", "a", "1", "b", ""},
		[]string{"MGET", "a", "b", "missing"}, []string{"EXISTS", "a", "a", "missing"}, []string{"SETNX", "a", "new"})
	for _, want := range []any{"PONG", "x\x00\r\ny", "OK", []any{"1", "", nil}, int64(2), int64(0)} {
		c.check(t, want)
	}

	c.send(t, []string{"SET", "temporary", "yes", "PX", "100"}, []string{"TTL", "temporary"})
	c.check(t, "OK")
	c.check(t, int64(0))
	clock.Add(int64(time.Second))
	c.send(t, []string{"GET", "temporary"}, []string{"TTL", "temporary"}, []string{"EXPIRE", "a", "60"}, []string{"TTL", "a"})
	for _, want := range []any{nil, int64(-2), int64(1), int64(60)} {
		c.check(t, want)
	}

	c.send(t, []string{"MULTI"}, []string{"INCRBY", "a", "4"}, []string{"INCR", "b"}, []string{"GET", "a"},
		[]string{"SET", "c", "ok"}, []string{"EXEC"}, []string{"KEYS", "c*"})
	for _, want := range []any{"OK", "QUEUED", "QUEUED", "QUEUED", "QUEUED",
		[]any{int64(5), replyError("ERR value is not an integer or out of range"), "5", "OK"}, []any{"c"}} {
		c.check(t, want)
	}
	c.send(t, []string{"MULTI"}, []string{"SET", "discard", "value"}, []string{"DISCARD"}, []string{"GET", "discard"},
		[]string{"MULTI"}, []string{"SET", "abort", "value"}, []string{"GET"}, []string{"EXEC"}, []string{"GET", "abort"})
	for _, want := range []any{"OK", "QUEUED", "OK", nil, "OK", "QUEUED",
		replyError("ERR wrong number of arguments for 'get' command"),
		replyError("EXECABORT Transaction discarded because of previous errors."), nil} {
		c.check(t, want)
	}

	c.send(t, []string{"SCAN", "0", "COUNT", "2"})
	raw, err := readReply(c.r)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := raw.([]any)
	if !ok || len(first) != 2 || first[0] == "0" || !reflect.DeepEqual(first[1], []any{"a", "b"}) {
		t.Fatalf("scan=%#v", raw)
	}
	c.send(t, []string{"SCAN", fmt.Sprint(first[0]), "COUNT", "2"}, []string{"DBSIZE"}, []string{"FLUSHALL"}, []string{"DBSIZE"}, []string{"QUIT"})
	for _, want := range []any{[]any{"0", []any{"c"}}, int64(3), "OK", int64(0), "OK"} {
		c.check(t, want)
	}
	if _, err := c.r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("quit: %v", err)
	}
}

func TestTCPProtocolFailureAndDeadline(t *testing.T) {
	_, addr := launch(t, ember.Options{}, server.Options{ReadTimeout: 150 * time.Millisecond})
	bad := connect(t, addr)
	if _, err := io.WriteString(bad.Conn, "*1\r\n$4\r\nPINGxx"); err != nil {
		t.Fatal(err)
	}
	bad.check(t, replyError("ERR Protocol error"))
	if _, err := bad.r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("malformed socket: %v", err)
	}
	idle := connect(t, addr)
	if _, err := idle.r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("idle socket: %v", err)
	}
}

type syncGate struct {
	vfs.FS
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *syncGate) OpenFile(path string, flag int, mode fs.FileMode) (vfs.File, error) {
	file, err := g.FS.OpenFile(path, flag, mode)
	if err != nil || !strings.HasSuffix(path, ".wal") {
		return file, err
	}
	return gatedFile{file, g}, nil
}

type gatedFile struct {
	vfs.File
	gate *syncGate
}

func (f gatedFile) Sync() error {
	if f.gate.armed.CompareAndSwap(true, false) {
		close(f.gate.entered)
		<-f.gate.release
	}
	return f.File.Sync()
}

func TestShutdownDrainsAdmittedWrite(t *testing.T) {
	g := &syncGate{FS: vfs.OS{}, entered: make(chan struct{}), release: make(chan struct{})}
	s, addr := launch(t, ember.Options{FS: g}, server.Options{})
	released := false
	defer func() {
		if !released {
			close(g.release)
		}
	}()
	active, idle := connect(t, addr), connect(t, addr)
	idle.send(t, []string{"PING"})
	idle.check(t, "PONG")
	g.armed.Store(true)
	active.send(t, []string{"SET", "committed", "yes"})
	select {
	case <-g.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not reach sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Shutdown(ctx) }()
	if _, err := idle.r.ReadByte(); !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("idle shutdown: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before sync: %v", err)
	default:
	}
	close(g.release)
	released = true
	active.check(t, "OK")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
