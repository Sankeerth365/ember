// Package server exposes a single Ember database over RESP2.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"ember/ember"
	"ember/internal/resp"
)

// Options controls connection and idle limits. Zero selects defaults.
type Options struct {
	// MaxConnections is the maximum number of active clients.
	MaxConnections int
	// ReadTimeout limits the time to send each complete command.
	ReadTimeout time.Duration
	// WriteTimeout limits the time to receive a reply.
	WriteTimeout time.Duration
	// Logger receives server lifecycle and storage diagnostics.
	Logger *slog.Logger
}

// Server accepts RESP2 requests and delegates storage to an Ember DB.
type Server struct {
	db       *ember.DB
	opts     Options
	mu       sync.Mutex
	listener net.Listener
	clients  map[net.Conn]struct{}
	stopping bool
	wg       sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}
}

// New constructs a server. The caller owns the database.
func New(db *ember.DB, opts Options) (*Server, error) {
	if opts.MaxConnections == 0 {
		opts.MaxConnections = 128
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 30 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if db == nil || opts.MaxConnections < 1 || opts.ReadTimeout < 0 || opts.WriteTimeout < 0 {
		return nil, errors.New("invalid Ember server options")
	}
	return &Server{db: db, opts: opts, clients: make(map[net.Conn]struct{}), done: make(chan struct{})}, nil
}

// Serve accepts connections until Shutdown or a listener error.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("missing server listener")
	}
	s.mu.Lock()
	if s.stopping || s.listener != nil {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.listener = listener
	s.mu.Unlock()
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if stopping {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.stopping || len(s.clients) >= s.opts.MaxConnections {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.clients[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveClient(conn)
	}
}

// Shutdown stops accepting and reading commands and waits for admitted
// commands to finish. A context timeout closes remaining network sockets.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		if s.listener != nil {
			_ = s.listener.Close()
		}
		for conn := range s.clients {
			_ = conn.SetReadDeadline(time.Now())
		}
		s.mu.Unlock()
		go func() { s.wg.Wait(); close(s.done) }()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for conn := range s.clients {
			_ = conn.Close()
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *Server) serveClient(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	reader, writer := resp.NewDecoder(conn), resp.NewEncoder(conn)
	c := client{server: s}
	for {
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		err := conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
		s.mu.Unlock()
		if err != nil {
			return
		}
		args, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) || errors.Is(err, io.ErrUnexpectedEOF) {
				_ = conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
				_ = writer.Write(resp.Error("ERR Protocol error"))
				_ = writer.Flush()
			}
			return
		}
		s.mu.Lock()
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			return
		}
		reply, quit := c.command(args)
		if reply.Size() > maxReplyBytes {
			reply = limitReply
		}
		if conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout)) != nil ||
			writer.Write(reply) != nil || writer.Flush() != nil || quit {
			return
		}
	}
}
