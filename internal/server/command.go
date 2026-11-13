package server

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"ember/ember"
	"ember/internal/resp"
)

const maxReplyBytes = 64 << 20

var okReply = resp.Simple("OK")
var syntaxReply = resp.Error("ERR syntax error")
var limitReply = resp.Error("ERR reply exceeds Ember size limit")
var integerReply = resp.Error("ERR value is not an integer or out of range")

type commandKind uint8

const (
	control commandKind = iota
	read
	write
	local
)

type commandSpec struct {
	min, max int
	kind     commandKind
}

var commands = map[string]commandSpec{
	"PING": {1, 2, local}, "ECHO": {2, 2, local},
	"SET": {3, 8, write}, "GET": {2, 2, read}, "DEL": {2, 0, write}, "EXISTS": {2, 0, read},
	"MSET": {3, 0, write}, "MGET": {2, 0, read}, "SETNX": {3, 3, write},
	"INCR": {2, 2, write}, "DECR": {2, 2, write}, "INCRBY": {3, 3, write},
	"EXPIRE": {3, 3, write}, "TTL": {2, 2, read},
	"KEYS": {2, 2, control}, "SCAN": {2, 6, control}, "DBSIZE": {1, 1, control}, "INFO": {1, 2, control},
	"FLUSHALL": {1, 2, control}, "COMMAND": {1, 2, control}, "QUIT": {1, 1, control},
	"MULTI": {1, 1, control}, "EXEC": {1, 1, control}, "DISCARD": {1, 1, control},
	"CONFIG": {3, 0, control}, "CLIENT": {2, 4, control}, "SELECT": {2, 2, control},
}

type queued struct {
	name string
	args [][]byte
}
type client struct {
	server         *Server
	inMulti, dirty bool
	pending        []queued
	pendingBytes   int
	cursorID       uint64
	cursors        map[uint64][]byte
}

func (c *client) command(args [][]byte) (resp.Value, bool) {
	name := ""
	if len(args[0]) <= 32 {
		name = strings.ToUpper(string(args[0]))
	}
	spec, known := commands[name]
	if !known {
		c.dirty = c.dirty || c.inMulti
		return resp.Error("ERR unknown command"), false
	}
	if len(args) < spec.min || spec.max != 0 && len(args) > spec.max || name == "MSET" && len(args)%2 != 1 {
		c.dirty = c.dirty || c.inMulti
		return resp.Error("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"), false
	}
	if name == "QUIT" {
		return okReply, true
	}
	if name == "MULTI" || name == "EXEC" || name == "DISCARD" {
		return c.transaction(name), false
	}
	if c.inMulti {
		return c.enqueue(name, spec, args), false
	}
	switch spec.kind {
	case local:
		return localReply(name, args), false
	case read:
		s, err := c.server.db.NewSnapshot()
		if err != nil {
			return c.storageError(err), false
		}
		defer s.Close()
		reply, err := readCommand(name, args, s.GetRecord, s.Time())
		if err != nil {
			return c.storageError(err), false
		}
		return reply, false
	case write:
		return c.writeCommand(name, args), false
	default:
		return c.controlCommand(name, args), false
	}
}

func localReply(name string, args [][]byte) resp.Value {
	if name == "PING" && len(args) == 1 {
		return resp.Simple("PONG")
	}
	return resp.Bulk(args[1])
}

func (c *client) storageError(err error) resp.Value {
	c.server.opts.Logger.Error("Ember command failed", "error", err)
	return resp.Error("ERR storage operation failed")
}

func (c *client) writeCommand(name string, args [][]byte) resp.Value {
	if name == "MSET" || name == "SET" && len(args) == 3 {
		var batch ember.Batch
		for i := 1; i < len(args); i += 2 {
			batch.Put(args[i], args[i+1])
		}
		if err := c.server.db.Write(&batch); err != nil {
			return c.storageError(err)
		}
		return okReply
	}
	var reply resp.Value
	err := c.server.db.Update(func(tx *ember.Tx) error {
		var err error
		reply, err = mutate(name, args, tx)
		return err
	})
	if err != nil {
		return c.storageError(err)
	}
	return reply
}

func (c *client) controlCommand(name string, args [][]byte) resp.Value {
	switch name {
	case "INFO":
		return resp.Bulk(info(c.server.db.Stats()))
	case "SCAN":
		return c.scan(args)
	case "KEYS", "DBSIZE":
		return c.keyspace(name, args)
	case "FLUSHALL":
		if len(args) == 2 && !bytes.EqualFold(args[1], []byte("SYNC")) {
			return syntaxReply
		}
		if err := c.server.db.Reset(); err != nil {
			return c.storageError(err)
		}
		clear(c.cursors)
		return okReply
	case "SELECT":
		if string(args[1]) != "0" {
			return resp.Error("ERR only database 0 is available")
		}
		return okReply
	case "COMMAND":
		return commandMetadata(args)
	case "CONFIG":
		return config(args)
	case "CLIENT":
		if len(args) == 4 && bytes.EqualFold(args[1], []byte("SETINFO")) {
			return okReply
		}
		return syntaxReply
	default:
		return syntaxReply
	}
}

func config(args [][]byte) resp.Value {
	if !bytes.EqualFold(args[1], []byte("GET")) {
		return syntaxReply
	}
	var items []resp.Value
	for _, field := range []struct{ key, value string }{{"save", ""}, {"appendonly", "no"}} {
		for _, pattern := range args[2:] {
			if bytes.EqualFold(pattern, []byte(field.key)) || string(pattern) == "*" {
				items = append(items, resp.Bulk([]byte(field.key)), resp.Bulk([]byte(field.value)))
				break
			}
		}
	}
	return resp.Array(items...)
}

func info(s ember.Stats) []byte {
	var b bytes.Buffer
	fmt.Fprint(&b, "# Server\r\nember_protocol:RESP2\r\n# Ember\r\n")
	fmt.Fprintf(&b, "sequence:%d\r\nimmutable_memtables:%d\r\n", s.Sequence, s.ImmutableMemtables)
	fmt.Fprintf(&b, "cache_hit_rate:%.6f\r\ncache_hits:%d\r\ncache_misses:%d\r\ncache_bytes:%d\r\n",
		s.CacheHitRate, s.CacheHits, s.CacheMisses, s.CacheBytes)
	fmt.Fprintf(&b, "bloom_skips:%d\r\nwrite_stalls:%d\r\nwrite_slowdowns:%d\r\ncompactions:%d\r\nflushes:%d\r\n",
		s.BloomSkips, s.WriteStalls, s.WriteSlowdowns, s.Compactions, s.Flushes)
	fmt.Fprintf(&b, "write_groups:%d\r\nwrite_batches:%d\r\n", s.WriteGroups, s.WriteBatches)
	for level, x := range s.Levels {
		fmt.Fprintf(&b, "level%d:files=%d,bytes=%d\r\n", level, x.Files, x.Bytes)
	}
	return b.Bytes()
}

func commandMetadata(args [][]byte) resp.Value {
	if len(args) == 2 {
		if bytes.EqualFold(args[1], []byte("COUNT")) {
			return resp.Integer(int64(len(commands)))
		}
		return syntaxReply
	}
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := make([]resp.Value, 0, len(names))
	for _, name := range names {
		spec := commands[name]
		arity := int64(spec.min)
		if spec.max != spec.min {
			arity = -arity
		}
		var first, last, step int64
		var flags []resp.Value
		switch spec.kind {
		case read, write:
			first, last, step = 1, 1, 1
			if spec.kind == read {
				flags = append(flags, resp.Simple("readonly"))
			} else {
				flags = append(flags, resp.Simple("write"))
			}
		}
		switch name {
		case "MGET", "EXISTS", "DEL":
			last = -1
		case "MSET":
			last, step = -1, 2
		case "KEYS", "SCAN", "DBSIZE":
			flags = append(flags, resp.Simple("readonly"))
		case "FLUSHALL":
			flags = append(flags, resp.Simple("write"))
		}
		entries = append(entries, resp.Array(resp.Bulk([]byte(strings.ToLower(name))), resp.Integer(arity),
			resp.Array(flags...), resp.Integer(first), resp.Integer(last), resp.Integer(step)))
	}
	return resp.Array(entries...)
}
