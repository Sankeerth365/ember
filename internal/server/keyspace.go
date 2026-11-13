package server

import (
	"bytes"
	"strconv"

	"ember/ember"
	"ember/internal/resp"
)

const (
	maxCursors     = 64
	maxCursorBytes = 4 << 20
	maxKeysReply   = 100000
	maxScanCount   = 10000
)

type matcher struct {
	key    []byte
	prefix bool
}

func parseMatcher(p []byte) (matcher, bool) {
	key, prefix := bytes.CutSuffix(p, []byte("*"))
	return matcher{key, prefix}, !bytes.ContainsAny(key, "*?[]\\")
}

func (m matcher) matches(key []byte) bool {
	if m.prefix {
		return bytes.HasPrefix(key, m.key)
	}
	return bytes.Equal(key, m.key)
}

func (m matcher) bounds() ember.IteratorOptions {
	upper := after(m.key)
	if m.prefix {
		upper = bytes.Clone(m.key)
		for len(upper) > 0 && upper[len(upper)-1] == 255 {
			upper = upper[:len(upper)-1]
		}
		if len(upper) == 0 {
			upper = nil
		} else {
			upper[len(upper)-1]++
		}
	}
	return ember.IteratorOptions{Lower: m.key, Upper: upper}
}

func after(key []byte) []byte { return append(bytes.Clone(key), 0) }

func (c *client) keyspace(name string, args [][]byte) resp.Value {
	var opts ember.IteratorOptions
	if name == "KEYS" {
		m, ok := parseMatcher(args[1])
		if !ok {
			return resp.Error("ERR KEYS supports literal keys and prefix patterns")
		}
		opts = m.bounds()
	}
	it, err := c.server.db.NewIterator(opts)
	if err != nil {
		return c.storageError(err)
	}
	defer it.Close()
	var items []resp.Value
	size := 24
	var count int64
	for it.Next() {
		count++
		if name == "KEYS" {
			if count > maxKeysReply || len(it.Key())+24 > maxReplyBytes-size {
				return limitReply
			}
			reply := resp.Bulk(bytes.Clone(it.Key()))
			size += reply.Size()
			items = append(items, reply)
		}
	}
	if err := it.Err(); err != nil {
		return c.storageError(err)
	}
	if name == "DBSIZE" {
		return resp.Integer(count)
	}
	return resp.Array(items...)
}

func (c *client) scan(args [][]byte) resp.Value {
	token, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR invalid cursor")
	}
	count := 10
	m := matcher{prefix: true}
	var hadCount, hadMatch bool
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return syntaxReply
		}
		switch {
		case bytes.EqualFold(args[i], []byte("COUNT")) && !hadCount:
			n, err := redisInteger(args[i+1])
			if err != nil || n < 1 {
				return syntaxReply
			}
			count = int(min(n, maxScanCount))
			hadCount = true
		case bytes.EqualFold(args[i], []byte("MATCH")) && !hadMatch:
			var ok bool
			m, ok = parseMatcher(args[i+1])
			if !ok {
				return resp.Error("ERR SCAN supports literal keys and prefix patterns")
			}
			hadMatch = true
		default:
			return syntaxReply
		}
	}
	var opts ember.IteratorOptions
	if token != 0 {
		key, ok := c.cursors[token]
		if !ok {
			return resp.Error("ERR unknown or expired cursor")
		}
		opts.Lower = after(key)
	}
	it, err := c.server.db.NewIterator(opts)
	if err != nil {
		return c.storageError(err)
	}
	defer it.Close()
	items := make([]resp.Value, 0, count)
	var last []byte
	size := 80
	more := false
	for i := 0; it.Next(); i++ {
		if i == count {
			more = true
			break
		}
		key := it.Key()
		if m.matches(key) {
			if len(key)+24 > maxReplyBytes-size {
				more = true
				break
			}
			reply := resp.Bulk(bytes.Clone(key))
			size += reply.Size()
			items = append(items, reply)
		}
		last = append(last[:0], key...)
	}
	if err := it.Err(); err != nil {
		return c.storageError(err)
	}
	if more && len(last) > maxCursorBytes {
		return limitReply
	}
	delete(c.cursors, token)
	token = 0
	if more {
		token = c.saveCursor(last)
	}
	return resp.Array(resp.Bulk(strconv.AppendUint(nil, token, 10)), resp.Array(items...))
}

func (c *client) saveCursor(key []byte) uint64 {
	if c.cursors == nil {
		c.cursors = make(map[uint64][]byte)
	}
	used := len(key)
	for _, previous := range c.cursors {
		used += len(previous)
	}
	for len(c.cursors) >= maxCursors || used > maxCursorBytes {
		oldest := uint64(0)
		for token := range c.cursors {
			if oldest == 0 || token < oldest {
				oldest = token
			}
		}
		used -= len(c.cursors[oldest])
		delete(c.cursors, oldest)
	}
	c.cursorID++
	if c.cursorID == 0 {
		clear(c.cursors)
		c.cursorID = 1
	}
	c.cursors[c.cursorID] = key
	return c.cursorID
}
