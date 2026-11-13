package server

import (
	"errors"

	"ember/ember"
	"ember/internal/resp"
)

const maxQueuedCommands = 1024

var errReplyLimit = errors.New("transaction reply exceeds limit")

func (c *client) enqueue(name string, spec commandSpec, args [][]byte) resp.Value {
	if spec.kind == control {
		c.dirty = true
		return resp.Error("ERR command is unavailable inside MULTI")
	}
	size := 24
	for _, arg := range args {
		size += len(arg) + 24
	}
	if len(c.pending) >= maxQueuedCommands || size > resp.MaxRequestBytes-c.pendingBytes {
		c.dirty = true
		return resp.Error("ERR transaction exceeds Ember queue limits")
	}
	c.pending = append(c.pending, queued{name, args})
	c.pendingBytes += size
	return resp.Simple("QUEUED")
}

func (c *client) transaction(name string) resp.Value {
	if name == "MULTI" {
		if c.inMulti {
			return resp.Error("ERR MULTI calls can not be nested")
		}
		c.inMulti = true
		return okReply
	}
	if !c.inMulti {
		return resp.Error("ERR " + name + " without MULTI")
	}
	pending, dirty := c.pending, c.dirty
	c.inMulti, c.dirty = false, false
	c.pending, c.pendingBytes = nil, 0
	if name == "DISCARD" {
		return okReply
	}
	if dirty {
		return resp.Error("EXECABORT Transaction discarded because of previous errors.")
	}
	if len(pending) == 0 {
		return resp.Array()
	}
	replies := make([]resp.Value, 0, len(pending))
	err := c.server.db.Update(func(tx *ember.Tx) error {
		size := 24
		for _, cmd := range pending {
			var reply resp.Value
			var err error
			switch commands[cmd.name].kind {
			case local:
				reply = localReply(cmd.name, cmd.args)
			case read:
				reply, err = readCommand(cmd.name, cmd.args, tx.Get, tx.Time())
			case write:
				reply, err = mutate(cmd.name, cmd.args, tx)
			}
			if err != nil {
				return err
			}
			size += reply.Size()
			if size > maxReplyBytes {
				return errReplyLimit
			}
			replies = append(replies, reply)
		}
		return nil
	})
	if errors.Is(err, errReplyLimit) {
		return limitReply
	}
	if err != nil {
		return c.storageError(err)
	}
	return resp.Array(replies...)
}
