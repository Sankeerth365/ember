package server

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"ember/ember"
	"ember/internal/resp"
)

type getter func([]byte) (ember.Record, error)

func readCommand(name string, args [][]byte, get getter, now time.Time) (resp.Value, error) {
	switch name {
	case "GET":
		return getBulk(get, args[1])
	case "MGET":
		items := make([]resp.Value, 0, len(args)-1)
		size := 24
		for _, key := range args[1:] {
			reply, err := getBulk(get, key)
			if err != nil {
				return resp.Value{}, err
			}
			size += reply.Size()
			if size > maxReplyBytes {
				return limitReply, nil
			}
			items = append(items, reply)
		}
		return resp.Array(items...), nil
	case "EXISTS":
		var count int64
		for _, key := range args[1:] {
			_, err := get(key)
			if err == nil {
				count++
			} else if !errors.Is(err, ember.ErrNotFound) {
				return resp.Value{}, err
			}
		}
		return resp.Integer(count), nil
	case "TTL":
		record, err := get(args[1])
		if errors.Is(err, ember.ErrNotFound) {
			return resp.Integer(-2), nil
		}
		if err != nil {
			return resp.Value{}, err
		}
		if record.ExpiresAt.IsZero() {
			return resp.Integer(-1), nil
		}
		remaining := record.ExpiresAt.Sub(now)
		seconds := remaining / time.Second
		if remaining%time.Second >= time.Second/2 {
			seconds++
		}
		return resp.Integer(int64(seconds)), nil
	default:
		return syntaxReply, nil
	}
}

func getBulk(get getter, key []byte) (resp.Value, error) {
	r, err := get(key)
	if errors.Is(err, ember.ErrNotFound) {
		return resp.Null(), nil
	}
	if err != nil {
		return resp.Value{}, err
	}
	return resp.Bulk(r.Value), nil
}

func mutate(name string, args [][]byte, tx *ember.Tx) (resp.Value, error) {
	switch name {
	case "SET":
		return set(args, tx)
	case "MSET":
		for i := 1; i < len(args); i += 2 {
			tx.Put(args[i], args[i+1], time.Time{})
		}
		return okReply, nil
	case "SETNX":
		_, found, err := lookup(tx, args[1])
		if err != nil {
			return resp.Value{}, err
		}
		if found {
			return resp.Integer(0), nil
		}
		tx.Put(args[1], args[2], time.Time{})
		return resp.Integer(1), nil
	case "DEL":
		var count int64
		for _, key := range args[1:] {
			_, found, err := lookup(tx, key)
			if err != nil {
				return resp.Value{}, err
			}
			if found {
				tx.Delete(key)
				count++
			}
		}
		return resp.Integer(count), nil
	case "INCR", "DECR", "INCRBY":
		return increment(name, args, tx)
	case "EXPIRE":
		return expire(args, tx)
	default:
		return syntaxReply, nil
	}
}

func lookup(tx *ember.Tx, key []byte) (ember.Record, bool, error) {
	r, err := tx.Get(key)
	if errors.Is(err, ember.ErrNotFound) {
		return ember.Record{}, false, nil
	}
	return r, err == nil, err
}

func redisInteger(p []byte) (int64, error) {
	n, err := strconv.ParseInt(string(p), 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != string(p) {
		return 0, errors.New("invalid Redis integer")
	}
	return n, nil
}

func increment(name string, args [][]byte, tx *ember.Tx) (resp.Value, error) {
	delta := int64(1)
	switch name {
	case "DECR":
		delta = -1
	case "INCRBY":
		var err error
		delta, err = redisInteger(args[2])
		if err != nil {
			return integerReply, nil
		}
	}
	r, found, err := lookup(tx, args[1])
	if err != nil {
		return resp.Value{}, err
	}
	var n int64
	if found {
		n, err = redisInteger(r.Value)
		if err != nil {
			return integerReply, nil
		}
	}
	if delta > 0 && n > math.MaxInt64-delta || delta < 0 && n < math.MinInt64-delta {
		return resp.Error("ERR increment or decrement would overflow"), nil
	}
	n += delta
	tx.Put(args[1], strconv.AppendInt(nil, n, 10), r.ExpiresAt)
	return resp.Integer(n), nil
}

func expire(args [][]byte, tx *ember.Tx) (resp.Value, error) {
	n, err := redisInteger(args[2])
	if err != nil {
		return integerReply, nil
	}
	var expiry time.Time
	if n > 0 {
		var valid bool
		expiry, valid = expiryAt(tx.Time(), n, time.Second)
		if !valid {
			return resp.Error("ERR invalid expire time"), nil
		}
	}
	r, found, err := lookup(tx, args[1])
	if err != nil {
		return resp.Value{}, err
	}
	if !found {
		return resp.Integer(0), nil
	}
	if n <= 0 {
		tx.Delete(args[1])
	} else {
		tx.Put(args[1], r.Value, expiry)
	}
	return resp.Integer(1), nil
}

func expiryAt(now time.Time, n int64, unit time.Duration) (time.Time, bool) {
	if n <= 0 || n > math.MaxInt64/int64(unit) {
		return time.Time{}, false
	}
	expiry := now.Add(time.Duration(n) * unit)
	if !time.Unix(0, expiry.UnixNano()).Equal(expiry) {
		return time.Time{}, false
	}
	return expiry, true
}

func set(args [][]byte, tx *ember.Tx) (resp.Value, error) {
	var condition, expiration string
	var expiry time.Time
	for i := 3; i < len(args); i++ {
		switch flag := strings.ToUpper(string(args[i])); flag {
		case "NX", "XX":
			if condition != "" {
				return syntaxReply, nil
			}
			condition = flag
		case "EX", "PX", "KEEPTTL":
			if expiration != "" {
				return syntaxReply, nil
			}
			expiration = flag
			if flag == "KEEPTTL" {
				continue
			}
			i++
			if i >= len(args) {
				return syntaxReply, nil
			}
			n, err := redisInteger(args[i])
			if err != nil {
				return integerReply, nil
			}
			unit := time.Second
			if flag == "PX" {
				unit = time.Millisecond
			}
			var valid bool
			expiry, valid = expiryAt(tx.Time(), n, unit)
			if !valid {
				return resp.Error("ERR invalid expire time"), nil
			}
		default:
			return syntaxReply, nil
		}
	}
	if condition != "" || expiration == "KEEPTTL" {
		r, found, err := lookup(tx, args[1])
		if err != nil {
			return resp.Value{}, err
		}
		if condition == "NX" && found || condition == "XX" && !found {
			return resp.Null(), nil
		}
		if expiration == "KEEPTTL" {
			expiry = r.ExpiresAt
		}
	}
	tx.Put(args[1], args[2], expiry)
	return okReply, nil
}
