package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

type kind byte

const (
	simple  kind = '+'
	failure kind = '-'
	integer kind = ':'
	bulk    kind = '$'
	array   kind = '*'
)

// Value is a RESP2 reply. Construct values using the package functions.
type Value struct {
	kind  kind
	text  string
	bytes []byte
	items []Value
	n     int64
}

// Simple returns a single-line simple string.
func Simple(s string) Value { return Value{kind: simple, text: s} }

// Error returns a single-line RESP error, including its error code.
func Error(s string) Value { return Value{kind: failure, text: s} }

// Integer returns a signed integer reply.
func Integer(n int64) Value { return Value{kind: integer, n: n} }

// Bulk returns a bulk string, including a zero-length string for nil bytes.
func Bulk(p []byte) Value { return Value{kind: bulk, bytes: p, n: int64(len(p))} }

// Null returns a RESP2 null bulk string.
func Null() Value { return Value{kind: bulk, n: -1} }

// Array returns an array reply.
func Array(items ...Value) Value { return Value{kind: array, items: items, n: int64(len(items))} }

// Size returns a conservative bound on the encoded reply size.
func (v Value) Size() int {
	size := 24 + len(v.text) + len(v.bytes)
	for _, item := range v.items {
		size += item.Size()
	}
	return size
}

// Encoder buffers and writes RESP2 values.
type Encoder struct{ w *bufio.Writer }

// NewEncoder buffers dst and returns a reply encoder.
func NewEncoder(dst io.Writer) *Encoder { return &Encoder{bufio.NewWriterSize(dst, 32<<10)} }

// Write writes one reply into the buffer.
func (e *Encoder) Write(v Value) error {
	switch v.kind {
	case simple, failure:
		if strings.ContainsAny(v.text, "\r\n") {
			return errors.New("RESP line contains a newline")
		}
		if err := e.w.WriteByte(byte(v.kind)); err != nil {
			return err
		}
		if _, err := e.w.WriteString(v.text); err != nil {
			return err
		}
		_, err := e.w.WriteString("\r\n")
		return err
	case integer, bulk, array:
		if err := e.number(v.kind, v.n); err != nil {
			return err
		}
	default:
		return errors.New("invalid RESP reply type")
	}
	switch v.kind {
	case bulk:
		if v.n >= 0 {
			if _, err := e.w.Write(v.bytes); err != nil {
				return err
			}
			_, err := e.w.WriteString("\r\n")
			return err
		}
	case array:
		for _, item := range v.items {
			if err := e.Write(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Encoder) number(k kind, n int64) error {
	var buf [32]byte
	p := strconv.AppendInt(append(buf[:0], byte(k)), n, 10)
	_, err := e.w.Write(append(p, '\r', '\n'))
	return err
}

// Flush sends any buffered reply bytes.
func (e *Encoder) Flush() error { return e.w.Flush() }
