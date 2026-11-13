// Package resp implements the bounded RESP2 subset used by Ember's server.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	// MaxArguments bounds the number of bulk strings in a request.
	MaxArguments = 1024
	// MaxBulkBytes bounds a single network argument.
	MaxBulkBytes = 16 << 20
	// MaxRequestBytes bounds the total argument bytes of one request.
	MaxRequestBytes = 32 << 20
	headerBytes     = 64
)

// ErrProtocol indicates malformed or oversized request framing.
var ErrProtocol = errors.New("RESP protocol error")

// Decoder reads arrays of non-null bulk strings from a connection.
type Decoder struct{ r *bufio.Reader }

// NewDecoder buffers src and returns a command decoder.
func NewDecoder(src io.Reader) *Decoder {
	return &Decoder{bufio.NewReaderSize(src, 32<<10)}
}

// ReadCommand returns the next command, or io.EOF between complete commands.
func (d *Decoder) ReadCommand() ([][]byte, error) {
	start, err := d.r.Peek(1)
	if err != nil {
		return nil, err
	}
	if start[0] != '*' {
		return d.inlinePing()
	}
	n, err := d.number('*', MaxArguments)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, protocol("empty command")
	}
	args := make([][]byte, 0, n)
	var total int
	for range n {
		size, err := d.number('$', MaxBulkBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if size > MaxRequestBytes-total {
			return nil, protocol("request too large")
		}
		total += size
		arg, err := d.bulk(size)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		var terminator [2]byte
		if _, err := io.ReadFull(d.r, terminator[:]); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if terminator != [2]byte{'\r', '\n'} {
			return nil, protocol("invalid bulk terminator")
		}
		args = append(args, arg)
	}
	return args, nil
}

func (d *Decoder) bulk(size int) ([]byte, error) {
	chunkSize := d.r.Size()
	if size <= chunkSize {
		arg := make([]byte, size)
		_, err := io.ReadFull(d.r, arg)
		return arg, err
	}
	// A bulk length is only a declaration. Grow the slice as payload bytes
	// arrive instead of reserving the entire advertised size up front.
	var arg []byte
	for len(arg) < size {
		n := min(chunkSize, size-len(arg))
		chunk, err := d.r.Peek(n)
		if err != nil {
			return nil, err
		}
		arg = append(arg, chunk...)
		_, _ = d.r.Discard(n) // Peek confirmed these bytes are buffered.
	}
	return arg, nil
}

// The Redis benchmark sends a literal PING line for its PING_INLINE case.
func (d *Decoder) inlinePing() ([][]byte, error) {
	line, err := d.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, protocol("inline command too long")
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if len(line) > headerBytes || !bytes.EqualFold(line, []byte("PING\r\n")) {
		return nil, protocol("expected RESP2 command array")
	}
	return [][]byte{[]byte("PING")}, nil
}

func (d *Decoder) number(prefix byte, max int) (int, error) {
	line, err := d.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return 0, protocol("header too long")
	}
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	if len(line) < 4 || len(line) > headerBytes || line[0] != prefix || line[len(line)-2] != '\r' {
		return 0, protocol("invalid length header")
	}
	n, err := strconv.ParseUint(string(line[1:len(line)-2]), 10, 32)
	if err != nil || n > uint64(max) {
		return 0, protocol("invalid length")
	}
	return int(n), nil
}

func protocol(message string) error { return fmt.Errorf("%w: %s", ErrProtocol, message) }
