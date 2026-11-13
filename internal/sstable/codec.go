package sstable

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
)

// Codec compresses a block. IDs 0 and 1 are reserved for raw and Deflate.
// Decode must enforce maxBytes before allocating or returning output.
// A codec shared across tables must allow concurrent calls.
type Codec interface {
	ID() byte
	Encode(src []byte) ([]byte, error)
	Decode(src []byte, maxBytes int) ([]byte, error)
}

// Deflate is the optional standard-library block codec.
type Deflate struct{}

// ID returns the persistent ID of Deflate.
func (Deflate) ID() byte { return 1 }

// Encode compresses a block using raw DEFLATE.
func (Deflate) Encode(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	_, writeErr := w.Write(src)
	if err := errors.Join(writeErr, w.Close()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decompresses a block and rejects oversized output or trailing bytes.
func (Deflate) Decode(src []byte, maxBytes int) ([]byte, error) {
	if maxBytes < 0 || maxBytes > maxBlockSize {
		return nil, errors.New("invalid DEFLATE decode limit")
	}
	input := bytes.NewReader(src)
	r := flate.NewReader(input)
	out, err := io.ReadAll(io.LimitReader(r, int64(maxBytes)+1))
	if err := errors.Join(err, r.Close()); err != nil {
		return nil, err
	}
	if len(out) > maxBytes || input.Len() != 0 {
		return nil, errors.New("invalid DEFLATE block size or trailing data")
	}
	return out, nil
}
