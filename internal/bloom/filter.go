// Package bloom implements Ember's persisted SSTable Bloom filter.
package bloom

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"math"
)

const (
	hashVersion    = 1
	headerSize     = 10
	maxProbes      = 30
	maxBitsPerKey  = 43
	maxFilterBytes = 64 << 20
)

// DefaultBitsPerKey is the default density for an SSTable's Bloom filter.
const DefaultBitsPerKey = 10

var ecma = crc64.MakeTable(crc64.ECMA)

// Filter holds the bit array and hash probe count. Add during construction
// only. MayContain is safe for concurrent readers after publication.
type Filter struct {
	bits   []byte
	probes uint8
}

// New allocates a filter for an expected number of distinct user keys.
// A bitsPerKey value of zero selects DefaultBitsPerKey.
func New(keys, bitsPerKey int) (*Filter, error) {
	if bitsPerKey == 0 {
		bitsPerKey = DefaultBitsPerKey
	}
	if keys < 0 || bitsPerKey < 1 || bitsPerKey > maxBitsPerKey {
		return nil, errors.New("invalid Bloom filter parameters")
	}
	if keys > maxFilterBytes*8/bitsPerKey {
		return nil, errors.New("configured Bloom filter exceeds size limit")
	}
	bits := max(64, keys*bitsPerKey)
	probes := min(maxProbes, max(1, int(math.Round(float64(bitsPerKey)*math.Ln2))))
	return &Filter{bits: make([]byte, (bits+7)/8), probes: uint8(probes)}, nil
}

// Add inserts a binary user key.
func (f *Filter) Add(key []byte) {
	first, stride, modulus := f.probeStart(key)
	for i := uint8(0); i < f.probes; i++ {
		f.bits[first/8] |= 1 << (first % 8)
		first = (first + stride) % modulus
	}
}

// MayContain reports whether the key could have been added.
func (f *Filter) MayContain(key []byte) bool {
	first, stride, modulus := f.probeStart(key)
	for i := uint8(0); i < f.probes; i++ {
		if f.bits[first/8]&(1<<(first%8)) == 0 {
			return false
		}
		first = (first + stride) % modulus
	}
	return true
}

func (f *Filter) probeStart(key []byte) (first, stride, modulus uint64) {
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)
	hash := uint64(fnvOffset)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= fnvPrime
	}
	modulus = uint64(len(f.bits)) * 8
	return hash % modulus, (crc64.Checksum(key, ecma) | 1) % modulus, modulus
}

// BitCount returns the number of addressable bits in the filter.
func (f *Filter) BitCount() uint64 { return uint64(len(f.bits)) * 8 }

// Probes returns the number of bit positions tested per key.
func (f *Filter) Probes() int { return int(f.probes) }

// Encode returns the versioned Bloom block payload.
func (f *Filter) Encode() []byte {
	out := make([]byte, headerSize+len(f.bits))
	out[0], out[1] = hashVersion, f.probes
	binary.LittleEndian.PutUint64(out[2:headerSize], f.BitCount())
	copy(out[headerSize:], f.bits)
	return out
}

// Decode validates a checksummed Bloom block's payload and owns a copy.
func Decode(payload []byte) (*Filter, error) {
	if len(payload) < headerSize+8 || len(payload)-headerSize > maxFilterBytes {
		return nil, errors.New("invalid Bloom filter size")
	}
	if payload[0] != hashVersion || payload[1] == 0 || payload[1] > maxProbes {
		return nil, errors.New("unsupported Bloom filter version or probe count")
	}
	if binary.LittleEndian.Uint64(payload[2:headerSize]) != uint64(len(payload)-headerSize)*8 {
		return nil, errors.New("encoded Bloom bit count does not match payload")
	}
	bits := make([]byte, len(payload)-headerSize)
	copy(bits, payload[headerSize:])
	return &Filter{bits: bits, probes: payload[1]}, nil
}
