package bloom_test

import (
	"encoding/binary"
	"testing"

	"ember/internal/bloom"
)

func key(i uint64) []byte { return binary.LittleEndian.AppendUint64(nil, i) }

func TestBloomRoundTrip(t *testing.T) {
	const count = 20000
	filter, err := bloom.New(count, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range uint64(count) {
		filter.Add(key(i))
	}
	filter, err = bloom.Decode(filter.Encode())
	if err != nil {
		t.Fatal(err)
	}
	positives := 0
	for i := range uint64(count) {
		if !filter.MayContain(key(i)) {
			t.Fatalf("false negative on %d", i)
		}
		if filter.MayContain(key(i + count)) {
			positives++
		}
	}
	t.Logf("Bloom false positives: %d/%d (%.2f%%)", positives, count, 100*float64(positives)/count)
	if positives > count/50 {
		t.Fatal("false-positive rate exceeds 2%")
	}
}
