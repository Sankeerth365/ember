package cache_test

import (
	"bytes"
	"testing"

	"ember/internal/cache"
)

func TestLRU(t *testing.T) {
	c := cache.New(4, 1)
	a, b, d := cache.Key{File: 1}, cache.Key{File: 2}, cache.Key{File: 3}
	buf := []byte("ab")
	c.Set(a, buf)
	c.Set(b, buf)
	buf[0] = 'z'
	if got, ok := c.Get(a); !ok || !bytes.Equal(got, []byte("ab")) {
		t.Fatalf("bad payload %q", got)
	}
	c.Set(d, buf)
	if _, ok := c.Get(b); ok || c.Stats() != (cache.Stats{Hits: 1, Misses: 1, Evictions: 1, Bytes: 4}) {
		t.Fatalf("bad eviction: %+v", c.Stats())
	}
}

func TestDisabledAndOversizeBlocks(t *testing.T) {
	key := cache.Key{File: 1, Offset: 64}
	for _, capacity := range []int64{0, 2} {
		c := cache.New(capacity, 1)
		c.Set(key, []byte("abc"))
		if _, ok := c.Get(key); ok {
			t.Fatal("oversize block cached")
		}
		if c.Stats().Bytes != 0 {
			t.Fatal("oversize block charged")
		}
	}
	c := cache.New(8, 1)
	c.Set(key, []byte("old"))
	hold, ok := c.Get(key)
	if !ok {
		t.Fatal("missing initial block")
	}
	c.Set(key, []byte("new"))
	if current, _ := c.Get(key); string(current) != "new" || string(hold) != "old" || c.Stats().Bytes != 3 {
		t.Fatal("replace mutated an outstanding block or charged twice")
	}
}
