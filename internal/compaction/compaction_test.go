package compaction_test

import (
	"errors"
	"slices"
	"testing"

	"ember/internal/compaction"
	"ember/internal/manifest"
	"ember/internal/skiplist"
	"ember/internal/version"
)

func file(level int, n, seq uint64, low, high string) manifest.FileMeta {
	return manifest.FileMeta{Level: level, Number: n, Size: 100, MinSeq: seq, MaxSeq: seq, MinKey: []byte(low), MaxKey: []byte(high)}
}

func pickVersion(t *testing.T, files ...manifest.FileMeta) *version.Pin {
	t.Helper()
	s := version.NewSet()
	if err := s.Replay(manifest.Edit{Added: files, AddWALs: []uint64{1}, ActiveWAL: 1, NextFileNumber: 100, LastSequence: 100}); err != nil {
		t.Fatal(err)
	}
	p := s.Acquire()
	t.Cleanup(p.Release)
	return p
}

func TestPickLevelZeroClosure(t *testing.T) {
	p := pickVersion(t, file(0, 2, 10, "a", "b"), file(0, 3, 11, "b", "d"), file(0, 4, 12, "d", "f"),
		file(0, 5, 13, "z", "z"), file(1, 6, 6, "c", "f"), file(1, 7, 6, "g", "g"), file(2, 8, 3, "a", "a"))
	plan, ok := compaction.Pick(p.Version(), 4, 1024, 5)
	if !ok || plan.OutputLevel != 1 {
		t.Fatalf("plan=%+v ok=%t", plan, ok)
	}
	var numbers []uint64
	for _, f := range plan.Inputs {
		numbers = append(numbers, f.Number)
	}
	slices.Sort(numbers)
	if !slices.Equal(numbers, []uint64{2, 3, 4, 6}) {
		t.Fatalf("inputs=%v", numbers)
	}
	if !plan.MayHaveOlder([]byte("a"), 5) || plan.MayHaveOlder([]byte("b"), 5) || plan.MayHaveOlder([]byte("a"), 3) {
		t.Fatal("older range detection differs from sequence and inclusive bounds")
	}
}

func TestPickLevelBudget(t *testing.T) {
	p := pickVersion(t, file(1, 2, 10, "a", "b"), file(1, 3, 11, "e", "f"), file(2, 4, 6, "b", "b"))
	for _, tc := range []struct {
		name   string
		budget uint64
		levels int
		want   bool
	}{
		{"at_budget", 200, 4, false}, {"above_budget", 199, 4, true}, {"bottom_level", 1, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, ok := compaction.Pick(p.Version(), 4, tc.budget, tc.levels)
			if ok != tc.want {
				t.Fatalf("selected=%t", ok)
			}
			if ok && (plan.OutputLevel != 2 || len(plan.Inputs) != 2 || plan.Inputs[0].Number != 2 || plan.Inputs[1].Number != 4) {
				t.Fatalf("plan=%+v", plan)
			}
		})
	}
}

type source struct {
	entries []skiplist.Entry
	at      int
	err     error
}

func (s *source) Next() bool { s.at++; return s.at < len(s.entries) }
func (s *source) Seek(k skiplist.Key) bool {
	s.at = slices.IndexFunc(s.entries, func(e skiplist.Entry) bool { return skiplist.Compare(e.Key, k) >= 0 })
	if s.at < 0 {
		s.at = len(s.entries)
	}
	return s.at < len(s.entries)
}
func (s *source) Entry() skiplist.Entry { return s.entries[s.at] }
func (s *source) Err() error            { return s.err }

func entry(key string, seq uint64, kind skiplist.Kind, expiry int64) skiplist.Entry {
	return skiplist.Entry{Key: skiplist.Key{User: []byte(key), Seq: seq, Kind: kind}, ExpiresAt: expiry}
}

func TestMergeOrderingSeekAndErrors(t *testing.T) {
	a := &source{at: -1, entries: []skiplist.Entry{entry("a", 5, skiplist.Put, 0), entry("c", 4, skiplist.Put, 0)}}
	b := &source{at: -1, entries: []skiplist.Entry{entry("a", 3, skiplist.Delete, 0), entry("b", 7, skiplist.Put, 0)}}
	m := compaction.NewMerge([]compaction.Source{a, b})
	var seq []uint64
	for m.Next() {
		seq = append(seq, m.Entry().Seq)
	}
	if !slices.Equal(seq, []uint64{5, 3, 7, 4}) || m.Err() != nil {
		t.Fatalf("merge=%v err=%v", seq, m.Err())
	}
	if !m.Seek(skiplist.Key{User: []byte("a"), Seq: 4}) || m.Entry().Seq != 3 {
		t.Fatal("seek lost the second version")
	}
	b.err = errors.New("read failed")
	if !m.Next() || m.Entry().Seq != 7 || m.Next() || !errors.Is(m.Err(), b.err) {
		t.Fatalf("source error: %v", m.Err())
	}
}

func TestPruningRespectsOldFilesAndSnapshots(t *testing.T) {
	pin := pickVersion(t, file(0, 2, 20, "a", "c"), file(1, 3, 7, "b", "b"), file(2, 4, 2, "b", "b"))
	plan, ok := compaction.Pick(pin.Version(), 1, 1024, 4)
	if !ok {
		t.Fatal("missing plan")
	}
	in := &source{at: -1, entries: []skiplist.Entry{
		entry("a", 20, skiplist.Put, 5), entry("a", 11, skiplist.Put, 0), entry("a", 9, skiplist.Delete, 0), entry("a", 8, skiplist.Put, 0),
		entry("b", 8, skiplist.Put, 5), entry("b", 7, skiplist.Put, 0), entry("c", 4, skiplist.Put, 0),
	}}
	p := compaction.NewPruner(plan, compaction.NewMerge([]compaction.Source{in}), 10, 5)
	want := []skiplist.Entry{entry("a", 20, skiplist.Delete, 0), entry("a", 11, skiplist.Put, 0),
		entry("b", 8, skiplist.Delete, 0), entry("c", 4, skiplist.Put, 0)}
	for _, w := range want {
		if !p.Next() {
			t.Fatalf("missing %+v: %v", w, p.Err())
		}
		got := p.Entry()
		if skiplist.Compare(got.Key, w.Key) != 0 || got.ExpiresAt != w.ExpiresAt {
			t.Fatalf("got %+v want %+v", got, w)
		}
	}
	if p.Next() || p.Err() != nil {
		t.Fatal("unexpected remaining entry")
	}
}
