// Package compaction selects and merges runs in a leveled LSM tree.
package compaction

import (
	"bytes"
	"math"

	"ember/internal/manifest"
	"ember/internal/version"
)

// Plan identifies the inputs, output level, and files outside the merge.
type Plan struct {
	// Inputs are the tables replaced by the compaction.
	Inputs []manifest.FileMeta
	// OutputLevel is the destination level.
	OutputLevel int
	outside     []manifest.FileMeta
}

// Pick selects the oldest eligible L0 run or the first file of an overfull
// deeper level. Level 1 gets baseBytes and each following level gets tenfold.
func Pick(v *version.Version, l0Trigger int, baseBytes uint64, levels int) (Plan, bool) {
	l0 := v.Files(0)
	if len(l0) > 0 && len(l0) >= l0Trigger && levels > 1 {
		return expand(v, l0[len(l0)-1], l0), true
	}
	budget := baseBytes
	for level := 1; level+1 < levels && level < v.LevelCount(); level++ {
		files := v.Files(level)
		var total uint64
		for _, f := range files {
			total += min(math.MaxUint64-total, f.Size)
		}
		if len(files) > 0 && total > budget {
			return expand(v, files[0], nil), true
		}
		if budget > math.MaxUint64/10 {
			budget = math.MaxUint64
		} else {
			budget *= 10
		}
	}
	return Plan{}, false
}

func expand(v *version.Version, seed manifest.FileMeta, l0 []manifest.FileMeta) Plan {
	p := Plan{Inputs: []manifest.FileMeta{seed}, OutputLevel: seed.Level + 1}
	in := map[uint64]bool{seed.Number: true}
	low, high := seed.MinKey, seed.MaxKey
	for changed := true; changed; {
		changed = false
		for _, f := range l0 {
			if !in[f.Number] && overlaps(f, low, high) {
				in[f.Number] = true
				p.Inputs = append(p.Inputs, f)
				if bytes.Compare(f.MinKey, low) < 0 {
					low = f.MinKey
				}
				if bytes.Compare(f.MaxKey, high) > 0 {
					high = f.MaxKey
				}
				changed = true
			}
		}
	}
	for _, f := range v.Files(p.OutputLevel) {
		if overlaps(f, low, high) {
			in[f.Number] = true
			p.Inputs = append(p.Inputs, f)
		}
	}
	for level := 0; level < v.LevelCount(); level++ {
		for _, f := range v.Files(level) {
			if !in[f.Number] {
				p.outside = append(p.outside, f)
			}
		}
	}
	return p
}

func overlaps(f manifest.FileMeta, low, high []byte) bool {
	return bytes.Compare(f.MinKey, high) <= 0 && bytes.Compare(f.MaxKey, low) >= 0
}

// MayHaveOlder reports whether a file outside the plan could contain an
// older value for user. It conservatively uses inclusive table bounds.
func (p Plan) MayHaveOlder(user []byte, seq uint64) bool {
	for _, f := range p.outside {
		if f.MinSeq < seq && overlaps(f, user, user) {
			return true
		}
	}
	return false
}
