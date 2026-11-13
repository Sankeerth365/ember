package compaction

import (
	"bytes"

	"ember/internal/skiplist"
)

// Pruner keeps all versions above the oldest snapshot sequence and at
// most one version at or below it for each key.
type Pruner struct {
	plan        Plan
	merge       *Merge
	sequence    uint64
	at          int64
	key         []byte
	seen, floor bool
	entry       skiplist.Entry
}

// NewPruner wraps a merge with sequence, expiry, and tombstone retention.
func NewPruner(plan Plan, merge *Merge, sequence uint64, readTime int64) *Pruner {
	return &Pruner{plan: plan, merge: merge, sequence: sequence, at: readTime}
}

// Next advances to the next retained internal entry.
func (p *Pruner) Next() bool {
	for p.merge.Next() {
		e := p.merge.Entry()
		if !p.seen || !bytes.Equal(p.key, e.User) {
			p.key = append(p.key[:0], e.User...)
			p.floor = false
			p.seen = true
		}
		if e.ExpiresAt != 0 && e.ExpiresAt <= p.at {
			e.Kind = skiplist.Delete
			e.Value = nil
			e.ExpiresAt = 0
		}
		if e.Seq <= p.sequence {
			if p.floor {
				continue
			}
			p.floor = true
			if e.Kind == skiplist.Delete && !p.plan.MayHaveOlder(e.User, e.Seq) {
				continue
			}
		}
		p.entry = e
		return true
	}
	return false
}

// Entry returns the current retained entry until the next Next call.
func (p *Pruner) Entry() skiplist.Entry { return p.entry }

// Err reports the first underlying merge error.
func (p *Pruner) Err() error { return p.merge.Err() }
