package version

import (
	"errors"
	"fmt"
	"sync"

	"ember/internal/manifest"
)

// ErrInvalidEdit identifies a change that violates version invariants.
var ErrInvalidEdit = errors.New("invalid version edit")

// Set tracks the current version and the table files referenced by pinned
// historical versions. Construct it with NewSet.
type Set struct {
	editMu   sync.Mutex
	mu       sync.Mutex
	current  *Version
	refs     map[*Version]int
	fileRefs map[uint64]int
	used     map[uint64]struct{}
	obsolete []uint64
}

// NewSet returns an empty set awaiting its initial MANIFEST edit.
func NewSet() *Set {
	empty := &Version{}
	return &Set{
		current:  empty,
		refs:     map[*Version]int{empty: 1},
		fileRefs: make(map[uint64]int),
		used:     make(map[uint64]struct{}),
	}
}

// Pin holds a version alive. Do not copy a Pin. Release it when finished.
type Pin struct {
	set      *Set
	version  *Version
	released bool
}

// Acquire pins the current version.
func (s *Set) Acquire() *Pin {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs[s.current]++
	return &Pin{set: s, version: s.current}
}

// Version returns the pinned version. The pin must remain held while the
// caller reads or opens any of its table files.
func (p *Pin) Version() *Version { return p.version }

// Clone pins the same version independently, or returns nil after release.
func (p *Pin) Clone() *Pin {
	p.set.mu.Lock()
	defer p.set.mu.Unlock()
	if p.released {
		return nil
	}
	p.set.refs[p.version]++
	return &Pin{set: p.set, version: p.version}
}

// Release drops the pin once. It may make old files eligible for deletion.
func (p *Pin) Release() {
	p.set.mu.Lock()
	defer p.set.mu.Unlock()
	if !p.released {
		p.released = true
		p.set.releaseLocked(p.version)
	}
}

// Replay applies one already-durable edit during recovery.
func (s *Set) Replay(edit manifest.Edit) error { return s.apply(edit, nil) }

// Commit validates an edit, persists it, then publishes the new version.
// persist must sync the MANIFEST before returning success. It may read the
// current version, but must not call Commit or Replay on the same Set.
func (s *Set) Commit(edit manifest.Edit, persist func(manifest.Edit) error) error {
	if persist == nil {
		return errors.New("missing version persistence callback")
	}
	return s.apply(edit, persist)
}

func (s *Set) apply(edit manifest.Edit, persist func(manifest.Edit) error) error {
	s.editMu.Lock()
	defer s.editMu.Unlock()
	// editMu serializes every modification to current and used. Readers
	// continue pinning the old version while the MANIFEST is synced.
	old := s.current
	next, numbers, err := build(old, edit, s.used)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEdit, err)
	}
	if persist != nil {
		if err := persist(edit); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for number := range numbers {
		s.used[number] = struct{}{}
	}
	s.refs[next] = 1
	for _, files := range next.levels {
		for _, f := range files {
			s.fileRefs[f.Number]++
		}
	}
	s.current = next
	// Register the replacement before releasing the old implicit pin.
	// Shared files must never become eligible for deletion in between.
	s.releaseLocked(old)
	return nil
}

func (s *Set) releaseLocked(v *Version) {
	s.refs[v]--
	if s.refs[v] != 0 {
		return
	}
	delete(s.refs, v)
	for _, files := range v.levels {
		for _, f := range files {
			s.fileRefs[f.Number]--
			if s.fileRefs[f.Number] == 0 {
				delete(s.fileRefs, f.Number)
				s.obsolete = append(s.obsolete, f.Number)
			}
		}
	}
}

// TakeObsolete transfers the list of SSTables no version can reference.
// The caller owns removing them and retrying any failed unlink.
func (s *Set) TakeObsolete() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	numbers := s.obsolete
	s.obsolete = nil
	return numbers
}
