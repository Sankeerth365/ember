// Package version tracks immutable views of the live SSTables and replay WALs.
package version

import (
	"bytes"
	"slices"
	"sort"

	"ember/internal/manifest"
)

// MaxLevels bounds valid level indexes in a MANIFEST.
const MaxLevels = 32

// Version is an immutable view. Hold a Pin while using its table files.
type Version struct {
	levels                            [MaxLevels][]manifest.FileMeta
	levelCount                        int
	wals                              []uint64
	activeWAL, nextFile, lastSequence uint64
}

// LevelCount is one past the highest populated table level, or zero.
func (v *Version) LevelCount() int { return v.levelCount }

// Files returns an independent copy of a level's file metadata.
func (v *Version) Files(level int) []manifest.FileMeta {
	if level < 0 || level >= v.levelCount {
		return nil
	}
	files := make([]manifest.FileMeta, len(v.levels[level]))
	for i, f := range v.levels[level] {
		files[i] = cloneFile(f)
	}
	return files
}

func cloneFile(f manifest.FileMeta) manifest.FileMeta {
	f.MinKey = bytes.Clone(f.MinKey)
	f.MaxKey = bytes.Clone(f.MaxKey)
	return f
}

// VisitCandidates calls visit in read order for files whose ranges contain
// user. Returning false from visit stops the traversal.
func (v *Version) VisitCandidates(user []byte, visit func(number uint64) bool) {
	for _, f := range v.levels[0] {
		if bytes.Compare(user, f.MinKey) >= 0 && bytes.Compare(user, f.MaxKey) <= 0 && !visit(f.Number) {
			return
		}
	}
	for level := 1; level < v.levelCount; level++ {
		files := v.levels[level]
		i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].MaxKey, user) >= 0 })
		if i < len(files) && bytes.Compare(user, files[i].MinKey) >= 0 && !visit(files[i].Number) {
			return
		}
	}
}

// ReplayWALs returns the referenced WALs in creation order.
func (v *Version) ReplayWALs() []uint64 { return slices.Clone(v.wals) }

// ActiveWAL returns the current writable WAL's number.
func (v *Version) ActiveWAL() uint64 { return v.activeWAL }

// NextFileNumber returns the persisted allocation high-water mark.
func (v *Version) NextFileNumber() uint64 { return v.nextFile }

// LastSequence returns the MANIFEST sequence high-water mark. WAL replay
// may find additional sequences above it.
func (v *Version) LastSequence() uint64 { return v.lastSequence }

// FileCount returns the number of tables in level.
func (v *Version) FileCount(level int) int {
	if level < 0 || level >= v.levelCount {
		return 0
	}
	return len(v.levels[level])
}

// LevelUsage returns a level's file count and combined physical size.
func (v *Version) LevelUsage(level int) (count int, size uint64) {
	if level < 0 || level >= v.levelCount {
		return 0, 0
	}
	for _, f := range v.levels[level] {
		size += f.Size
	}
	return v.FileCount(level), size
}
