package version

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"ember/internal/manifest"
)

func build(old *Version, e manifest.Edit, used map[uint64]struct{}) (*Version, map[uint64]struct{}, error) {
	next := &Version{
		activeWAL:    old.activeWAL,
		nextFile:     old.nextFile,
		lastSequence: old.lastSequence,
	}
	if e.ActiveWAL != 0 {
		next.activeWAL = e.ActiveWAL
	}
	if e.NextFileNumber != 0 {
		next.nextFile = e.NextFileNumber
	}
	if e.LastSequence != 0 {
		next.lastSequence = e.LastSequence
	}
	if next.nextFile < old.nextFile || next.lastSequence < old.lastSequence {
		return nil, nil, errors.New("MANIFEST high-water mark regressed")
	}

	newNumbers := make(map[uint64]struct{}, len(e.Added)+len(e.AddWALs))
	reserve := func(number uint64) error {
		_, previouslyUsed := used[number]
		_, repeated := newNumbers[number]
		if number == 0 || number >= next.nextFile || previouslyUsed || repeated {
			return fmt.Errorf("invalid or reused file number %d", number)
		}
		newNumbers[number] = struct{}{}
		return nil
	}
	if err := copyTables(next, old, e.Deleted); err != nil {
		return nil, nil, err
	}
	for _, f := range e.Added {
		if f.Level < 0 || f.Level >= MaxLevels || f.Size == 0 ||
			f.MinSeq == 0 || f.MinSeq > f.MaxSeq || f.MaxSeq > next.lastSequence ||
			bytes.Compare(f.MinKey, f.MaxKey) > 0 {
			return nil, nil, fmt.Errorf("invalid table metadata for file %d", f.Number)
		}
		if err := reserve(f.Number); err != nil {
			return nil, nil, err
		}
		next.levels[f.Level] = append(next.levels[f.Level], cloneFile(f))
	}
	if err := orderLevels(next); err != nil {
		return nil, nil, err
	}

	if err := copyWALs(next, old, e.RemoveWALs); err != nil {
		return nil, nil, err
	}
	for _, number := range e.AddWALs {
		if err := reserve(number); err != nil {
			return nil, nil, err
		}
		next.wals = append(next.wals, number)
	}
	slices.Sort(next.wals)
	if len(next.wals) == 0 || next.wals[len(next.wals)-1] != next.activeWAL {
		return nil, nil, errors.New("active WAL must be the newest replay WAL")
	}
	return next, newNumbers, nil
}

func copyTables(next, old *Version, deletions []manifest.FileRef) error {
	deleted := make(map[uint64]int, len(deletions))
	for _, f := range deletions {
		if _, exists := deleted[f.Number]; exists {
			return fmt.Errorf("repeated table deletion %d", f.Number)
		}
		deleted[f.Number] = f.Level
	}
	for level := 0; level < old.levelCount; level++ {
		for _, f := range old.levels[level] {
			if expected, ok := deleted[f.Number]; ok {
				if expected != level {
					return fmt.Errorf("file %d is in level %d, deletion names level %d", f.Number, level, expected)
				}
				delete(deleted, f.Number)
				continue
			}
			next.levels[level] = append(next.levels[level], f)
		}
	}
	if len(deleted) != 0 {
		return errors.New("deletion references a table outside the live version")
	}
	return nil
}

func copyWALs(next, old *Version, removals []uint64) error {
	removed := make(map[uint64]struct{}, len(removals))
	for _, number := range removals {
		if _, exists := removed[number]; exists {
			return fmt.Errorf("repeated WAL removal %d", number)
		}
		removed[number] = struct{}{}
	}
	for _, number := range old.wals {
		if _, ok := removed[number]; ok {
			delete(removed, number)
			continue
		}
		next.wals = append(next.wals, number)
	}
	if len(removed) != 0 {
		return errors.New("removal references an inactive replay WAL")
	}
	return nil
}

func orderLevels(v *Version) error {
	for level, files := range v.levels {
		if len(files) == 0 {
			continue
		}
		v.levelCount = level + 1
		if level == 0 {
			slices.SortFunc(files, func(a, b manifest.FileMeta) int {
				if a.MaxSeq > b.MaxSeq || a.MaxSeq == b.MaxSeq && a.Number > b.Number {
					return -1
				}
				if a.Number == b.Number {
					return 0
				}
				return 1
			})
			continue
		}
		slices.SortFunc(files, func(a, b manifest.FileMeta) int { return bytes.Compare(a.MinKey, b.MinKey) })
		for i := 1; i < len(files); i++ {
			if bytes.Compare(files[i-1].MaxKey, files[i].MinKey) >= 0 {
				return fmt.Errorf("overlapping tables %d and %d in level %d", files[i-1].Number, files[i].Number, level)
			}
		}
	}
	return nil
}
