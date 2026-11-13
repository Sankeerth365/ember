package version_test

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"ember/internal/manifest"
	"ember/internal/version"
	"ember/internal/vfs"
)

var initial = manifest.Edit{AddWALs: []uint64{2}, ActiveWAL: 2, NextFileNumber: 3}

func create(t *testing.T, model *vfs.FaultFS) *manifest.Log {
	t.Helper()
	log, err := manifest.Create(model, "/", 1, initial)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

func table(level int, number, seq uint64, min, max string) manifest.FileMeta {
	return manifest.FileMeta{Level: level, Number: number, Size: 100, MinSeq: seq, MaxSeq: seq, MinKey: []byte(min), MaxKey: []byte(max)}
}

func TestVersionPinsAndLookups(t *testing.T) {
	model := vfs.NewFaultFS()
	log := create(t, model)
	set := version.NewSet()
	if err := set.Replay(initial); err != nil {
		t.Fatal(err)
	}
	added := []manifest.FileMeta{table(0, 3, 5, "a", "f"), table(0, 4, 8, "a", "g"), table(1, 5, 3, "a", "c"), table(1, 6, 4, "d", "f")}
	if err := set.Commit(manifest.Edit{Added: added, LastSequence: 8, NextFileNumber: 7}, log.Append); err != nil {
		t.Fatal(err)
	}
	added[0].MinKey[0] = 'z'
	first := set.Acquire()
	first.Version().Files(1)[1].MinKey[0] = 'z'
	var got []uint64
	first.Version().VisitCandidates([]byte("d"), func(n uint64) bool { got = append(got, n); return true })
	if !slices.Equal(got, []uint64{4, 3, 6}) {
		t.Fatalf("candidates %v", got)
	}
	clone := first.Clone()
	if err := set.Commit(manifest.Edit{Deleted: []manifest.FileRef{{Level: 0, Number: 3}}, Added: []manifest.FileMeta{table(1, 7, 6, "g", "h")}, NextFileNumber: 8}, log.Append); err != nil {
		t.Fatal(err)
	}
	second := set.Acquire()
	if err := set.Commit(manifest.Edit{Deleted: []manifest.FileRef{{Level: 1, Number: 5}}}, log.Append); err != nil {
		t.Fatal(err)
	}
	first.Release()
	first.Release()
	if first.Clone() != nil || len(set.TakeObsolete()) != 0 {
		t.Fatal("released a still-pinned file")
	}
	clone.Release()
	if got := set.TakeObsolete(); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("got obsolete %v", got)
	}
	second.Release()
	if got := set.TakeObsolete(); !slices.Equal(got, []uint64{5}) {
		t.Fatalf("got obsolete %v", got)
	}
	if err := set.Commit(manifest.Edit{Added: []manifest.FileMeta{table(0, 3, 8, "i", "j")}}, log.Append); !errors.Is(err, version.ErrInvalidEdit) {
		t.Fatalf("reuse: %v", err)
	}
	if err := set.Commit(manifest.Edit{Added: []manifest.FileMeta{table(1, 8, 8, "f", "g")}, NextFileNumber: 9}, log.Append); !errors.Is(err, version.ErrInvalidEdit) {
		t.Fatalf("overlap: %v", err)
	}
	log.Close()
	model.Crash()
	recovered := version.NewSet()
	log, _, err := manifest.Open(model, "/", func(r manifest.Record) error { return recovered.Replay(r.Edit) })
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	pin := recovered.Acquire()
	defer pin.Release()
	if files := pin.Version().Files(1); len(files) != 2 || files[0].Number != 6 || files[1].Number != 7 {
		t.Fatalf("recovered %+v", files)
	}
}

func TestVersionPublicationAndSyncFailure(t *testing.T) {
	model := vfs.NewFaultFS()
	log := create(t, model)
	set := version.NewSet()
	if err := set.Replay(initial); err != nil {
		t.Fatal(err)
	}
	entered, allow := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- set.Commit(manifest.Edit{LastSequence: 9}, func(e manifest.Edit) error {
			close(entered)
			<-allow
			return log.Append(e)
		})
	}()
	<-entered
	var readers sync.WaitGroup
	for range 10 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			p := set.Acquire()
			defer p.Release()
			if p.Version().LastSequence() != 0 {
				t.Error("published before sync")
			}
		}()
	}
	readers.Wait()
	close(allow)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	model.FailAfter(1)
	if err := set.Commit(manifest.Edit{LastSequence: 10}, log.Append); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("sync error: %v", err)
	}
	model.FailAfter(-1)
	if err := set.Commit(manifest.Edit{LastSequence: 11}, log.Append); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("log reused after failure: %v", err)
	}
	model.Crash()
	recovered := version.NewSet()
	opened, _, err := manifest.Open(model, "/", func(r manifest.Record) error { return recovered.Replay(r.Edit) })
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	pin := recovered.Acquire()
	defer pin.Release()
	if pin.Version().LastSequence() != 9 {
		t.Fatal("lost the previously synced edit")
	}
}
