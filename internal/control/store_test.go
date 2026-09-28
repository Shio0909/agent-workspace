package control

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// saveSnapshot writes the legacy JSON snapshot format. Tests use it to seed a
// data directory in one write; OpenStore imports it on first open.
func saveSnapshot[T any](path string, items map[string]T) error {
	b, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}

func TestStoreExcludesSecondProcessAndRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenStore(dir); err == nil {
		other.Close()
		t.Fatal("allowed two controllers")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenStore(dir); err == nil {
		other.Close()
		t.Fatal("silently reset corrupted data")
	}
}

func TestStoreImportsLegacySnapshotOnce(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	items := map[string]Workspace{"demo": {ID: "demo", Profile: "demo", Desired: DesiredRunning, LastActivity: now}}
	ops := map[string]Operation{"biz-1": {BizID: "biz-1", Workspace: "demo", Type: OpStart, Status: OpSuccess}}
	if err := saveSnapshot(filepath.Join(dir, "workspaces.json"), items); err != nil {
		t.Fatal(err)
	}
	if err := saveSnapshot(filepath.Join(dir, "operations.json"), ops); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if w, err := s.Get("demo"); err != nil || !w.LastActivity.Equal(now) {
		t.Fatalf("workspace not imported: %+v %v", w, err)
	}
	if _, err := s.GetOperation("biz-1"); err != nil {
		t.Fatalf("operation not imported: %v", err)
	}
	if err := s.Put(Workspace{ID: "demo", Profile: "demo", Desired: DesiredStopped}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := os.Stat(filepath.Join(dir, "workspaces.json.migrated")); err != nil {
		t.Fatalf("legacy snapshot was not kept for rollback: %v", err)
	}
	// A stale snapshot reappearing later must not overwrite newer state.
	if err := saveSnapshot(filepath.Join(dir, "workspaces.json"), items); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if w, _ := s.Get("demo"); w.Desired != DesiredStopped {
		t.Fatalf("stale snapshot was imported again: %+v", w)
	}
}

func TestStoreRejectsCorruptLegacySnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workspaces.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenStore(dir); err == nil {
		s.Close()
		t.Fatal("started with an unreadable legacy snapshot")
	}
	// The failed import must not have marked the migration as done.
	if err := os.WriteFile(filepath.Join(dir, "workspaces.json"), []byte(`{"demo":{"id":"demo"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Get("demo"); err != nil {
		t.Fatalf("repaired snapshot not imported: %v", err)
	}
}

// TestStoreConcurrentWritesSurviveReopen checks group commit end to end: every
// acknowledged write from many concurrent writers, including repeated writes
// of one key, is on disk with its last acknowledged value.
func TestStoreConcurrentWritesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const writers, rounds = 32, 50
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				// Half the writers share keys, so same-key ordering is exercised.
				id := fmt.Sprintf("w-%d", g%(writers/2))
				if err := s.Put(Workspace{ID: id, Profile: "demo", LastError: fmt.Sprintf("%d-%d", g, r)}); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	want := map[string]string{}
	for _, w := range s.List() {
		want[w.ID] = w.LastError
	}
	s.Close()
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := s.List()
	if len(got) != writers/2 {
		t.Fatalf("got %d workspaces, want %d", len(got), writers/2)
	}
	for _, w := range got {
		if want[w.ID] != w.LastError {
			t.Fatalf("%s: disk has %q, memory had %q", w.ID, w.LastError, want[w.ID])
		}
	}
}
