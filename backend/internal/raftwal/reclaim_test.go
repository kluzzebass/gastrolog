package raftwal

import (
	"testing"

	hraft "github.com/hashicorp/raft"
)

// openTestWAL opens a WAL in a temp dir with small segments so rotation and
// reclamation exercise real code paths without large I/O.
func openTestWAL(t *testing.T, cfg Config) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	if cfg.SegmentTargetSize == 0 {
		cfg.SegmentTargetSize = 2048
	}
	// Removed in the compaction-retirement task along with the field.
	if cfg.CompactionMinSegments == 0 {
		cfg.CompactionMinSegments = 1 << 30
	}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func TestStableAndRegLocationsTracked(t *testing.T) {
	w, _ := openTestWAL(t, Config{})
	gs := w.GroupStore("grp")
	if err := gs.Set([]byte("CurrentTerm"), []byte{9}); err != nil {
		t.Fatalf("set: %v", err)
	}

	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state := w.groups[gs.groupID]
	if state.regName != "grp" {
		t.Errorf("regName = %q, want %q", state.regName, "grp")
	}
	if state.regLoc.length == 0 {
		t.Errorf("regLoc not recorded: %+v", state.regLoc)
	}
	sv, ok := state.stable["CurrentTerm"]
	if !ok {
		t.Fatal("stable key missing")
	}
	if len(sv.value) != 1 || sv.value[0] != 9 {
		t.Errorf("stable value = %v", sv.value)
	}
	if sv.loc.length == 0 || sv.loc.seg == 0 {
		t.Errorf("stable loc not recorded: %+v", sv.loc)
	}
	var _ = hraft.Log{} // imports used heavily by later tests in this file
}
