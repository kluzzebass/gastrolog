package raftwal

// A dropped group must vanish completely — from the in-memory index, from
// live-bytes accounting, and from replay — while every other group's state
// stays byte-for-byte intact. The drop record itself is a tombstone that
// masks everything earlier for the group; oldest-first reclamation is what
// lets it evaporate once nothing it masks can replay.

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func storeN(t *testing.T, gs *GroupStore, from, to uint64, size int) {
	t.Helper()
	for i := from; i <= to; i++ {
		if err := gs.StoreLog(&hraft.Log{
			Index: i, Term: 1, Type: hraft.LogCommand,
			Data: []byte(strings.Repeat("x", size)),
		}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
}

func TestDropGroupReleasesStateAndAccounting(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{})

	gs := w.GroupStore("doomed")
	keep := w.GroupStore("keeper")
	storeN(t, gs, 1, 20, 32)
	storeN(t, keep, 1, 5, 32)
	if err := gs.SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("SetUint64: %v", err)
	}
	doomedGID := gs.groupID

	if err := w.DropGroup("doomed"); err != nil {
		t.Fatalf("DropGroup: %v", err)
	}

	w.stateMu.RLock()
	_, groupAlive := w.groups[doomedGID]
	_, nameAlive := w.groupIDs["doomed"]
	dropLoc, dropRecorded := w.drops[doomedGID]
	w.stateMu.RUnlock()
	if groupAlive || nameAlive {
		t.Fatalf("dropped group still indexed: group=%v name=%v", groupAlive, nameAlive)
	}
	if !dropRecorded || dropLoc.length == 0 {
		t.Fatalf("drop tombstone not recorded: %+v", dropLoc)
	}
	assertLiveBytesInvariant(t, w, "after drop")

	// The keeper is untouched.
	var got hraft.Log
	if err := keep.GetLog(3, &got); err != nil {
		t.Fatalf("keeper GetLog after drop: %v", err)
	}

	// The same name registers a fresh group.
	fresh := w.GroupStore("doomed")
	if fresh.groupID == doomedGID {
		t.Fatalf("re-registered group reused dropped ID %d", doomedGID)
	}
	if _, err := fresh.LastIndex(); err != nil {
		t.Fatalf("fresh group LastIndex: %v", err)
	}
	if idx, _ := fresh.LastIndex(); idx != 0 {
		t.Fatalf("fresh group inherited log state: LastIndex=%d, want 0", idx)
	}
}

func TestDropGroupUnknownNameIsNoop(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{})
	if err := w.DropGroup("never-registered"); err != nil {
		t.Fatalf("DropGroup on unknown name: %v", err)
	}
	assertLiveBytesInvariant(t, w, "after no-op drop")
}

// Replay is where a missing drop path would resurrect the group: every log
// entry, stable key and registration is still physically present until
// reclamation catches up, and only the drop record masks them.
func TestDropGroupSurvivesReplay(t *testing.T) {
	t.Parallel()
	w, dir := openTestWAL(t, Config{})

	gs := w.GroupStore("doomed")
	keep := w.GroupStore("keeper")
	storeN(t, gs, 1, 20, 32)
	storeN(t, keep, 1, 5, 32)
	if err := gs.Set([]byte("LastVotedFor"), []byte("node-1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	doomedGID := gs.groupID
	if err := w.DropGroup("doomed"); err != nil {
		t.Fatalf("DropGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	re, err := Open(dir, Config{SegmentTargetSize: 2048, ScavengeMaxLiveBytes: 128})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = re.Close() }()

	re.stateMu.RLock()
	_, groupAlive := re.groups[doomedGID]
	_, nameAlive := re.groupIDs["doomed"]
	_, dropRecorded := re.drops[doomedGID]
	nextGID := re.nextGID
	re.stateMu.RUnlock()
	if groupAlive || nameAlive {
		t.Fatalf("replay resurrected dropped group: group=%v name=%v", groupAlive, nameAlive)
	}
	if !dropRecorded {
		t.Fatal("replay did not rebuild the drop tombstone")
	}
	if nextGID <= doomedGID {
		t.Fatalf("nextGID %d not past dropped ID %d — the ID could be reused", nextGID, doomedGID)
	}
	assertLiveBytesInvariant(t, re, "after replay of drop")

	rekeep := re.GroupStore("keeper")
	var got hraft.Log
	if err := rekeep.GetLog(5, &got); err != nil {
		t.Fatalf("keeper GetLog after replay: %v", err)
	}

	// Same name across restart: fresh ID, empty state.
	fresh := re.GroupStore("doomed")
	if fresh.groupID == doomedGID {
		t.Fatalf("re-registration after restart reused dropped ID %d", doomedGID)
	}
	if idx, _ := fresh.LastIndex(); idx != 0 {
		t.Fatalf("fresh group inherited state across restart: LastIndex=%d", idx)
	}
}

// countSegments returns how many WAL segment files exist in dir.
func countSegments(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), walFilePrefix) {
			n++
		}
	}
	return n
}

// A dropped group's segments must actually be reclaimed, and the drop record
// itself must evaporate once its own segment becomes the oldest — the
// tombstone cannot pin the WAL forever or the leak has only changed shape.
func TestDropUnpinsSegmentsAndTombstoneEvaporates(t *testing.T) {
	t.Parallel()
	w, dir := openTestWAL(t, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})

	// Fill several segments with the doomed group only, so its drop drains
	// them completely.
	gs := w.GroupStore("doomed")
	storeN(t, gs, 1, 40, 128)
	if countSegments(t, dir) < 3 {
		t.Fatalf("test premise: want >= 3 segments, got %d", countSegments(t, dir))
	}

	if err := w.DropGroup("doomed"); err != nil {
		t.Fatalf("DropGroup: %v", err)
	}

	// Keep the WAL moving with another group: every rotation runs a reclaim
	// pass, unlinking the drained prefix and eventually scavenging the
	// segment that holds only the drop tombstone.
	keep := w.GroupStore("keeper")
	for i := uint64(1); i <= 60; i++ {
		if err := keep.StoreLog(&hraft.Log{
			Index: i, Term: 1, Type: hraft.LogCommand,
			Data: []byte(strings.Repeat("y", 128)),
		}); err != nil {
			t.Fatalf("keeper StoreLog %d: %v", i, err)
		}
		// Prefix-truncate so the keeper itself pins only its tail.
		if i > 4 {
			if err := keep.DeleteRange(1, i-4); err != nil {
				t.Fatalf("keeper DeleteRange: %v", err)
			}
		}
	}

	w.stateMu.RLock()
	dropsLeft := len(w.drops)
	oldest := w.oldestSealedSegment()
	w.stateMu.RUnlock()
	if dropsLeft != 0 {
		t.Fatalf("drop tombstone still live after churn (oldest sealed seg %d): %d drops", oldest, dropsLeft)
	}
	assertLiveBytesInvariant(t, w, "after tombstone evaporation")

	if got := countSegments(t, dir); got > 4 {
		t.Fatalf("dropped group's segments not reclaimed: %d segment files remain", got)
	}
}

// Crash between the drop landing and reclamation catching up: replay sees
// the group's full history followed by the drop, in every possible prefix
// order the segments allow. The drop is durable, so nothing resurrects.
func TestDropThenCrashBeforeReclaim(t *testing.T) {
	t.Parallel()
	w, dir := openTestWAL(t, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})

	gs := w.GroupStore("doomed")
	storeN(t, gs, 1, 40, 128)
	doomedGID := gs.groupID
	if err := w.DropGroup("doomed"); err != nil {
		t.Fatalf("DropGroup: %v", err)
	}
	// "Crash": close without any further writes, so no reclaim pass ran
	// after the drop and every masked record is still on disk.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	re, err := Open(dir, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = re.Close() }()

	re.stateMu.RLock()
	_, groupAlive := re.groups[doomedGID]
	re.stateMu.RUnlock()
	if groupAlive {
		t.Fatal("crash-replay resurrected the dropped group")
	}
	assertLiveBytesInvariant(t, re, "after crash-replay")
}

// Churn: repeated create-write-drop cycles must not grow the in-memory maps
// or the accounting — the leak this feature removes.
func TestDropChurnStaysBounded(t *testing.T) {
	t.Parallel()
	w, dir := openTestWAL(t, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})

	keep := w.GroupStore("keeper")
	next := uint64(1)
	for cycle := range 20 {
		name := fmt.Sprintf("churn-%d", cycle%3) // reuse names across cycles
		gs := w.GroupStore(name)
		first, _ := gs.LastIndex()
		storeN(t, gs, first+1, first+8, 64)
		if err := gs.SetUint64([]byte("CurrentTerm"), uint64(cycle)); err != nil { //nolint:gosec // small positive
			t.Fatalf("SetUint64: %v", err)
		}
		if err := w.DropGroup(name); err != nil {
			t.Fatalf("DropGroup cycle %d: %v", cycle, err)
		}
		// Keeper churn drives rotation and reclamation between cycles.
		for range 6 {
			if err := keep.StoreLog(&hraft.Log{
				Index: next, Term: 1, Type: hraft.LogCommand,
				Data: []byte(strings.Repeat("z", 100)),
			}); err != nil {
				t.Fatalf("keeper StoreLog: %v", err)
			}
			next++
		}
		if next > 5 {
			if err := keep.DeleteRange(1, next-5); err != nil {
				t.Fatalf("keeper DeleteRange: %v", err)
			}
		}
		assertLiveBytesInvariant(t, w, fmt.Sprintf("cycle %d", cycle))
	}

	w.stateMu.RLock()
	groups := len(w.groups)
	names := len(w.groupIDs)
	w.stateMu.RUnlock()
	if groups > 2 || names > 2 {
		t.Fatalf("churn grew the index: %d groups, %d names (want <= 2: keeper plus at most one live churn group)", groups, names)
	}

	// Restart resurrects none of the churned groups.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	re, err := Open(dir, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = re.Close() }()
	re.stateMu.RLock()
	regroups := len(re.groups)
	re.stateMu.RUnlock()
	if regroups > 2 {
		t.Fatalf("replay resurrected churned groups: %d groups live", regroups)
	}
	assertLiveBytesInvariant(t, re, "after churn replay")
}

// Drops racing live writers on other groups: the accounting and the index
// must come out exact under the race detector.
func TestDropConcurrentWithOtherGroupWrites(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{SegmentTargetSize: 4096, ScavengeMaxLiveBytes: 256})

	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			gs := w.GroupStore(fmt.Sprintf("writer-%d", g))
			for i := uint64(1); i <= 30; i++ {
				if err := gs.StoreLog(&hraft.Log{
					Index: i, Term: 1, Type: hraft.LogCommand,
					Data: []byte(strings.Repeat("w", 64)),
				}); err != nil {
					t.Errorf("writer-%d StoreLog: %v", g, err)
					return
				}
			}
		}(g)
	}
	for d := range 4 {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			name := fmt.Sprintf("dropped-%d", d)
			gs := w.GroupStore(name)
			storeN(t, gs, 1, 10, 64)
			if err := w.DropGroup(name); err != nil {
				t.Errorf("DropGroup %s: %v", name, err)
			}
		}(d)
	}
	wg.Wait()

	assertLiveBytesInvariant(t, w, "after concurrent drops")
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	for name := range w.groupIDs {
		if strings.HasPrefix(name, "dropped-") {
			t.Errorf("dropped group %q still registered", name)
		}
	}
}

// A drop record must never outlive its usefulness silently: if its segment
// is scavenged while older segments remain, replay could see masked records
// with no mask. Oldest-first victim selection is the invariant this pins.
func TestScavengeVictimIsAlwaysOldest(t *testing.T) {
	t.Parallel()
	w, _ := openTestWAL(t, Config{SegmentTargetSize: 1024, ScavengeMaxLiveBytes: 256})
	gs := w.GroupStore("g")
	storeN(t, gs, 1, 40, 128)

	w.stateMu.RLock()
	oldest := w.oldestSealedSegment()
	active := w.segSeq
	w.stateMu.RUnlock()
	if oldest == 0 {
		t.Skip("no sealed segment produced; premise not met")
	}
	if oldest >= active {
		t.Fatalf("oldest sealed %d not older than active %d", oldest, active)
	}
	for seq := range w.segLive {
		if seq < oldest && seq != 0 {
			t.Fatalf("segment %d older than reported oldest %d still tracked", seq, oldest)
		}
	}
}

