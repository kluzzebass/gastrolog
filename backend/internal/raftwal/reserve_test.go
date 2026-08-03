package raftwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// segPath builds the segment path for a sequence number, mirroring the WAL's
// own naming.
func segPath(dir string, seq int) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d%s", walFilePrefix, seq, walFileSuffix))
}

// TestWALSpareSegmentLifecycle pins the reserve invariant: from Open onward
// the NEXT segment always exists (created and reserved ahead of need), and
// rotation promotes it instead of allocating at crisis time.
func TestWALSpareSegmentLifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Small target so a handful of appends forces rotation, and a scavenge
	// threshold well under it so reclamation leaves these live segments
	// alone — the reserve lifecycle is what is under test here.
	w, err := Open(dir, Config{SegmentTargetSize: 4096, ScavengeMaxLiveBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	// Open rotates to segment 1 and must leave segment 2 reserved.
	if _, err := os.Stat(segPath(dir, 2)); err != nil {
		t.Fatalf("spare segment must exist right after Open: %v", err)
	}
	if w.ReserveLost() {
		t.Fatal("reserve must be intact after Open")
	}

	// Write past the target: rotation must promote the spare (segment 2
	// becomes active) and reserve a new spare (segment 3).
	gs := w.GroupStore("g")
	payload := make([]byte, 1024)
	for i := uint64(1); i <= 8; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if _, err := os.Stat(segPath(dir, 3)); err != nil {
		t.Fatalf("rotation must reserve the next spare: %v", err)
	}

	// The promoted spare carries real data now.
	info, err := os.Stat(segPath(dir, 2))
	if err != nil || info.Size() == 0 {
		t.Fatalf("promoted spare must hold appended data: size=%v err=%v", info, err)
	}
}

// TestWALStaleSpareCleanupOnReopen pins the restart path: the previous run's
// reserved spare (logically empty, physically reserved) must be removed on
// reopen so restarts never accumulate orphaned reservations.
func TestWALStaleSpareCleanupOnReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("g")
	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for reopen := 1; reopen <= 3; reopen++ {
		w, err = Open(dir)
		if err != nil {
			t.Fatalf("reopen %d: %v", reopen, err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// After any number of reopens: data segments plus the active segment
	// plus exactly ONE spare — no graveyard of empty reserved files.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var empty int
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			empty++
		}
	}
	if empty > 2 {
		t.Fatalf("stale spares accumulated: %d empty segments (want ≤ 2: last active + spare)", empty)
	}

	// And the data survived every cleanup pass.
	w, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	gs = w.GroupStore("g")
	log := new(hraft.Log)
	if err := gs.GetLog(1, log); err != nil {
		t.Fatalf("data lost across spare cleanup: %v", err)
	}
}

// TestWALReserveFailureDegradesGracefully pins the degraded mode: when
// preallocation fails, appends still succeed (ordinary allocation), the loss
// is reported exactly once, and a later successful rotation restores it.
func TestWALReserveFailureDegradesGracefully(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Both hooks run on the batch writer, and reclamation can rotate after
	// the batch round-trip has already returned — so neither the flag nor
	// the transition log can rely on submit for happens-before.
	var failing atomic.Bool
	failing.Store(true)
	var mu sync.Mutex
	var transitions []bool
	readTransitions := func() []bool {
		mu.Lock()
		defer mu.Unlock()
		return append([]bool(nil), transitions...)
	}
	w, err := Open(dir, Config{
		SegmentTargetSize: 2048,
		SegmentPreallocate: func(_ *os.File, _ int64) error {
			if failing.Load() {
				return errors.New("injected: no space left on device")
			}
			return nil
		},
		OnReserveState: func(lost bool, _ error) {
			mu.Lock()
			transitions = append(transitions, lost)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if !w.ReserveLost() {
		t.Fatal("reserve must report lost while preallocation fails")
	}
	if got := readTransitions(); len(got) != 1 || !got[0] {
		t.Fatalf("exactly one lost transition expected, got %v", got)
	}

	// Degraded, not broken: appends keep working.
	gs := w.GroupStore("g")
	payload := make([]byte, 512)
	for i := uint64(1); i <= 4; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("append under lost reserve must still succeed: %v", err)
		}
	}

	// Space returns: the next rotation restores the reserve and reports it.
	failing.Store(false)
	for i := uint64(5); i <= 12; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if w.ReserveLost() {
		t.Fatal("reserve must be restored after a successful rotation")
	}
	if got := readTransitions(); len(got) != 2 || got[1] {
		t.Fatalf("lost-then-restored transitions expected, got %v", got)
	}
}

// TestWALReplayStopsAtZeroHeader pins the zero-region guard: CRC32 of an
// empty payload is 0, so a zeroed header would otherwise read as a valid
// empty entry and replay would walk an entire zeroed region.
func TestWALReplayStopsAtZeroHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("g")
	if err := gs.StoreLog(&hraft.Log{Index: 7, Term: 3, Data: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a zeroed region to the data segment — the shape of a
	// preallocated tail or a torn write — followed by a decoy entry that
	// must NOT be replayed (data past a zero header is unreachable).
	seg := dataSegment(t, dir)
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 4*headerSize)); err != nil {
		t.Fatal(err)
	}
	decoy := []byte("ghost")
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], 1)
	hdr[4] = byte(entryLog)
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(decoy)))
	binary.LittleEndian.PutUint32(hdr[9:13], crc32.Checksum(decoy, crc32Table))
	if _, err := f.Write(append(hdr, decoy...)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = Open(dir)
	if err != nil {
		t.Fatalf("replay over a zeroed region must succeed: %v", err)
	}
	defer func() { _ = w.Close() }()
	gs = w.GroupStore("g")
	log := new(hraft.Log)
	if err := gs.GetLog(7, log); err != nil {
		t.Fatalf("entry before the zero region must survive: %v", err)
	}
	if string(log.Data) != "kept" {
		t.Fatalf("log data = %q, want %q", log.Data, "kept")
	}
	last, _ := gs.LastIndex()
	if last != 7 {
		t.Fatalf("replay must stop at the zero header: last index = %d, want 7", last)
	}
}

// dataSegment returns the single non-empty WAL segment in dir.
func dataSegment(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var found string
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 0 {
			if found != "" {
				t.Fatalf("expected one data segment, found %s and %s", found, e.Name())
			}
			found = filepath.Join(dir, e.Name())
		}
	}
	if found == "" {
		t.Fatal("no data segment found")
	}
	return found
}

// TestWALReclamationProceedsWhenSpareReservationFails pins the ENOSPC-safe
// reclamation property: reclamation only unlinks drained segments, so it frees
// space even when reserving the NEXT spare fails (a full disk). Without this,
// reclamation — the mechanism that frees WAL space — could itself be blocked
// at ENOSPC.
func TestWALReclamationProceedsWhenSpareReservationFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	var reclaims atomic.Int64
	// The hook runs on the batch writer, including from reclamation after
	// the batch round-trip returned: the flag needs its own synchronization.
	var failSpare atomic.Bool
	w, err := Open(dir, Config{
		SegmentTargetSize:    2048,
		ScavengeMaxLiveBytes: 512,
		OnReclaim:            func(ReclaimStats) { reclaims.Add(1) },
		SegmentPreallocate: func(_ *os.File, _ int64) error {
			if failSpare.Load() {
				return errors.New("injected: no space left on device")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	gs := w.GroupStore("g")
	payload := make([]byte, 512)
	for i := uint64(1); i <= 20; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}

	// Space runs out: the pre-existing spare is still reserved (promotion
	// allocates nothing), but the next rotation cannot reserve a new one.
	failSpare.Store(true)
	for i := uint64(21); i <= 26; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: payload}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if !w.ReserveLost() {
		t.Fatal("reserve must report lost once a rotation cannot reserve the next spare")
	}

	// Reclamation must still free space on the full disk — it unlinks, and
	// scavenges through the already-reserved active segment, so it needs no
	// new allocation to make progress.
	if err := gs.DeleteRange(1, 26); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	syncBarrier(t, w)

	if reclaims.Load() == 0 {
		t.Fatal("reclamation must free segments even when the next spare can't be reserved")
	}
	assertLiveBytesInvariant(t, w, "after reclamation")
	if !w.ReserveLost() {
		t.Fatal("reserve must still report lost after reclamation")
	}
}

// TestWALPreallocatePlatform smoke-tests the real platform syscall: the
// reservation must succeed on a fresh file and must not change its logical
// size (replay's EOF-is-end-of-data invariant).
func TestWALPreallocatePlatform(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "prealloc")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if err := preallocate(f, 1<<20); err != nil {
		t.Fatalf("platform preallocate: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("preallocation must not change logical size, got %d", info.Size())
	}
}
