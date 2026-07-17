package chunking_test

// Blocked-build operator alert (gastrolog-67c9b0 follow-up): a sealed
// manifest whose referenced segment files are missing on this node pins the
// vault's serial seal queue. That condition was previously silent on
// follower homes (ErrAwaitingLocalSegments → nil) and a generic warn on the
// leader — ~60 chunks sat in Sealing for a day with no operator signal.

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

type recordingAlertSink struct {
	mu      sync.Mutex
	active  map[string]string
	cleared int
}

func (s *recordingAlertSink) Raise(typeID, instanceKey, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		s.active = make(map[string]string)
	}
	s.active[sinkAlarmID(typeID, instanceKey)] = detail
}

func (s *recordingAlertSink) RaiseOperator(a alert.OperatorAlarm) {
	s.Raise(a.TypeID, a.InstanceKey, a.Detail)
}

func (s *recordingAlertSink) Clear(typeID, instanceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, sinkAlarmID(typeID, instanceKey))
	s.cleared++
}

// sinkAlarmID mirrors the collector's type:instance ID composition.
func sinkAlarmID(typeID, instanceKey string) string {
	if instanceKey == "" {
		return typeID
	}
	return typeID + ":" + instanceKey
}

func (s *recordingAlertSink) snapshot() (map[string]string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.active))
	for k, v := range s.active {
		out[k] = v
	}
	return out, s.cleared
}

// TestBlockedBuildRaisesAndClearsAlert drives the blocked-build condition
// against a REAL alert.Collector sharing the vault's deterministic clock:
// the grace window is the catalog's DelayOn (gastrolog-4wvxqh), enforced by
// the collector, so this is the black-box behavior an operator sees — no
// alarm for a transient block, an alarm once it sustains, clear on heal.
func TestBlockedBuildRaisesAndClearsAlert(t *testing.T) {
	t.Parallel()
	blockedType, ok := alert.TypeByID("chunking-build-blocked")
	if !ok || blockedType.DelayOn <= 0 {
		t.Fatal("chunking-build-blocked must carry a catalog DelayOn — the call site no longer times the grace period")
	}
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base,
		Checksum:      1,
		PublishedAt:   base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        4096,
		RefAddedAt:        base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	now := base.Add(time.Minute)
	var nowMu sync.Mutex
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		now = now.Add(d)
		nowMu.Unlock()
	}
	collector := alert.NewWithClock(nowFn)

	mgr := chunking.New(chunking.Config{Alerts: collector})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
		Now:       nowFn,
	}); err != nil {
		t.Fatal(err)
	}

	// The referenced segment file was never written: the build is blocked.
	// The raw condition is raised, but the catalog delay-on holds the
	// alarm back inside the grace window.
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should fail: referenced segment file is missing")
	}
	if got := collector.Count(); got != 0 {
		t.Fatalf("alarm active inside the delay-on window: %d", got)
	}

	// Still blocked past the window: the alarm must be active.
	advance(blockedType.DelayOn + time.Minute)
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should still fail")
	}
	active := collector.Active()
	if len(active) != 1 {
		t.Fatalf("want 1 active alarm after the delay-on window, got %v", active)
	}
	if active[0].ID != "chunking-build-blocked:"+vaultID.String() {
		t.Fatalf("alarm ID = %q", active[0].ID)
	}

	// Segment appears (e.g. collection caught up / operator restored it):
	// the build completes and the alarm clears.
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "payload"}})
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce after segment restored: %v", err)
	}
	if e := fsm.Get(chunkID); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk after restore = %+v, want Sealed", e)
	}
	if got := collector.Count(); got != 0 {
		t.Fatalf("alarm not cleared after successful build: %d", got)
	}
}

// TestBlockedBuildTransientBlipNeverAlarms pins the suppression itself: a
// block that heals inside the delay-on window (routine collection catch-up)
// must never annunciate — the chattering EEMUA 191 principle 3 forbids.
func TestBlockedBuildTransientBlipNeverAlarms(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base,
		Checksum:      1,
		PublishedAt:   base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        4096,
		RefAddedAt:        base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	now := base.Add(time.Minute)
	var nowMu sync.Mutex
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		now = now.Add(d)
		nowMu.Unlock()
	}
	collector := alert.NewWithClock(nowFn)

	mgr := chunking.New(chunking.Config{Alerts: collector})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
		Now:       nowFn,
	}); err != nil {
		t.Fatal(err)
	}

	// Blocked for two passes 30s apart, then the segment shows up: the
	// alarm must never have existed at any observable point.
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should fail: referenced segment file is missing")
	}
	advance(30 * time.Second)
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should still fail")
	}
	if got := collector.Count(); got != 0 {
		t.Fatalf("transient block annunciated: %d", got)
	}
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "payload"}})
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce after segment restored: %v", err)
	}
	// Even long after: the healed blip must not resurface.
	advance(time.Hour)
	if got := collector.Count(); got != 0 {
		t.Fatalf("healed transient block resurfaced as an alarm: %d", got)
	}
}
