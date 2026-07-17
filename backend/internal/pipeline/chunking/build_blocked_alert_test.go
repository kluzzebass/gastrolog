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

func TestBlockedBuildRaisesAndClearsAlert(t *testing.T) {
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

	sink := &recordingAlertSink{}
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

	mgr := chunking.New(chunking.Config{Alerts: sink})
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
	// First pass starts tracking; no alert inside the grace period.
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should fail: referenced segment file is missing")
	}
	if active, _ := sink.snapshot(); len(active) != 0 {
		t.Fatalf("alert raised inside grace period: %v", active)
	}

	// Still blocked past the grace period: the alert must fire.
	advance(3 * time.Minute)
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("BuildOnce should still fail")
	}
	active, _ := sink.snapshot()
	if len(active) != 1 {
		t.Fatalf("want 1 active alert after grace period, got %v", active)
	}

	// Segment appears (e.g. collection caught up / operator restored it):
	// the build completes and the alert clears.
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "payload"}})
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce after segment restored: %v", err)
	}
	if e := fsm.Get(chunkID); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk after restore = %+v, want Sealed", e)
	}
	active, cleared := sink.snapshot()
	if len(active) != 0 || cleared == 0 {
		t.Fatalf("alert not cleared after successful build: active=%v cleared=%d", active, cleared)
	}
}
