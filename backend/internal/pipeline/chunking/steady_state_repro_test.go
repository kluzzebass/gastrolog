package chunking_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Steady-state chunking reproduction for gastrolog-68sfsl.
//
// The 18h cluster run showed a cloud-backed, age-rotation, short-give-up-TTL
// vault seal exactly ONE chunk while shedding 43k never-chunked segments — even
// though its home held ctl-leadership for ~18 of 18h. These tests reproduce the
// mechanism deterministically (injected clock, no wall-clock sleeps, no async
// worker) at the chunking planner + give-up layer:
//
//   - HealthyHolders: when collection delivers a second holder for each
//     segment, the planner seals continuously and NOTHING is given up.
//   - MissingSecondHolder: when collection never delivers a second holder, the
//     min(2, placement) planning-eligibility gate refuses to chunk segments the
//     leader fully holds locally; they age past the give-up TTL and are shed —
//     ZERO seals, exactly matching the cluster. The standing give-up alarm
//     (Task 2) annunciates the starvation and clears once shedding stops.
//
// The gate itself is correct (gastrolog-4bl9xx: a single-copy chunk wedges the
// seal queue if its sole holder dies). The disease these tests localize is
// upstream: collection failing to deliver the second holder within the TTL.

func TestSteadyStateSealsWithHealthyHolders(t *testing.T) {
	t.Parallel()
	h := newSteadyHarness(t, chunking.ManifestRotationPolicy{MaxAge: 2 * time.Minute}, 5*time.Minute, []string{"leader", "peer"})

	const tick = 30 * time.Second
	for i := 0; i < 20; i++ {
		h.advance(tick)
		seg, recs := h.publishLocal(3)
		h.published += recs
		h.ackHolder(seg, "leader")
		h.ackHolder(seg, "peer") // healthy collection: second holder arrives promptly
		h.planToFixpoint()
		h.releaseToFixpoint()
	}

	if h.sealedRecords() == 0 {
		t.Fatalf("healthy steady state produced NO seals (published=%d)", h.published)
	}
	if raised, _ := h.sink.snapshot(); len(raised) != 0 {
		t.Fatalf("healthy steady state raised alarms: %v", raised)
	}
	t.Logf("HEALTHY: publishedRecords=%d sealedRecords=%d sealedChunks=%d registryRemaining=%d",
		h.published, h.sealedRecords(), h.sealedChunks(), h.registryLen())
}

func TestSteadyStateGiveUpRaisesAndClearsStandingAlert(t *testing.T) {
	t.Parallel()
	h := newSteadyHarness(t, chunking.ManifestRotationPolicy{MaxAge: 2 * time.Minute}, 5*time.Minute, []string{"leader", "peer"})

	// Shedding phase: only the leader holds each segment (peer collection never
	// delivers the second holder). The planner cannot chunk below min(2,
	// placement) holders, so segments age past the 5m give-up TTL and are shed.
	const tick = 30 * time.Second
	for i := 0; i < 40; i++ {
		h.advance(tick)
		seg, recs := h.publishLocal(3)
		h.published += recs
		h.ackHolder(seg, "leader") // peer never collects → stuck at 1 holder
		h.planToFixpoint()
		h.releaseToFixpoint()
	}

	if h.sealedRecords() != 0 {
		t.Fatalf("expected ZERO seals while the second holder is missing, got %d records", h.sealedRecords())
	}
	raised, _ := h.sink.snapshot()
	if _, ok := raised[sinkAlarmID("chunking-retention-giveup", h.vaultID.String())]; !ok {
		t.Fatalf("sustained give-up did NOT raise the chunking-retention-giveup alarm; raised=%v", raised)
	}
	t.Logf("STARVED: publishedRecords=%d sealedRecords=%d registryRemaining=%d alarmRaised=true",
		h.published, h.sealedRecords(), h.registryLen())

	// Recovery phase: collection recovers and delivers the second holder for the
	// segments still in the registry. They cross the min(2, placement) gate, get
	// chunked, and the vault seals a chunk again — which clears the standing
	// give-up alarm.
	for _, e := range h.fsm.ListCompletedSegments() {
		h.ackHolder(e.SegmentID, "peer")
	}
	sealedBefore := h.sealedRecords()
	for i := 0; i < 10 && h.sealedRecords() <= sealedBefore; i++ {
		h.advance(tick) // let the recovered manifest age past MaxAge and seal
		h.planToFixpoint()
	}
	if h.sealedRecords() <= sealedBefore {
		t.Fatalf("recovery did not seal any chunk (sealedRecords stayed %d)", h.sealedRecords())
	}
	if raised, _ := h.sink.snapshot(); len(raised) != 0 {
		t.Fatalf("give-up alarm did not clear after the vault sealed a chunk again: %v", raised)
	}
}

// ---- harness ----

type steadyHarness struct {
	t         *testing.T
	mgr       *chunking.Manager
	fsm       *vaultctlfsm.FSM
	applier   *fsmApplier
	sink      *recordingAlertSink
	vaultID   glid.GLID
	vaultRoot string
	mu        sync.Mutex
	now       time.Time
	published int
}

func newSteadyHarness(t *testing.T, policy chunking.ManifestRotationPolicy, giveUpTTL time.Duration, placement []string) *steadyHarness {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	h := &steadyHarness{
		t:         t,
		fsm:       vaultctlfsm.New(),
		sink:      &recordingAlertSink{},
		vaultID:   glid.New(),
		vaultRoot: t.TempDir(),
		now:       base,
	}
	h.applier = &fsmApplier{fsm: h.fsm}
	h.mgr = chunking.New(chunking.Config{})
	if err := h.mgr.RegisterVault(h.vaultID, chunking.VaultConfig{
		RequiredHolders: func() ([]string, bool) { return placement, true },
		VaultRoot:       h.vaultRoot,
		ChunkRoot:       filepath.Join(h.vaultRoot, "chunks"),
		FSM:             h.fsm,
		Locate:          chunking.VaultSegmentLocator{Root: h.vaultRoot},
		Applier:         h.applier,
		IsLeader:        func() bool { return true },
		Policy:          policy,
		Now:             h.nowFn,
		Alerts:          h.sink,
		RetentionGiveUpTTL: func() (time.Duration, bool) {
			if giveUpTTL > 0 {
				return giveUpTTL, true
			}
			return 0, false
		},
	}); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *steadyHarness) nowFn() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.now }
func (h *steadyHarness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

func (h *steadyHarness) publishLocal(records int) (glid.GLID, int) {
	h.t.Helper()
	segID := glid.New()
	now := h.nowFn()
	recs := make([]recordForSeg, records)
	for r := 0; r < records; r++ {
		recs[r] = recordForSeg{uint32(r), now.Add(time.Duration(r) * time.Millisecond), "rec"}
	}
	writeCompletedSegment(h.t, h.vaultRoot, segID, h.vaultID, recs)
	publishSegment(h.t, h.fsm, segID, now, uint32(records), now, now.Add(time.Duration(records)*time.Millisecond))
	return segID, records
}

func (h *steadyHarness) ackHolder(segID glid.GLID, nodeID string) {
	h.t.Helper()
	applyChunkCmd(h.t, h.fsm, vaultctlfsm.MarshalAckSegmentHolder(segID, nodeID))
}

func (h *steadyHarness) planToFixpoint() {
	h.t.Helper()
	for step := 0; step < 128; step++ {
		before := h.fsm.OpenChunk()
		beforeSealed := h.fsm.SealedManifest() != nil
		if err := h.mgr.PlanOnce(context.Background(), h.vaultID); err != nil {
			h.t.Fatalf("PlanOnce: %v", err)
		}
		if h.fsm.SealedManifest() != nil {
			_ = h.mgr.BuildOnce(context.Background(), h.vaultID)
		}
		after := h.fsm.OpenChunk()
		afterSealed := h.fsm.SealedManifest() != nil
		if manifestSame(before, after) && beforeSealed == afterSealed {
			break
		}
	}
}

func (h *steadyHarness) releaseToFixpoint() {
	h.t.Helper()
	for step := 0; step < 64; step++ {
		before := h.registryLen()
		if err := h.mgr.ReleaseOnce(context.Background(), h.vaultID); err != nil {
			h.t.Fatalf("ReleaseOnce: %v", err)
		}
		if h.registryLen() == before {
			break
		}
	}
}

func (h *steadyHarness) sealedRecords() int {
	for _, s := range h.mgr.SealStats() {
		if s.VaultID == h.vaultID {
			return int(s.SealedRecords)
		}
	}
	return 0
}

func (h *steadyHarness) sealedChunks() int {
	for _, s := range h.mgr.StageStats() {
		if s.VaultID == h.vaultID {
			return int(s.ChunksSealed)
		}
	}
	return 0
}

func (h *steadyHarness) registryLen() int { return len(h.fsm.ListCompletedSegments()) }

func manifestSame(a, b *vaultctlfsm.OpenChunkManifest) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return a.ChunkID == b.ChunkID && len(a.Refs) == len(b.Refs) && a.TotalRecords == b.TotalRecords
}
