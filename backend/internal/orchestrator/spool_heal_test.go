package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

type stubSpoolSlotFetcher struct {
	mu    sync.Mutex
	slots map[string]chunk.Record
}

func (s *stubSpoolSlotFetcher) slotKey(vaultID glid.GLID, seq uint64) string {
	return fmt.Sprintf("%s:%d", vaultID, seq)
}

func (s *stubSpoolSlotFetcher) set(vaultID glid.GLID, rec chunk.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots == nil {
		s.slots = make(map[string]chunk.Record)
	}
	s.slots[s.slotKey(vaultID, rec.VaultSeq)] = rec
}

func (s *stubSpoolSlotFetcher) ReadSpoolSeq(_ context.Context, _ string, vaultID glid.GLID, seq uint64) (chunk.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.slots[s.slotKey(vaultID, seq)]
	return rec, ok, nil
}

func TestSpoolSlotHealFillsAssignedMissing(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	rec1 := sequencedTestRecord("a", ingesterID, 1)
	rec1.VaultSeq = 1
	rec2 := sequencedTestRecord("b", ingesterID, 2)
	rec2.VaultSeq = 2
	rec3 := sequencedTestRecord("c", ingesterID, 3)
	rec3.VaultSeq = 3
	for _, rec := range []chunk.Record{rec1, rec3} {
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}
	fetcher := &stubSpoolSlotFetcher{}
	fetcher.set(vaultID, rec2)
	orch.SetSpoolSlotFetcher(fetcher)
	orch.mu.Lock()
	orch.vaults[vaultID].seqFanOutTargets = []system.ReplicationTarget{{NodeID: "peer-a"}}
	orch.mu.Unlock()
	store.setMaterializationWatermark(3)

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	orch.healAssignedMissingSpoolSlots(vaultID, fence, []uint64{2})

	if _, err := store.ReadByVaultSeq(context.Background(), vaultID, 2); err != nil {
		t.Fatalf("healed seq 2 not present: %v", err)
	}
	if got := orch.convergenceWatermark(vaultID); got != 3 {
		t.Fatalf("C_r = %d, want 3 after heal", got)
	}
}

func TestSpoolSlotHealDoesNotPullUnassignedGap(t *testing.T) {
	t.Parallel()
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 3, PrevBoundSeq: 0}
	alloc := vaultctlfsm.SeqAllocatorSnapshot{
		BurnedTails: []vaultctlfsm.SeqBurnedTail{{Start: 2, End: 2}},
	}
	holes := ClassifyFenceHoles(fence, alloc, func(uint64) bool { return false })
	for _, h := range holes {
		if h.Seq == 2 && h.Class != HoleClassUnassignedGap {
			t.Fatalf("seq 2 class = %s, want unassigned_gap", h.Class)
		}
	}
	for _, seq := range assignedMissingHoles(holes) {
		if seq == 2 {
			t.Fatal("unassigned gap seq 2 must not be assigned-missing")
		}
	}
}

func TestSpoolSlotHealWaitsWhenPeerMissing(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	rec1 := sequencedTestRecord("a", ingesterID, 1)
	rec1.VaultSeq = 1
	if err := store.AppendTentative(rec1); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitAcceptance(rec1); err != nil {
		t.Fatal(err)
	}
	orch.SetSpoolSlotFetcher(&stubSpoolSlotFetcher{})
	orch.mu.Lock()
	orch.vaults[vaultID].seqFanOutTargets = []system.ReplicationTarget{{NodeID: "peer-a"}}
	orch.mu.Unlock()
	store.setMaterializationWatermark(2)
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 2, PrevBoundSeq: 0}
	orch.scheduleSpoolSlotHeal(vaultID, fence, []uint64{2})
	time.Sleep(50 * time.Millisecond)
	if got := orch.convergenceWatermark(vaultID); got != 0 {
		t.Fatalf("C_r = %d, want 0 when peer has no slot", got)
	}
}
