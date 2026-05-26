package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestSequenceMaterializerFenceRange(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 50)

	now := time.Unix(0, 10).UTC()
	fence := vaultctlfsm.FenceRecord{
		ID:             1,
		UpperBoundSeq:  50,
		PrevBoundSeq:   0,
		CreatedAtNanos: now.UnixNano(),
	}
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov == nil || cov.RecordCount != 50 {
		t.Fatalf("coverage = %+v, want 50 records", cov)
	}
	if got := orch.materializationWatermark(vaultID); got != 50 {
		t.Fatalf("M_r = %d, want 50", got)
	}
	metas, err := orch.ListLocalChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 || !metas[0].Sealed || metas[0].RecordCount != 50 {
		t.Fatalf("chunk metas = %+v", metas)
	}
}

func TestSequenceMaterializerDeterministicReplay(t *testing.T) {
	t.Parallel()
	run := func() (int, uint64) {
		orch, vaultID := newSequencedFenceTestOrch(t, 25)
		fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 25, PrevBoundSeq: 0}
		if _, err := orch.materializeFence(vaultID, fence); err != nil {
			t.Fatal(err)
		}
		metas, err := orch.ListLocalChunkMetas(vaultID)
		if err != nil {
			t.Fatal(err)
		}
		return int(metas[0].RecordCount), orch.materializationWatermark(vaultID)
	}
	aCount, aMr := run()
	bCount, bMr := run()
	if aCount != bCount || aMr != bMr {
		t.Fatalf("replay diverged: (%d,%d) vs (%d,%d)", aCount, aMr, bCount, bMr)
	}
}

func TestSequenceMaterializerIdempotentAtWatermark(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 10)
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	if _, err := orch.materializeFence(vaultID, fence); err != nil {
		t.Fatal(err)
	}
	metasBefore, _ := orch.ListLocalChunkMetas(vaultID)
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov != nil {
		t.Fatalf("second materialize should no-op, got %+v", cov)
	}
	metasAfter, _ := orch.ListLocalChunkMetas(vaultID)
	if len(metasBefore) != len(metasAfter) {
		t.Fatalf("chunk count changed: before=%d after=%d", len(metasBefore), len(metasAfter))
	}
}

func TestFSMOnPublishFenceCallback(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	now := time.Now().UTC()
	var got vaultctlfsm.FenceRecord
	fsm.SetOnPublishFence(func(rec vaultctlfsm.FenceRecord) {
		got = rec
	})
	result := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishFence(42, now)})
	rec, ok := result.(*vaultctlfsm.FenceRecord)
	if !ok || rec == nil {
		t.Fatalf("apply result = %T %v", result, result)
	}
	if got.UpperBoundSeq != 42 {
		t.Fatalf("callback fence = %+v", got)
	}
}
