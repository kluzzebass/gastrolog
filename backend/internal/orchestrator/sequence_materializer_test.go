package orchestrator

import (
	"errors"
	"testing"
	"time"

	"gastrolog/internal/glid"
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

func TestSequenceMaterializerSkipsBurnedTailGaps(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	burnSeqAllocatorTail(t, orch, vaultID, 10, 7) // unassigned gap seq 8-10

	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	for _, seq := range []uint64{1, 2, 3, 4, 5, 6, 7} {
		rec := sequencedTestRecord("x", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov == nil || cov.RecordCount != 7 {
		t.Fatalf("coverage = %+v, want 7 records", cov)
	}
	if len(cov.MissingSeqs) != 0 {
		t.Fatalf("MissingSeqs = %v, want none (gaps are unassigned)", cov.MissingSeqs)
	}
	if got := orch.materializationWatermark(vaultID); got != 10 {
		t.Fatalf("M_r = %d, want 10", got)
	}
	if got := orch.convergenceWatermark(vaultID); got != 10 {
		t.Fatalf("C_r = %d, want 10 after materialize across burned tail", got)
	}
	if !orch.IsFenceConvergeSealed(vaultID, fence) {
		t.Fatal("expected converge-sealed after materialize with burned-tail gaps")
	}
	metas, err := orch.ListLocalChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 || metas[0].RecordCount != 7 {
		t.Fatalf("chunk metas = %+v", metas)
	}
}

func TestSequenceMaterializerFailsAssignedMissingNotGap(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	burnSeqAllocatorTail(t, orch, vaultID, 10, 7)

	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	for _, seq := range []uint64{1, 2, 4, 5, 6, 7} { // assigned-missing at seq 3
		rec := sequencedTestRecord("x", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	cov, err := orch.materializeFence(vaultID, fence)
	if err == nil {
		t.Fatal("expected materialize error for assigned-missing seq 3")
	}
	if !errors.Is(err, ErrMaterializeMissingSeq) {
		t.Fatalf("err = %v, want ErrMaterializeMissingSeq", err)
	}
	if cov == nil || len(cov.MissingSeqs) != 1 || cov.MissingSeqs[0] != 3 {
		t.Fatalf("MissingSeqs = %v, want [3]", cov)
	}
}

func TestSequenceMaterializerOnlyBurnedTailGapAdvancesWatermarks(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	reserveSeqAllocatorRange(t, orch, vaultID, 10)
	if err := orch.bumpVaultSeqAllocatorEpoch(vaultID); err != nil {
		t.Fatal(err)
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov == nil || cov.RecordCount != 0 {
		t.Fatalf("coverage = %+v, want zero assigned records", cov)
	}
	if got := orch.materializationWatermark(vaultID); got != 10 {
		t.Fatalf("M_r = %d, want 10", got)
	}
	if got := orch.convergenceWatermark(vaultID); got != 10 {
		t.Fatalf("C_r = %d, want 10 for fence spanning only unassigned gaps", got)
	}
}

func TestSequenceMaterializerBurnedTailViaSeqAssignBurn(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	grant, err := orch.reserveVaultSeqRange(vaultID, vaultctlfsm.InitialSeqEpoch, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Start != 1 || grant.End != 10 {
		t.Fatalf("grant = %+v, want [1,10]", grant)
	}

	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	for _, seq := range []uint64{1, 2, 3, 4, 5, 6, 7} {
		rec := sequencedTestRecord("assign-burn", ingesterID, uint32(seq))
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := orch.burnVaultSeqLeaseTail(vaultID, grant.Epoch, 7); err != nil {
		t.Fatal(err)
	}
	sub, err := orch.vaultCtlSubFSM(vaultID)
	if err != nil || sub == nil {
		t.Fatal("vault ctl sub FSM required")
	}
	alloc := sub.SeqAllocatorState()
	if len(alloc.BurnedTails) != 1 || alloc.BurnedTails[0].Start != 8 || alloc.BurnedTails[0].End != 10 {
		t.Fatalf("BurnedTails = %+v, want [{8 10 ...}]", alloc.BurnedTails)
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov == nil || cov.RecordCount != 7 {
		t.Fatalf("coverage = %+v, want 7 records", cov)
	}
	if got := orch.convergenceWatermark(vaultID); got != 10 {
		t.Fatalf("C_r = %d, want 10 after seq-assign burned tail", got)
	}
}

func TestSequenceMaterializerDedupEventIDByLowestSeq(t *testing.T) {
	t.Parallel()
	orch, vaultID := newSequencedFenceTestOrch(t, 0)
	store := orch.vaultSpoolStore(vaultID)
	if err := store.EnsureSwathWindow(1, 512); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	eventID := sequencedTestRecord("dup", ingesterID, 1).EventID
	for _, seq := range []uint64{1, 2} {
		rec := sequencedTestRecord("dup", ingesterID, uint32(seq))
		rec.EventID = eventID
		rec.VaultSeq = seq
		if err := store.AppendTentative(rec); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitAcceptance(rec); err != nil {
			t.Fatal(err)
		}
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 2, PrevBoundSeq: 0}
	cov, err := orch.materializeFence(vaultID, fence)
	if err != nil {
		t.Fatal(err)
	}
	if cov == nil || cov.RecordCount != 1 {
		t.Fatalf("coverage = %+v, want one canonical row", cov)
	}
	metas, err := orch.ListLocalChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 || metas[0].RecordCount != 1 {
		t.Fatalf("chunk metas = %+v, want single deduped record", metas)
	}
}

func burnSeqAllocatorTail(t *testing.T, orch *Orchestrator, vaultID glid.GLID, reserveEnd, burnThrough uint64) {
	t.Helper()
	reserveSeqAllocatorRange(t, orch, vaultID, reserveEnd)
	fsm, err := orch.vaultCtlSubFSM(vaultID)
	if err != nil || fsm == nil {
		t.Fatal("vault ctl sub FSM required for burned tail test")
	}
	const holder = "test-holder"
	const epoch = vaultctlfsm.InitialSeqEpoch
	burn, err := vaultctlfsm.MarshalBurnSeqLeaseTail(holder, epoch, burnThrough)
	if err != nil {
		t.Fatal(err)
	}
	if result := fsm.Apply(&hraft.Log{Data: burn}); result != nil {
		if applyErr, ok := result.(error); ok && applyErr != nil {
			t.Fatalf("burn tail: %v", applyErr)
		}
	}
}

func reserveSeqAllocatorRange(t *testing.T, orch *Orchestrator, vaultID glid.GLID, count uint64) {
	t.Helper()
	fsm, err := orch.vaultCtlSubFSM(vaultID)
	if err != nil || fsm == nil {
		t.Fatal("vault ctl sub FSM required for allocator test")
	}
	const holder = "test-holder"
	const epoch = vaultctlfsm.InitialSeqEpoch
	reserve, err := vaultctlfsm.MarshalReserveSeqRange(holder, epoch, count)
	if err != nil {
		t.Fatal(err)
	}
	if result := fsm.Apply(&hraft.Log{Data: reserve}); result != nil {
		if applyErr, ok := result.(error); ok && applyErr != nil {
			t.Fatalf("reserve: %v", applyErr)
		}
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
