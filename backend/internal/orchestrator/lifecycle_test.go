package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// slowAckReplicator delays AppendRecords so the ack goroutine is still
// running when Stop() is called. Implements orchestrator.TierReplicator.
type slowAckReplicator struct {
	calls atomic.Int32
	delay time.Duration
}

func (m *slowAckReplicator) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	time.Sleep(m.delay)
	m.calls.Add(1)
	return nil
}
func (m *slowAckReplicator) SealTier(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *slowAckReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (m *slowAckReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

// TestStopWaitsForAckGoroutines verifies that Stop() blocks until all
// in-flight ack-gated replication goroutines have completed.
func TestStopWaitsForAckGoroutines(t *testing.T) {
	t.Parallel()

	// Slow replicator: AppendRecords takes 200ms.
	replicator := &slowAckReplicator{delay: 200 * time.Millisecond}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetTierReplicator(replicator)

	// Create a vault with a follower target so ack-gated records trigger replication.
	tierID := glid.New()
	vaultID := glid.New()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(cm, im, nil)
	tier := &TierInstance{
		TierID:          tierID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           qe,
		FollowerTargets: []system.ReplicationTarget{{NodeID: "node-2"}},
	}
	vault := NewVault(vaultID, tier)
	vault.Name = "ack-test"
	orch.RegisterVault(vault)

	// Set up a catch-all filter targeting this vault.
	orch.SetFilterSet(NewFilterSet([]*CompiledFilter{
		{VaultID: vaultID, Kind: FilterCatchAll, Expr: "*"},
	}))

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send an ack-gated record through the pipeline.
	ackCh := make(chan error, 1)
	orch.ingestCh <- IngestMessage{
		Raw:      []byte("test-ack-record"),
		IngestTS: time.Now(),
		Ack:      ackCh,
	}

	// Give the pipeline time to process the record and launch the ack goroutine.
	time.Sleep(50 * time.Millisecond)

	// Stop should block until the ack goroutine finishes (200ms delay).
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// After Stop returns, the ack goroutine must have completed.
	if replicator.calls.Load() != 1 {
		t.Errorf("expected 1 AppendRecords call after Stop, got %d", replicator.calls.Load())
	}

	// The ack channel must have been written to.
	select {
	case ackErr := <-ackCh:
		if ackErr != nil {
			t.Errorf("expected nil ack, got: %v", ackErr)
		}
	default:
		t.Error("ack channel was not written to — Stop() returned before ack goroutine finished")
	}
}

// TestStopWaitsForAuxGoroutines verifies that Stop() blocks until the
// watchdog goroutine has exited.
func TestStopWaitsForAuxGoroutines(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop must not hang — the auxWg tracks the watchdog goroutine.
	// A test timeout catches the failure if it blocks.
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
