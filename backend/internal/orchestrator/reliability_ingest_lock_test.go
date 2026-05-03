package orchestrator

import (
	"io"
	"context"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// blockingReplicator blocks AppendRecords until released. Models a
// SIGSTOPed or frozen follower: the TCP stream stays open, the caller
// waits for an ack that never arrives. Used to reproduce gastrolog-5oofa:
// if ingest() held o.mu.RLock across fireAndForgetRemote (the bug),
// concurrent orchestrator operations would deadlock.
type blockingReplicator struct {
	release     chan struct{}
	entered     chan struct{} // closed on first AppendRecords entry
	enteredOnce sync.Once
	unblockOnce sync.Once
}

func newBlockingReplicator() *blockingReplicator {
	return &blockingReplicator{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

func (m *blockingReplicator) Unblock() {
	m.unblockOnce.Do(func() { close(m.release) })
}

func (m *blockingReplicator) AppendRecords(ctx context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	m.enteredOnce.Do(func() { close(m.entered) })
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *blockingReplicator) SealTier(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *blockingReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (m *blockingReplicator) ImportBlob(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ int64, _ io.Reader) ([32]byte, error) {
	return [32]byte{}, nil
}
func (m *blockingReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *blockingReplicator) RequestReplicaCatchup(_ context.Context, _ string, _, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

// TestReliability_Ingest_ReleasesLockBeforeReplication is the regression
// test for gastrolog-5oofa. Reproduces the exact deadlock: an ingest
// that triggers cross-node replication must not hold the orchestrator
// lock while waiting for the replication RPC to complete. If it did,
// a paused peer would stall the entire orchestrator — every other RPC
// that needs o.mu (search, retention, reconfig, stats collection)
// would queue behind the stuck ingest.
//
// The test sets up a vault with a cross-node follower target, wires a
// blocking TierReplicator that never acks, ingests a record via the
// single-threaded writeLoop path, and then races a concurrent
// UnregisterVault (write lock). With the fix, UnregisterVault succeeds
// promptly. Without it, UnregisterVault blocks until the ingest call
// eventually times out — orders of magnitude longer.
//
// A 500ms budget is generous: the goal isn't to catch slow code, it's
// to catch a deadlock. Pre-fix, the test would hang for the full test
// timeout; post-fix, Unregister completes in single-digit milliseconds.
func TestReliability_Ingest_ReleasesLockBeforeReplication(t *testing.T) {
	t.Parallel()

	replicator := newBlockingReplicator()
	defer replicator.Unblock() // always release at test end

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetTierReplicator(replicator)

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
		FollowerTargets: []system.ReplicationTarget{{NodeID: "node-2-paused"}},
	}
	vault := NewVault(vaultID, tier)
	vault.Name = "5oofa-regression"
	orch.RegisterVault(vault)

	// Set up a catch-all filter so Ingest routes to this vault.
	orch.SetFilterSet(NewFilterSet([]*CompiledFilter{
		{VaultID: vaultID, Kind: FilterCatchAll, Expr: "*"},
	}))

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Ingest a record in the background. With a blocking replicator, this
	// will get stuck in fireAndForgetRemote → wg.Wait → replicator.
	ingestDone := make(chan error, 1)
	go func() {
		ingestDone <- orch.Ingest(chunk.Record{
			IngestTS: time.Now(),
			Raw:      []byte("regression-record"),
		})
	}()

	// Wait until the replicator entered AppendRecords — i.e., the ingest
	// path is now inside fireAndForgetRemote. This is the critical window:
	// if the orchestrator mutex were held, every other RPC would queue.
	select {
	case <-replicator.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("replicator never entered AppendRecords (ingest path broken)")
	}

	// Fire an operation that takes the write lock. If ingest is holding
	// o.mu.RLock, this will block until the replicator is unblocked.
	// With the fix, the lock was released before fireAndForgetRemote and
	// UnregisterVault should return quickly.
	done := make(chan error, 1)
	go func() {
		done <- orch.UnregisterVault(vaultID)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("UnregisterVault returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("UnregisterVault blocked for 500ms — orchestrator mutex held across replication (gastrolog-5oofa regressed)")
	}

	// Cleanup: let the in-flight ingest unblock and return.
	replicator.Unblock()
	select {
	case <-ingestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest did not complete after replicator unblocked")
	}
}
