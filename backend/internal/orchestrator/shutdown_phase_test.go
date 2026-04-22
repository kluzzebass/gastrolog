package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"sync/atomic"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/lifecycle"
)

// recordingTierReplicator is a TierReplicator that counts every call. Used
// to assert that the orchestrator's shutdown-aware replication helpers
// actually skip remote work during drain.
type recordingTierReplicator struct {
	appendCalls atomic.Int32
	sealCalls   atomic.Int32
}

func (r *recordingTierReplicator) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	r.appendCalls.Add(1)
	return nil
}

func (r *recordingTierReplicator) SealTier(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	r.sealCalls.Add(1)
	return nil
}

func (r *recordingTierReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}

func (r *recordingTierReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

// TestFireAndForgetRemoteSkipsDuringShutdown is the regression test for the
// orchestrator half of gastrolog-1e5ke. Before the fix, the orchestrator's
// Stop() drain would process buffered records and fan out each one to
// remote followers via fireAndForgetRemote → tierReplicator.AppendRecords.
// Every fanout attempt against a dead or dying peer produced a WARN log
// ("replication: failed to forward record to follower"), flooding the
// shutdown trail with 127 noise lines on a ~six-second drain.
//
// The fix: phase.BeginShutdown flips an atomic bool that fireAndForgetRemote
// checks on every call. Once the phase is shutting down, the method is a
// silent no-op — the records are already durable on the leader's local
// disk, and peers will reconcile via replication-catchup on next startup.
//
// This test exercises the helper directly with a recording fake replicator,
// first asserting the happy path (calls happen before shutdown), then
// asserting zero calls after BeginShutdown. Unit scope — it does not go
// through the full append pipeline because the skip is purely a property
// of the fireAndForgetRemote helper.
func TestFireAndForgetRemoteSkipsDuringShutdown(t *testing.T) {
	t.Parallel()

	phase := lifecycle.New()
	orch := newTestOrch(t, Config{LocalNodeID: "local", Phase: phase})

	replicator := &recordingTierReplicator{}
	orch.SetTierReplicator(replicator)

	targets := []remoteForwardTarget{
		{nodeID: "remote-a", vaultID: glid.New(), tierID: glid.New()},
		{nodeID: "remote-b", vaultID: glid.New(), tierID: glid.New()},
	}
	rec := chunk.Record{Raw: []byte("payload")}

	// Happy path: replicator is called once per target per record.
	orch.fireAndForgetRemote(targets, rec)
	if got := replicator.appendCalls.Load(); got != int32(len(targets)) {
		t.Fatalf("pre-shutdown: appendCalls = %d, want %d", got, len(targets))
	}

	// Flip the phase. This is what Orchestrator.Stop() does at stage 0,
	// before the pipeline drain starts.
	phase.BeginShutdown("test: draining")

	// Another call: should be a silent no-op now — no extra replicator
	// calls, no error, no panic.
	orch.fireAndForgetRemote(targets, rec)
	if got := replicator.appendCalls.Load(); got != int32(len(targets)) {
		t.Errorf("post-shutdown: appendCalls = %d, want %d (no new calls)", got, len(targets))
	}
}

// TestSealRemoteFollowersSkipsDuringShutdown verifies the same drain-aware
// behaviour for the seal-synchronization path. sealRemoteFollowers is
// called when the leader rotates a chunk while replicating to followers;
// during shutdown we skip it because the local chunk is already sealed
// on disk and peers will reseal on their next replication-catchup pass.
func TestSealRemoteFollowersSkipsDuringShutdown(t *testing.T) {
	t.Parallel()

	phase := lifecycle.New()
	orch := newTestOrch(t, Config{LocalNodeID: "local", Phase: phase})

	replicator := &recordingTierReplicator{}
	orch.SetTierReplicator(replicator)

	targets := []remoteForwardTarget{
		{nodeID: "remote-a", vaultID: glid.New(), tierID: glid.New()},
	}
	chunkID := chunk.ChunkID{}

	// Happy path first.
	orch.sealRemoteFollowers(targets, chunkID)
	if got := replicator.sealCalls.Load(); got != 1 {
		t.Fatalf("pre-shutdown: sealCalls = %d, want 1", got)
	}

	phase.BeginShutdown("test: draining")

	orch.sealRemoteFollowers(targets, chunkID)
	if got := replicator.sealCalls.Load(); got != 1 {
		t.Errorf("post-shutdown: sealCalls = %d, want 1 (no new calls)", got)
	}
}

// TestFireAndForgetRemoteNilPhaseDoesNotPanic verifies that orchestrators
// constructed without a shared Phase (the test / single-node harness case)
// still behave correctly. shuttingDown() returns false for a nil phase,
// preserving the pre-gastrolog-1e5ke behaviour where every call reaches
// the replicator.
func TestFireAndForgetRemoteNilPhaseDoesNotPanic(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "local"}) // no Phase
	replicator := &recordingTierReplicator{}
	orch.SetTierReplicator(replicator)

	targets := []remoteForwardTarget{
		{nodeID: "remote-a", vaultID: glid.New(), tierID: glid.New()},
	}
	orch.fireAndForgetRemote(targets, chunk.Record{Raw: []byte("payload")})

	if got := replicator.appendCalls.Load(); got != 1 {
		t.Errorf("appendCalls = %d, want 1 (nil phase should not skip)", got)
	}
}

// TestOrchestratorStopFlipsPhase verifies that Orchestrator.Stop() itself
// flips the phase at stage 0, even when no external caller has. This is
// defence-in-depth for the case where a component (other than the top-level
// shutdown) stops the orchestrator first — the phase should still reach
// "shutting down" state so subsequent replication helpers skip.
func TestOrchestratorStopFlipsPhase(t *testing.T) {
	t.Parallel()

	phase := lifecycle.New()
	orch := newTestOrch(t, Config{LocalNodeID: "local", Phase: phase})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if phase.ShuttingDown() {
		t.Fatal("phase should not be shutting down after Start")
	}
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if !phase.ShuttingDown() {
		t.Error("Stop should flip the phase to shutting down")
	}
	if phase.Label() == "" {
		t.Error("Stop should set a phase label")
	}
}
