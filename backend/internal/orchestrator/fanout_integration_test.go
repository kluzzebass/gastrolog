// End-to-end integration tests for the fan-out write path
// (gastrolog-n6pah).
//
// These tests exercise the full dispatch chain:
//
//     Orchestrator.Append  →  appendRecord (peeks WriteModel via FSM)
//                          →  buildFanOutTask (snapshot + W clamp)
//                          →  runFanOut → fanOutAppend → AppendRecords
//
// against a mock chunkReplicator. They run on a single-node
// orchestrator with a custom VaultInstance whose ChunkPlacement
// callback returns a synthetic FanOut placement — sufficient to
// validate that the dispatch decisions land correctly without
// standing up the full multi-node setupMultiNode harness (which
// would require harness extensions for cross-node vault-ctl Raft
// groups and FanOut chunk creation through the placement layer).
//
// Coverage scenarios match the gastrolog-n6pah description:
//
//   - Happy-path FanOut writes: snapshot includes self + 2 peers,
//     W=3 → all peers see the record, self auto-acks via the local
//     chunk manager.
//   - Concurrent placement edit + writes: a peer is removed from
//     live Receiving mid-write. The isStillReceiving classifier
//     de-escalates the removed peer's failure, the write still
//     succeeds against the (now-shrunken) effective W.
//   - Vault without placement: no fanOutTask is built; the chunk
//     stays local-only (memory/jsonl-mode behavior) and
//     chunkReplicator.AppendRecords is NOT called.

package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/query"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// fanOutVaultBuilder constructs a Vault + VaultInstance whose
// ChunkPlacement callback returns the supplied placement. Used to
// inject a synthetic FanOut state into a single-node orchestrator
// without standing up a real Raft group.
type fanOutVaultBuilder struct {
	vaultID   glid.GLID
	placement *vaultctlfsm.ChunkPlacement
}

func (b *fanOutVaultBuilder) build(t *testing.T) *Vault {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatalf("chunkmem.NewFactory: %v", err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatalf("indexmem.NewFactory: %v", err)
	}
	inst := &VaultInstance{
		VaultID: b.vaultID,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
		ChunkPlacement: func(_ chunk.ChunkID) *vaultctlfsm.ChunkPlacement {
			if b.placement == nil {
				return nil
			}
			cp := *b.placement
			return &cp
		},
	}
	return &Vault{
		ID:       b.vaultID,
		Name:     "fanout-test-vault",
		Enabled:  true,
		Instance: inst,
	}
}

func TestFanOutIntegrationHappyPath(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	vaultID := glid.New()
	placement := &vaultctlfsm.ChunkPlacement{
			Receiving:  []string{orch.localNodeID, "node-b", "node-c"},
	}
	vault := (&fanOutVaultBuilder{vaultID: vaultID, placement: placement}).build(t)
	orch.RegisterVault(vault)

	rec := testFanOutRecord(t)
	if _, _, err := orch.Append(vaultID, rec); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Append fires fan-out fire-and-forget; give peers time to land.
	if !waitForCalls(rep, []string{"node-b", "node-c"}, 1) {
		t.Errorf("peers didn't see expected calls: b=%d c=%d",
			rep.callsFor("node-b"), rep.callsFor("node-c"))
	}
	// Self should NOT have been called via chunkReplicator — local
	// cm.Append handles that path.
	if rep.callsFor(orch.localNodeID) != 0 {
		t.Errorf("self should not hit chunkReplicator; got %d calls", rep.callsFor(orch.localNodeID))
	}
}

func TestFanOutIntegrationDeescalatesRemovedReceiver(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	// Mock: node-b fails (simulating peer drained mid-write); node-c
	// succeeds. The classifier reads from the FSM — which our
	// synthetic placement holds. We'll mutate the placement after
	// the snapshot is taken (the orchestrator reads it again at
	// failure-classification time).
	var classifierShouldExcludeB atomic.Bool
	rep.appendStub = func(nodeID string) error {
		if nodeID == "node-b" {
			classifierShouldExcludeB.Store(true)
			return errors.New("drained")
		}
		return nil
	}

	vaultID := glid.New()
	placement := &vaultctlfsm.ChunkPlacement{
			Receiving:  []string{orch.localNodeID, "node-b", "node-c"},
	}
	vault := &Vault{
		ID:      vaultID,
		Name:    "fanout-de-escalation",
		Enabled: true,
		Instance: &VaultInstance{
			VaultID: vaultID,
			Type:    "memory",
		},
	}
	cm, _ := chunkmem.NewFactory()(nil, nil)
	im, _ := indexmem.NewFactory()(nil, cm, nil)
	vault.Instance.Chunks = cm
	vault.Instance.Indexes = im
	vault.Instance.Query = query.New(cm, im, nil)
	// Live-Receiving lookup: drops node-b once classifier flips.
	vault.Instance.ChunkPlacement = func(_ chunk.ChunkID) *vaultctlfsm.ChunkPlacement {
		p := &vaultctlfsm.ChunkPlacement{
					Receiving:  []string{orch.localNodeID, "node-c"},
		}
		if !classifierShouldExcludeB.Load() {
			p.Receiving = placement.Receiving
		}
		return p
	}
	orch.RegisterVault(vault)

	rec := testFanOutRecord(t)
	rec.WaitForReplica = true
	ack := make(chan error, 1)

	// Build a pendingAcks with the fan-out task directly (ack-gated
	// path goes through ackAfterReplication). buildFanOutTask
	// requires o.mu.RLock — acquire it briefly to call it.
	orch.mu.RLock()
	task := orch.buildFanOutTask(vaultID, chunk.ChunkID{1}, placement, rec)
	orch.mu.RUnlock()
	if task == nil {
		t.Fatal("buildFanOutTask returned nil")
	}
	pa := (*pendingAcks)(nil).addFanOut(*task)
	orch.ackAfterReplication(ack, pa, rec)

	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("ack-gated fan-out: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack-gated fan-out timed out")
	}
}

func TestFanOutIntegrationNoPlacementDoesNotFanOut(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	vaultID := glid.New()
	// No placement — ChunkPlacement returns nil (memory/jsonl-mode vault).
	vault := (&fanOutVaultBuilder{vaultID: vaultID, placement: nil}).build(t)
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testFanOutRecord(t)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Mock replicator should see ZERO calls — fan-out path never
	// fires because ChunkPlacement returned nil.
	time.Sleep(50 * time.Millisecond) // brief window for any stray goroutine
	for _, n := range []string{"node-b", "node-c", orch.localNodeID} {
		if rep.callsFor(n) != 0 {
			t.Errorf("vault without placement should not hit chunkReplicator; got %s=%d calls", n, rep.callsFor(n))
		}
	}
}

// waitForCalls polls rep until every node in want has been called at
// least minCount times, or 2 seconds elapse. Returns true on success.
func waitForCalls(rep *fakeReplicator, want []string, minCount int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for _, n := range want {
			if rep.callsFor(n) < minCount {
				allReady = false
				break
			}
		}
		if allReady {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// silence unused-import in case lifecycle is unused under refactor.
var _ = lifecycle.Phase{}

// silence unused-context in case the ackAfterReplication signature
// drifts. The integration path here uses context.Background()
// internally via runFanOut.
var _ = context.Background
