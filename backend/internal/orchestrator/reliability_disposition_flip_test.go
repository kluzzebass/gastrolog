package orchestrator_test

// End-to-end coverage: a disposition changed on a RUNNING cluster must be
// honoured by the retention sweep, not deferred until the sweep that is
// already in flight finishes.
//
// The unit tests pin that currentDisposition resolves from the config store.
// This pins the thing the operator actually observes: flip the setting, and the
// destination stops receiving. The harness wires SystemLoader straight to
// h.cfgStore, so a PutVault here is picked up by the running orchestrator's own
// sweeps — no restart, no reload, no test driving the sweep by hand.
//
// The dev-cluster incident this reproduces: first-vault's stored disposition was
// "delete" while its records kept arriving in second-vault at ~2600 records/sec,
// because the in-flight sweep still held "transfer"/"route" from before the
// change.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/system"
)

// switchToDeleteDisposition flips a vault to delete on the shared store,
// leaving every other field (including its retention rules) intact — the
// operator action under test.
func switchToDeleteDisposition(t *testing.T, h *orchRelHarness, v vaultSpec) {
	t.Helper()
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.RetentionDisposition = system.RetentionDispositionDelete
	cfg.RetentionTransferTargetVaultID = nil
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (switch to delete): %v", v.label, err)
	}
}

func TestOrchRel_DispositionFlipToDeleteStopsFeedingTheDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 1,
		withExtraVault([]int{0}), // vaults[1] = source
		withExtraVault([]int{0}), // vaults[2] = destination
		withMatchAllRoute(1),     // ingest lands in the source only
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	source := h.vaults[1]
	dest := h.vaults[2]
	node := h.nodeIDs[0]
	enableVault(t, h, source)
	enableVault(t, h, dest)

	// Two chunks: the first proves transfer is live, the second is what must
	// NOT arrive after the flip.
	h.submitIngestRecords(node, pipelineChunkMaxRecords*2, "disposition-flip")
	h.waitSealedRecords(source, node, pipelineChunkMaxRecords*2)

	policyID := putTransferRetentionPolicy(t, h, "flip-policy")
	configureTransferDisposition(t, h, source, dest.id, policyID)

	// Premise: the destination is actually receiving. Without this the rest of
	// the test would pass on a cluster where transfer never worked at all.
	h.waitProgress("destination receiving under transfer disposition", 50*time.Millisecond,
		func() (string, bool) {
			n := len(h.sealedPipelineChunks(dest, node))
			return fmt.Sprintf("dest_sealed_chunks=%d", n), n >= 1
		}, nil)
	arrivedBeforeFlip := len(h.sealedPipelineChunks(dest, node))

	// The operator action: flip to delete, mid-flight, with retention still
	// working through the vault's chunks.
	switchToDeleteDisposition(t, h, source)

	// The destination must stop growing. Held across three OBSERVED retention
	// sweeps rather than a wall-clock window: the negative only means something
	// if the sweeps that could have violated it actually ran. Before the fix,
	// the in-flight sweep kept its captured disposition and the destination
	// kept gaining chunks.
	h.holdAcrossSweeps("destination stops receiving once disposition is delete",
		node, retentionSweepJob, 3, func() (string, bool) {
			n := len(h.sealedPipelineChunks(dest, node))
			return fmt.Sprintf("dest_sealed_chunks=%d (was %d at the flip)", n, arrivedBeforeFlip),
				n <= arrivedBeforeFlip
		})

	// And the source must still be draining — delete disposition means the
	// chunks go away, not that retention stops. A test that only asserted the
	// destination stayed still would also pass if retention had wedged.
	h.waitProgress("source still draining under delete disposition", 50*time.Millisecond,
		func() (string, bool) {
			n := len(h.sealedPipelineChunks(source, node))
			return fmt.Sprintf("source_sealed_chunks=%d", n), n == 0
		}, func() {
			t.Logf("source retained %d sealed chunks; delete disposition must still expire them",
				len(h.sealedPipelineChunks(source, node)))
		})
}

// The reverse edge: flipping delete -> transfer mid-flight must START feeding
// the destination without waiting for a restart. Same staleness bug, opposite
// direction — a captured "delete" would silently keep destroying records the
// operator had just asked to be preserved elsewhere.
func TestOrchRel_DispositionFlipToTransferStartsFeedingTheDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 1,
		withExtraVault([]int{0}),
		withExtraVault([]int{0}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	source := h.vaults[1]
	dest := h.vaults[2]
	node := h.nodeIDs[0]
	enableVault(t, h, source)
	enableVault(t, h, dest)

	h.submitIngestRecords(node, pipelineChunkMaxRecords, "flip-to-transfer")
	h.waitSealedRecords(source, node, pipelineChunkMaxRecords)

	// Start on delete with a live retention rule, so the sweep is running and
	// has a captured disposition before the operator changes anything.
	policyID := putTransferRetentionPolicy(t, h, "flip-to-transfer-policy")
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, source.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault source: %v", err)
	}
	cfg.RetentionDisposition = system.RetentionDispositionDelete
	cfg.RetentionRules = []system.RetentionRule{{RetentionPolicyID: policyID}}
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault source (delete): %v", err)
	}

	// Flip to transfer FIRST, then feed. Ingesting before the flip made this
	// test race its own setup: the delete-disposition sweep destroys chunks as
	// fast as they seal (the policy is MaxAge 1ms), so under full-suite load
	// there was nothing left to transfer by the time the flip landed and the
	// destination never received anything. Feeding after the flip tests the
	// same staleness — a runner still holding "delete" would destroy these
	// chunks instead of transferring them — without depending on which of the
	// two racers wins.
	configureTransferDisposition(t, h, source, dest.id, policyID)
	h.submitIngestRecords(node, pipelineChunkMaxRecords, "flip-to-transfer-2")

	h.waitProgress("destination receives after the flip to transfer", 50*time.Millisecond,
		func() (string, bool) {
			n := len(h.sealedPipelineChunks(dest, node))
			return fmt.Sprintf("dest_sealed_chunks=%d", n), n >= 1
		}, func() {
			t.Log("destination never received a chunk; a captured 'delete' would destroy them instead")
		})
}
