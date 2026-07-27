package orchestrator_test

// Orchestrator-level acceptance coverage for the retention "transfer"
// disposition (gastrolog-2l918): a fired retention event re-homes a sealed
// chunk to a target vault UNCHANGED via the reused RepatriateChunk
// announce-import + GLCB replica catch-up + holder-receipt machinery (see
// backend/internal/orchestrator/retention_transfer.go). These tests use
// the same orchRelHarness real file-vault + real vault-ctl Raft + real
// pipeline infrastructure the GLCB replica catch-up and cluster-ingest
// acceptance tests use (reliability_glcb_catchup_test.go,
// reliability_pipeline_test.go) — file vaults only, per spec decision #4,
// so this family cannot run against the memory-vault-only harness in
// server package tests (33ul6h finding).
//
// These tests wait on REAL retention sweep ticks — nothing here pokes the
// sweep by hand, and the assertions are written in sweeps rather than
// seconds so they hold at any cadence. The package's test binary installs a
// compressed cron profile (testprofile_test.go, gastrolog-4yzpcj), so a
// sweep costs a second instead of a minute; run the same tests without that
// profile and they still pass, only slower. testing.Short() skips them; they
// run under the mandatory full gate (go test ./... without -short).

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// retentionSweepJob is the scheduler job name the retention sweep registers
// under (orchestrator.retentionJobName). Tests here reason in SWEEPS, not
// seconds: the retention sweep is what makes a transfer happen, so its
// observed run count — read back through Scheduler.ListJobs — is the honest
// unit of retention time (the same unit retentionDeferralAlarmAfter counts
// in), and it stays correct at any cron cadence.
const retentionSweepJob = "retention"

// putTransferRetentionPolicy creates a MaxAge=1ms retention policy — the
// fastest deterministic trigger available. MaxChunks=0 looks like an
// equally fast "keep nothing" trigger but ToRetentionPolicy() rejects a
// non-positive MaxChunks outright ("invalid maxChunks: must be positive"),
// which would silently drop the vault out of the sweep entirely (no rules
// resolve, retentionTargetForInstance skips it) rather than making
// anything eligible. By the time a policy set here is picked up by the
// running orchestrator's next sweep, real wall-clock time (seconds, at
// minimum) has always elapsed since the chunk sealed, so 1ms is
// effectively "always past due" without racing any specific duration.
func putTransferRetentionPolicy(t *testing.T, h *orchRelHarness, name string) glid.GLID {
	t.Helper()
	policyID := glid.New()
	maxAge := "1ms"
	if err := h.cfgStore.PutRetentionPolicy(context.Background(), system.RetentionPolicyConfig{
		ID: policyID, Name: name, MaxAge: &maxAge,
	}); err != nil {
		t.Fatalf("PutRetentionPolicy %s: %v", name, err)
	}
	return policyID
}

// configureTransferDisposition mutates a vault's stored config to
// disposition=transfer with the given target and retention policy,
// preserving every other field already set (rotation policy, storage
// class) so the running pipeline keeps working. The harness's pipeline
// mode wires SystemLoader directly to h.cfgStore, so the running
// orchestrator picks this up on its next retentionSweepAll tick — no
// restart or explicit reload needed.
func configureTransferDisposition(t *testing.T, h *orchRelHarness, v vaultSpec, targetID, policyID glid.GLID) {
	t.Helper()
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.RetentionDisposition = system.RetentionDispositionTransfer
	cfg.RetentionTransferTargetVaultID = &targetID
	cfg.RetentionRules = []system.RetentionRule{{RetentionPolicyID: policyID}}
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (configure transfer): %v", v.label, err)
	}
}

// setReplicationFactor sets a vault's desired replication factor in the
// shared store.
func setReplicationFactor(t *testing.T, h *orchRelHarness, v vaultSpec, rf uint32) {
	t.Helper()
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.ReplicationFactor = rf
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (set RF): %v", v.label, err)
	}
}

// enableVault flips a vault's Enabled flag on in the shared store.
// orchRelHarness's seedSharedConfig creates every vault with Enabled left
// at its zero value (false) — that default only gates ingest admission
// elsewhere in production, which is why plain ingest/seal still work
// against harness vaults untouched — but retention's resolveTransferTarget
// treats a disabled TARGET as a real defer condition (spec decision #5),
// so any test exercising a successful transfer must explicitly enable
// both the source and the target here.
func enableVault(t *testing.T, h *orchRelHarness, v vaultSpec) {
	t.Helper()
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.Enabled = true
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (enable): %v", v.label, err)
	}
}

// disableVault flips a vault's Enabled flag off in the shared store.
func disableVault(t *testing.T, h *orchRelHarness, v vaultSpec) {
	t.Helper()
	ctx := context.Background()
	cfg, err := h.cfgStore.GetVault(ctx, v.id)
	if err != nil || cfg == nil {
		t.Fatalf("GetVault %s: %v", v.label, err)
	}
	cfg.Enabled = false
	if err := h.cfgStore.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault %s (disable): %v", v.label, err)
	}
}

// TestOrchPipeline_TransferDispositionSingleNode is the full happy-path
// acceptance: source vault B transfers its sealed chunk to target vault C
// on one node. Verifies the GLCB lands byte-identical in the destination
// chunk root, the destination FSM registers it with a FRESH SealedAt
// anchor (spec decision #6) and TransferSourceVaultID pointing back at the
// source, and the source's local copy is expired ONLY after that — this
// harness cannot directly observe "not before", but it CAN observe the end
// state: source gone, destination present and byte-identical, which is
// the only externally-checkable consequence of the 5034va ordering.
func TestOrchPipeline_TransferDispositionSingleNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node-harness pipeline acceptance test (single node here, but real cron + real Raft)")
	}

	h := newOrchRelHarness(t, 1,
		withExtraVault([]int{0}), // vaults[1] = "B", source
		withExtraVault([]int{0}), // vaults[2] = "C", destination
		withMatchAllRoute(1),     // route ingest to source vault B only
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	source := h.vaults[1]
	dest := h.vaults[2]
	node := h.nodeIDs[0]
	enableVault(t, h, source)
	enableVault(t, h, dest)

	h.submitIngestRecords(node, pipelineChunkMaxRecords, "transfer-happy")
	entries := h.waitSealedRecords(source, node, pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk on source, got %d", len(entries))
	}
	e := entries[0]
	h.waitGLCBsOnHomes(source, []int{0}, entries)

	wantBytes, err := os.ReadFile(h.pipelineGLCBPath(node, source, e.ID))
	if err != nil {
		t.Fatalf("read source GLCB before transfer: %v", err)
	}
	sourceSealedAt := e.SealedAt

	policyID := putTransferRetentionPolicy(t, h, "transfer-happy-policy")
	configureTransferDisposition(t, h, source, dest.id, policyID)

	// The chunk lands on the destination — same ID, same record count,
	// byte-identical GLCB, a FRESH SealedAt anchor, and
	// TransferSourceVaultID naming the source.
	var destEntry chunk.ChunkID
	h.waitProgress("chunk transferred to destination vault", 50*time.Millisecond, func() (string, bool) {
		entries := h.sealedPipelineChunks(dest, node)
		return fmt.Sprintf("dest_sealed_chunks=%d", len(entries)), len(entries) == 1
	}, nil)
	destEntries := h.sealedPipelineChunks(dest, node)
	if len(destEntries) != 1 {
		t.Fatalf("destination has %d sealed chunks, want 1", len(destEntries))
	}
	de := destEntries[0]
	destEntry = de.ID
	if destEntry != e.ID {
		t.Fatalf("destination chunk ID = %s, want the transferred chunk's own ID %s (unchanged identity)", destEntry, e.ID)
	}
	if de.RecordCount != e.RecordCount {
		t.Fatalf("destination record count = %d, want %d (unchanged)", de.RecordCount, e.RecordCount)
	}
	if de.TransferSourceVaultID != source.id {
		t.Fatalf("destination TransferSourceVaultID = %s, want source vault %s", de.TransferSourceVaultID, source.id)
	}
	if !de.SealedAt.After(sourceSealedAt) {
		t.Fatalf("destination SealedAt (%s) must be a FRESH anchor after the source's original seal time (%s)",
			de.SealedAt, sourceSealedAt)
	}

	h.waitGLCBsOnHomes(dest, []int{0}, destEntries)
	gotBytes, err := os.ReadFile(h.pipelineGLCBPath(node, dest, destEntry))
	if err != nil {
		t.Fatalf("read destination GLCB: %v", err)
	}
	if string(gotBytes) != string(wantBytes) {
		t.Fatalf("destination GLCB bytes differ from source's original — transfer must move the chunk unchanged")
	}

	// The source's local copy is gone (retained until receipts, expired
	// only after) — this is the observable consequence of the 5034va
	// ordering: by the time the destination is confirmed, the source has
	// released its copy.
	h.waitProgress("source chunk expired after destination receipts", 50*time.Millisecond, func() (string, bool) {
		entries := h.sealedPipelineChunks(source, node)
		return fmt.Sprintf("source_sealed_chunks=%d", len(entries)), len(entries) == 0
	}, nil)
	if _, err := os.Stat(h.pipelineGLCBPath(node, source, e.ID)); !os.IsNotExist(err) {
		t.Fatalf("source GLCB file still on disk after transfer completed (err=%v)", err)
	}
}

// TestOrchPipeline_TransferDispositionDefersWhenTargetDisabled pins spec
// decision #5 (destination state: defer, never drop): a transfer target
// that's disabled at sweep time must retain the source chunk rather than
// destroying it. One real retention sweep is enough to observe "retained,
// not destroyed" — the 3-consecutive-sweep alarm THRESHOLD itself is
// already covered fast and deterministically by the unit-level deferral
// streak tests (retention_deferral_test.go), so this acceptance test does
// not also wait through 3 real one-minute cron cycles for it.
func TestOrchPipeline_TransferDispositionDefersWhenTargetDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("pipeline acceptance test (real cron + real Raft)")
	}

	h := newOrchRelHarness(t, 1,
		withExtraVault([]int{0}), // vaults[1] = "B", source
		withExtraVault([]int{0}), // vaults[2] = "C", destination (will be disabled)
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	source := h.vaults[1]
	dest := h.vaults[2]
	node := h.nodeIDs[0]
	enableVault(t, h, source)

	h.submitIngestRecords(node, pipelineChunkMaxRecords, "transfer-deferred")
	entries := h.waitSealedRecords(source, node, pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk on source, got %d", len(entries))
	}
	e := entries[0]
	h.waitGLCBsOnHomes(source, []int{0}, entries)

	disableVault(t, h, dest)
	policyID := putTransferRetentionPolicy(t, h, "transfer-deferred-policy")
	configureTransferDisposition(t, h, source, dest.id, policyID)

	// Proving the negative (retention never destroys the chunk) only means
	// something once the sweep that COULD have destroyed it has actually
	// run. The observation window is therefore counted in retention sweeps
	// the scheduler reports having executed — not in seconds, which merely
	// approximated "a tick probably happened in there". Three of them, so
	// the window also spans the deferral streak that raises the alarm
	// (retentionDeferralAlarmAfter): the invariant must survive the streak,
	// not just the first deferral. The invariant is re-checked on every
	// poll, so a violation at ANY point in the window fails immediately.
	h.holdAcrossSweeps("transfer target disabled: source retained, destination untouched",
		node, retentionSweepJob, 3, func() (string, bool) {
			entriesNow := h.sealedPipelineChunks(source, node)
			if len(entriesNow) != 1 || entriesNow[0].ID != e.ID {
				return fmt.Sprintf("source chunk must be RETAINED (deferred) while the transfer target is disabled; sealed_chunks=%v", entriesNow), false
			}
			if destEntries := h.sealedPipelineChunks(dest, node); len(destEntries) != 0 {
				return fmt.Sprintf("destination must NOT have received anything while disabled; got %d entries", len(destEntries)), false
			}
			return "retained", true
		})
	if _, err := os.Stat(h.pipelineGLCBPath(node, source, e.ID)); err != nil {
		t.Fatalf("source GLCB must still be on disk while deferred: %v", err)
	}
}

// TestOrchPipeline_TransferDispositionMultiNodeDestRF is the spec's
// Testing-section multi-node requirement: destination RF > 1, receipts
// gate holds until every destination home holds the chunk. Source vault B
// is placed ONLY on node-1; destination vault C is placed on nodes 2-4,
// entirely DISJOINT from the source's placement. This specifically
// exercises the cross-vault generalization of the GLCB replica catch-up
// sweep (glcb_catchup.go's TransferSourceVaultID handling): no node ever
// holds both vaults locally, so tryLocalTransferCopy's same-node fast path
// is a no-op everywhere, and the ENTIRE byte transfer to all 3 destination
// homes must go through real cross-node PullChunkGLCB RPCs addressed at
// the source vault. If that generalization were missing or wrong, every
// destination home's pull would try peers of vault C (which never holds
// the bytes either) and this test would time out at the transfer
// executor's receipts-wait stall window.
func TestOrchPipeline_TransferDispositionMultiNodeDestRF(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test (real cron + real Raft + real gRPC)")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0}),       // vaults[1] = "B", source: node-1 ONLY
		withExtraVault([]int{1, 2, 3}), // vaults[2] = "C", destination: nodes 2-4, disjoint from source
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	source := h.vaults[1]
	dest := h.vaults[2]
	sourceNode := h.nodeIDs[0]
	destHomeIdxs := []int{1, 2, 3}
	enableVault(t, h, source)
	enableVault(t, h, dest)

	h.submitIngestRecords(sourceNode, pipelineChunkMaxRecords, "transfer-multinode")
	entries := h.waitSealedRecords(source, sourceNode, pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk on source, got %d", len(entries))
	}
	e := entries[0]
	h.waitGLCBsOnHomes(source, []int{0}, entries)
	wantBytes, err := os.ReadFile(h.pipelineGLCBPath(sourceNode, source, e.ID))
	if err != nil {
		t.Fatalf("read source GLCB before transfer: %v", err)
	}

	setReplicationFactor(t, h, dest, 3)
	policyID := putTransferRetentionPolicy(t, h, "transfer-multinode-policy")
	configureTransferDisposition(t, h, source, dest.id, policyID)

	// Every destination home earns a GLCB file — this is the cross-node
	// pull generalization actually running end to end.
	h.waitProgress("destination vault C: sealed chunk appears", 50*time.Millisecond, func() (string, bool) {
		n := len(h.sealedPipelineChunks(dest, h.nodeIDs[destHomeIdxs[0]]))
		return fmt.Sprintf("dest_sealed=%d", n), n == 1
	}, nil)
	destEntries := h.sealedPipelineChunks(dest, h.nodeIDs[destHomeIdxs[0]])
	if len(destEntries) != 1 || destEntries[0].ID != e.ID {
		t.Fatalf("destination sealed entries = %v, want exactly the transferred chunk %s", destEntries, e.ID)
	}

	h.waitGLCBsOnHomes(dest, destHomeIdxs, destEntries)
	for _, idx := range destHomeIdxs {
		got, err := os.ReadFile(h.pipelineGLCBPath(h.nodeIDs[idx], dest, e.ID))
		if err != nil {
			t.Fatalf("destination home %s: GLCB unreadable: %v", h.nodes[h.nodeIDs[idx]].label, err)
		}
		if string(got) != string(wantBytes) {
			t.Fatalf("destination home %s: GLCB bytes differ from source original", h.nodes[h.nodeIDs[idx]].label)
		}
	}

	// Holder receipts converge to exactly the 3 destination homes — the
	// gate fireTransferEvent's waitForDestHolders actually waited on.
	h.waitChunkHolders(dest, e.ID, destHomeIdxs)

	// The source releases its copy only once every destination home
	// holds the chunk (observable end state of the 5034va ordering).
	h.waitProgress("source chunk expired after multi-home destination receipts", 50*time.Millisecond, func() (string, bool) {
		entries := h.sealedPipelineChunks(source, sourceNode)
		return fmt.Sprintf("source_sealed=%d", len(entries)), len(entries) == 0
	}, nil)
	if _, err := os.Stat(h.pipelineGLCBPath(sourceNode, source, e.ID)); !os.IsNotExist(err) {
		t.Fatalf("source GLCB file still on disk after multi-home transfer completed (err=%v)", err)
	}

	// gastrolog-2l918 review finding 1a: on completion, the destination's
	// manifest entry must have TransferSourceVaultID CLEARED — left set,
	// every future replica-repair pull for this chunk would keep
	// addressing itself at the now-empty source vault.
	finalEntries := h.sealedPipelineChunks(dest, h.nodeIDs[destHomeIdxs[0]])
	if len(finalEntries) != 1 || finalEntries[0].ID != e.ID {
		t.Fatalf("destination sealed entries after completion = %v, want exactly %s", finalEntries, e.ID)
	}
	if !finalEntries[0].TransferSourceVaultID.IsZero() {
		t.Fatalf("destination entry TransferSourceVaultID = %s after completion, want zero (cleared on completion)",
			finalEntries[0].TransferSourceVaultID)
	}

	// gastrolog-2l918 review finding 1b, the defense-in-depth other half:
	// a destination home that loses its copy AFTER the source vault has
	// expired its own copies must still self-heal. The source vault no
	// longer holds this chunk at all (asserted above), so if
	// runGLCBPull's replica-repair pull were still addressed at the
	// (already-empty) source, this would time out forever — recovery can
	// only succeed via the holder-set fallback pulling from ANOTHER
	// destination home.
	victimIdx := destHomeIdxs[0]
	victimPath := h.pipelineGLCBPath(h.nodeIDs[victimIdx], dest, e.ID)
	if err := os.Remove(victimPath); err != nil {
		t.Fatalf("remove victim destination GLCB: %v", err)
	}
	h.waitProgress("victim destination home GLCB recovery after source vault is fully gone", 200*time.Millisecond, func() (string, bool) {
		_, err := os.Stat(victimPath)
		return fmt.Sprintf("victim_glcb_stat=%v", err), err == nil
	}, func() { h.dumpPipelineState(dest) })
	got, err := os.ReadFile(victimPath)
	if err != nil {
		t.Fatalf("read recovered destination GLCB: %v", err)
	}
	if string(got) != string(wantBytes) {
		t.Fatalf("recovered destination GLCB differs from the original transferred bytes")
	}
	h.waitChunkHolders(dest, e.ID, destHomeIdxs)
}
