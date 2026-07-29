package orchestrator

// Unit coverage for the transfer disposition executor's non-Raft-dependent
// pieces (gastrolog-2l918): the destination-receipts watchdog, the
// announce-import idempotency/corruption gate, target-state re-validation,
// and the disposition-gate dispatch. Full end-to-end (real vault-ctl Raft
// FSM, real GLCB bytes, real receipts) coverage lives in the
// orchestrator_test package's orchRelHarness-backed acceptance tests
// (retention_transfer_test.go) — this file covers what can be tested fast
// and deterministically without standing up Raft.

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ackHolder applies AckChunkHolderCommand to fsm for chunkID/nodeID via the
// real command path — no Raft needed, ApplyCommand runs the FSM apply
// function directly.
func ackHolder(fsm *vaultctlfsm.FSM, chunkID chunk.ChunkID, nodeID string) {
	fsm.ApplyCommand(vaultctlfsm.NewAckChunkHolders([]chunk.ChunkID{chunkID}, nodeID))
}

func seedManifestEntry(fsm *vaultctlfsm.FSM, entry vaultctlfsm.ManifestEntry) {
	fsm.ApplyCommand(vaultctlfsm.NewRepatriateChunk(entry))
}

// ---------- waitForDestHolders ----------

func TestWaitForDestHoldersSucceedsImmediatelyWhenAlreadyMet(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{ID: id, RecordCount: 3})
	ackHolder(fsm, id, "node-A")

	r := &retentionRunner{}
	// A closed/never-read tick channel proves this path never blocks on a
	// tick: if it did, this would deadlock the test.
	tick := make(chan time.Time)
	if !r.waitForDestHolders(fsm, id, 1, tick) {
		t.Fatal("want immediate success when holders already meet need")
	}
}

func TestWaitForDestHoldersSucceedsAfterProgressTicks(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{ID: id, RecordCount: 3})

	r := &retentionRunner{}
	// Buffered generously so every send below is non-blocking regardless of
	// exactly when the receiver goroutine gets scheduled: an unbuffered
	// channel's send/receive rendezvous only synchronizes the handshake
	// itself, not the receiver's SUBSEQUENT holderCount() check, so a
	// lockstep unbuffered exchange can race the waiter into returning
	// early (once cur>=need) and leave a later send with no reader —
	// deadlock. A buffer big enough for every send in this test sidesteps
	// that without any wall-clock sleep.
	tick := make(chan time.Time, 8)
	done := make(chan bool, 1)
	go func() { done <- r.waitForDestHolders(fsm, id, 2, tick) }()

	// First tick: still 0 holders — not yet satisfied.
	tick <- time.Now()
	// A holder arrives between ticks (simulates the destination's own
	// catch-up sweep earning a receipt).
	ackHolder(fsm, id, "node-A")
	tick <- time.Now()
	// Second holder arrives; next tick observes need met.
	ackHolder(fsm, id, "node-B")
	tick <- time.Now()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("want success once holders reach need via injected ticks")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForDestHolders did not return after injected progress ticks")
	}
}

func TestWaitForDestHoldersStallsAfterMaxTicksWithNoProgress(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{ID: id, RecordCount: 3})
	// One holder arrives once, then nothing — never reaches need=2.
	ackHolder(fsm, id, "node-A")

	r := &retentionRunner{}
	tick := make(chan time.Time)
	done := make(chan bool, 1)
	go func() { done <- r.waitForDestHolders(fsm, id, 2, tick) }()

	// Exactly transferReceiptsMaxStallTicks ticks with no new holder is what
	// crosses the stall threshold (see waitForDestHolders); the tick
	// channel is unbuffered and the receiver goroutine reads exactly this
	// many times before returning, so sending one more here would deadlock
	// the test on a send nobody reads.
	for range transferReceiptsMaxStallTicks {
		tick <- time.Now()
	}

	select {
	case ok := <-done:
		if ok {
			t.Fatal("want stall-abort (false) when no new holder arrives within the stall window")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForDestHolders did not return after exhausting the stall window")
	}
}

// ---------- ensureDestManifestEntry ----------

func TestEnsureDestManifestEntryIdempotentOnMatchingRecordCount(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	sourceVaultID := glid.New()
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{
		ID: id, RecordCount: 7, TransferSourceVaultID: sourceVaultID,
	})

	r := &retentionRunner{vaultID: sourceVaultID, now: time.Now}
	meta := chunk.ChunkMeta{ID: id, RecordCount: 7, Sealed: true}
	entry, _, cause := r.ensureDestManifestEntry(fsm, glid.New(), id, meta)
	if entry == nil {
		t.Fatalf("want existing entry returned on matching record count, got defer cause: %s", cause)
	}
	if entry.RecordCount != 7 {
		t.Fatalf("returned entry record count = %d, want 7", entry.RecordCount)
	}
}

func TestEnsureDestManifestEntryDefersOnRecordCountMismatch(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{ID: id, RecordCount: 7})

	r := &retentionRunner{vaultID: glid.New(), now: time.Now}
	// Same chunk ID, DIFFERENT record count: destination already holds a
	// different chunk under this ID — corruption, not a benign retry.
	meta := chunk.ChunkMeta{ID: id, RecordCount: 8, Sealed: true}
	entry, category, cause := r.ensureDestManifestEntry(fsm, glid.New(), id, meta)
	if entry != nil {
		t.Fatal("want nil entry on record-count mismatch — must not silently proceed")
	}
	if category != deferCatCorruption {
		t.Errorf("category = %q, want %q", category, deferCatCorruption)
	}
	if cause == "" {
		t.Fatal("want a non-empty defer cause naming the mismatch")
	}
	for _, want := range []string{"7", "8", "corruption"} {
		if !strings.Contains(cause, want) {
			t.Errorf("defer cause must mention %q; got: %s", want, cause)
		}
	}
}

// TestEnsureDestManifestEntryDefersOnTombstone pins gastrolog-2l918 review
// finding 3b: an announce refused because the destination TOMBSTONED this
// chunk ID must be a NAMED defer cause (deferCatTombstoned) distinct from
// deferCatCorruption — a prior transfer to this destination was retracted
// (abandoned-announce GC, finding 4) or an operator deleted the chunk, and
// the right response is "wait for the tombstone to prune", not "declare
// corruption and alarm as such".
func TestEnsureDestManifestEntryDefersOnTombstone(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	// Seed then finalize-delete so the FSM tombstones id — no pendingDelete
	// request needed; applyFinalizeDelete tombstones unconditionally.
	seedManifestEntry(fsm, vaultctlfsm.ManifestEntry{ID: id, RecordCount: 4})
	fsm.ApplyCommand(vaultctlfsm.NewFinalizeDelete(id))
	if !fsm.IsTombstoned(id) {
		t.Fatal("test setup: chunk must be tombstoned before exercising ensureDestManifestEntry")
	}

	r := &retentionRunner{vaultID: glid.New(), now: time.Now}
	meta := chunk.ChunkMeta{ID: id, RecordCount: 4, Sealed: true}
	entry, category, cause := r.ensureDestManifestEntry(fsm, glid.New(), id, meta)
	if entry != nil {
		t.Fatal("want nil entry for a tombstoned chunk ID — must not silently proceed")
	}
	if category != deferCatTombstoned {
		t.Errorf("category = %q, want %q (distinct from corruption)", category, deferCatTombstoned)
	}
	if !strings.Contains(cause, "tombstone") {
		t.Errorf("cause must name the tombstone; got: %s", cause)
	}
}

// ---------- resolveTransferTarget ----------

func newResolveTargetOrch(t *testing.T, vaults ...system.VaultConfig) *Orchestrator {
	t.Helper()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	store := sysmem.NewStore()
	if len(vaults) == 0 {
		// sysmem.Store.Load returns (nil, nil) for a wholly empty store
		// (see TestRetentionSweepAllClearsAlarmOnRunnerGC's identical
		// workaround) — seed an unrelated vault so loadSystem returns a
		// real (empty-of-the-target) Config.Vaults instead of a nil
		// System, which would make resolveTransferTarget report "failed
		// to load config" instead of the "target doesn't exist" cause
		// these missing-target tests actually want to pin.
		if err := store.PutVault(t.Context(), system.VaultConfig{ID: glid.New(), Name: "unrelated", Type: system.VaultTypeMemory}); err != nil {
			t.Fatalf("seed unrelated vault: %v", err)
		}
	}
	for _, v := range vaults {
		if err := store.PutVault(t.Context(), v); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
	}
	orch.setSystemLoader(&transitionSystemLoader{store: store})
	return orch
}

func TestResolveTransferTargetDefersWhenTargetMissing(t *testing.T) {
	t.Parallel()
	orch := newResolveTargetOrch(t)
	r := &retentionRunner{orch: orch}

	targetID := glid.New()
	cfg, category, cause := r.resolveTransferTarget(targetID)
	if cfg != nil {
		t.Fatal("want nil config for a target that doesn't exist")
	}
	if category != deferCatTargetNotFound {
		t.Errorf("category = %q, want %q", category, deferCatTargetNotFound)
	}
	if !strings.Contains(cause, "no longer exists") {
		t.Errorf("cause must say the target is gone; got: %s", cause)
	}
}

func TestResolveTransferTargetDefersWhenTargetDisabled(t *testing.T) {
	t.Parallel()
	targetID := glid.New()
	orch := newResolveTargetOrch(t, system.VaultConfig{
		ID: targetID, Name: "cold-archive", Type: system.VaultTypeFile, Enabled: false,
	})
	r := &retentionRunner{orch: orch}

	cfg, category, cause := r.resolveTransferTarget(targetID)
	if cfg != nil {
		t.Fatal("want nil config for a disabled target")
	}
	if category != deferCatTargetDisabled {
		t.Errorf("category = %q, want %q", category, deferCatTargetDisabled)
	}
	if !strings.Contains(cause, "disabled") {
		t.Errorf("cause must say the target is disabled; got: %s", cause)
	}
}

func TestResolveTransferTargetDefersWhenTargetNotFileVault(t *testing.T) {
	t.Parallel()
	targetID := glid.New()
	orch := newResolveTargetOrch(t, system.VaultConfig{
		ID: targetID, Name: "mem-target", Type: system.VaultTypeMemory, Enabled: true,
	})
	r := &retentionRunner{orch: orch}

	cfg, category, cause := r.resolveTransferTarget(targetID)
	if cfg != nil {
		t.Fatal("want nil config for a non-file target (config drifted after PutVault validation)")
	}
	if category != deferCatTargetNotFileVault {
		t.Errorf("category = %q, want %q", category, deferCatTargetNotFileVault)
	}
	if !strings.Contains(cause, "file vault") {
		t.Errorf("cause must name the file-vault requirement; got: %s", cause)
	}
}

func TestResolveTransferTargetSucceedsForEnabledFileVault(t *testing.T) {
	t.Parallel()
	targetID := glid.New()
	orch := newResolveTargetOrch(t, system.VaultConfig{
		ID: targetID, Name: "archive", Type: system.VaultTypeFile, Enabled: true, ReplicationFactor: 2,
	})
	r := &retentionRunner{orch: orch}

	cfg, _, cause := r.resolveTransferTarget(targetID)
	if cfg == nil {
		t.Fatalf("want resolved config, got defer cause: %s", cause)
	}
	if cfg.ReplicationFactor != 2 {
		t.Fatalf("resolved config RF = %d, want 2", cfg.ReplicationFactor)
	}
}

// ---------- applyRetentionDispositionToChunk dispatch ----------

// TestApplyRetentionDispositionToChunkDispatchesTransfer confirms the
// disposition gate routes "transfer" to fireTransferEvent rather than
// silently no-op'ing like "delete" — using a target-less runner so the
// transfer arm defers immediately (fast, no Raft) while proving it was
// actually invoked (a deferral was noted).
func TestApplyRetentionDispositionToChunkDispatchesTransfer(t *testing.T) {
	t.Parallel()
	sink := &recordingSink{}
	r := &retentionRunner{
		vaultID:     glid.New(),
		vaultName:   "src",
		disposition: system.RetentionDispositionTransfer,
		orch:        &Orchestrator{alerts: sink},
		now:         time.Now,
		logger:      slog.Default(),
		idleLog:     logging.Throttle{Interval: 10 * time.Minute},
	}
	if got := r.applyRetentionDispositionToChunk(chunk.NewChunkID()); got {
		t.Fatal("want false: no transfer target configured, must defer")
	}
	r.mu.Lock()
	deferred := r.sweepDeferred
	cause := r.lastDeferralCause
	r.mu.Unlock()
	if !deferred {
		t.Fatal("transfer arm must have noted a deferral")
	}
	if !strings.Contains(cause, "retention_transfer_target_vault_id") {
		t.Errorf("deferral cause must name the missing target field; got: %s", cause)
	}
}

// TestApplyRetentionDispositionToChunkDeleteIsNoOp pins that "delete" (and
// the empty/default disposition) still short-circuits to true without
// touching the deferral streak — unchanged by the transfer arm's addition.
func TestApplyRetentionDispositionToChunkDeleteIsNoOp(t *testing.T) {
	t.Parallel()
	r := &retentionRunner{disposition: system.RetentionDispositionDelete}
	if !r.applyRetentionDispositionToChunk(chunk.NewChunkID()) {
		t.Fatal("delete disposition must always return true (no-op)")
	}
}

// TestFireTransferEventReturnsFalseWithNoOrchestrator pins gastrolog-2l918
// review finding 6: a nil orchestrator must retain the chunk (false), not
// silently report success (true) — the old behavior would have destroyed
// the source's only copy with nothing to transfer into.
func TestFireTransferEventReturnsFalseWithNoOrchestrator(t *testing.T) {
	t.Parallel()
	r := &retentionRunner{}
	if got := r.fireTransferEvent(chunk.NewChunkID(), nil); got {
		t.Fatal("want false (retain) when r.orch is nil")
	}
}

// ---------- per-sweep transfer-target stall circuit breaker (gastrolog-2l918 review finding 2) ----------

// TestTransferTargetStalledThisSweep pins the breaker's state machine in
// isolation: unmarked reports not-stalled; marking one target trips the
// breaker for THAT target only, leaving a different target unaffected.
func TestTransferTargetStalledThisSweep(t *testing.T) {
	t.Parallel()
	r := &retentionRunner{}
	targetA := glid.New()
	targetB := glid.New()

	if _, stalled := r.transferTargetStalledThisSweep(targetA); stalled {
		t.Fatal("want not-stalled before anything is marked")
	}

	r.markTransferTargetStalledThisSweep(targetA)

	if cause, stalled := r.transferTargetStalledThisSweep(targetA); !stalled {
		t.Fatal("want stalled after marking targetA")
	} else if !strings.Contains(cause, "stalled earlier this sweep") {
		t.Errorf("cause must name the earlier-this-sweep stall; got: %s", cause)
	}
	if _, stalled := r.transferTargetStalledThisSweep(targetB); stalled {
		t.Fatal("marking targetA must not trip the breaker for targetB")
	}
}

// TestFireTransferEventDefersImmediatelyWhenTargetStalledThisSweep is the
// direct pin for finding 2's "one bounded wait, not three": once a target
// has stalled once this sweep, fireTransferEvent for ANY OTHER chunk
// targeting the same vault must defer immediately via the circuit-breaker
// check — before resolveTransferTarget, admission, announce-import, or any
// receipts wait ever runs. A target-less/no-groupMgr runner would defer
// anyway once it reached resolveTransferTarget, so the meaningful
// assertion is the CAUSE: it must name "stalled earlier this sweep" (the
// breaker), not any of the later gate's causes — proving the early return
// actually fired instead of coincidentally reaching the same outcome via a
// different path.
func TestFireTransferEventDefersImmediatelyWhenTargetStalledThisSweep(t *testing.T) {
	t.Parallel()
	sink := &recordingSink{}
	targetID := glid.New()
	r := &retentionRunner{
		vaultID:        glid.New(),
		vaultName:      "src",
		disposition:    system.RetentionDispositionTransfer,
		transferTarget: &targetID,
		orch:           &Orchestrator{alerts: sink}, // no sysLoader/groupMgr: any later gate would report a DIFFERENT cause
		now:            time.Now,
		logger:         slog.Default(),
		idleLog:        logging.Throttle{Interval: 10 * time.Minute},
	}
	r.markTransferTargetStalledThisSweep(targetID)

	// The target is passed in now rather than read off the runner
	// (gastrolog-6ckv0y): the disposition and its target are resolved per
	// chunk from current config.
	if got := r.fireTransferEvent(chunk.NewChunkID(), &targetID); got {
		t.Fatal("want false (defer) when the target already stalled this sweep")
	}
	r.mu.Lock()
	cause := r.lastDeferralCause
	r.mu.Unlock()
	if !strings.Contains(cause, "stalled earlier this sweep") {
		t.Errorf("cause must be the circuit-breaker's, not a later gate's; got: %s", cause)
	}
}

// TestSweepResetsTransferStallCircuitBreakerAtStart verifies the breaker
// is scoped to ONE sweep: sweep() clears sweepStalledTransferTargets at
// its very start (before the empty-rules early return), so a target that
// stalled last sweep gets a clean, un-throttled retry this sweep.
func TestSweepResetsTransferStallCircuitBreakerAtStart(t *testing.T) {
	t.Parallel()
	r := &retentionRunner{vaultID: glid.New(), isLeader: true}
	targetID := glid.New()
	r.markTransferTargetStalledThisSweep(targetID)

	r.sweep(nil) // no rules — still must reset before the early return

	if _, stalled := r.transferTargetStalledThisSweep(targetID); stalled {
		t.Fatal("sweep() must reset the per-sweep stall circuit breaker even with an empty rule set")
	}
}
