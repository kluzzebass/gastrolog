package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/system"
)

const (
	defaultRetentionSchedule = "0 * * * * *" // every minute
	retentionJobName         = "retention"

	// retentionFanOutWorkers is the number of goroutines that submit records
	// from a single chunk's cursor into the routing pipeline. Matches the
	// digestion and routing worker counts so retention can feed the pipeline
	// at scatterbox-like concurrency without unbounded goroutines.
	retentionFanOutWorkers = 4

	// retentionChunkWorkers limits how many chunks a sweep may process
	// concurrently. Each chunk still routes at most once (!alreadyPending).
	retentionChunkWorkers = 4

	// Vault catchup sweep — runs every 20 seconds (cron: 13/33/53s of
	// each minute) on every node, with a phase offset that doesn't
	// collide with the retention sweep at second 0. Each node consults
	// its OWN replicated FSM and reconciles local disk state in both
	// directions — no leader involvement. Drives three independent
	// catchup mechanisms per vault instance:
	//
	//   - SweepPendingObligations    receipt-protocol delete acks
	//                                (gastrolog-51gme)
	//   - SweepLocalOrphans          tombstone-aware orphan cleanup
	//                                (gastrolog-51gme)
	//   - SweepMissingReplicas       create-side catchup for sealed
	//                                chunks pushed during a follower's
	//                                pause/partition (gastrolog-2dgvj)
	//
	// 20s is fast enough that operator-visible divergence symptoms
	// resolve within a sweep cycle, slow enough that a cluster of N
	// nodes only generates N applies per cycle even when nothing is
	// diverged.
	vaultCatchupSweepJobName  = "vault-catchup-sweep"
	vaultCatchupSweepSchedule = "13,33,53 * * * * *"
)

// retentionKey returns a unique map key for a vault instance's retention state.
func retentionKey(vaultID glid.GLID, storageID string) string {
	if storageID == "" {
		return vaultID.String()
	}
	return vaultID.String() + ":" + storageID
}

// retentionRule is a resolved retention trigger: a compiled policy that
// decides WHEN a chunk fires a retention event. Phase 4 (gastrolog-42f9z)
// removed the action enum and route-target list — the routing engine now
// owns the WHAT, with the chunk's records streamed through it under
// `source = retention-trigger(vault_id)` and the original chunk
// unconditionally destroyed.
type retentionRule struct {
	policy chunk.RetentionPolicy
}

// retentionRunner holds per-vault-instance state that persists across sweeps.
// Only leaders get runners — followers react to the vault-ctl FSM manifest
// via the ChunkFSM.OnDelete callback.
type retentionRunner struct {
	mu      sync.Mutex
	vaultID glid.GLID
	// Cached for job descriptions so the Jobs inspector can tell sweep
	// sub-jobs apart by their vault. Refreshed from config on every sweep
	// via retentionTargetForInstance.
	vaultName string
	vaultType string
	cm           chunk.ChunkManager
	im           index.IndexManager
	inflight     map[chunk.ChunkID]bool // chunks currently being processed
	unreadable   map[chunk.ChunkID]*unreadableEntry // chunks that failed to read — retried with exponential backoff (gastrolog-25vur)
	orch         *Orchestrator          // for eject/transition callbacks

	applyRaftRetentionPending func(id chunk.ChunkID) error

	// reconciler is the instance lifecycle reconciler that owns chunk-lifecycle
	// execution. All production deletes route through reconciler.deleteChunk
	// → CmdRequestDelete (gastrolog-51gme steps 4-7). Nil only in older test
	// harnesses that build VaultInstances directly without going through
	// buildInstance; those harnesses fall through to the legacy
	// direct-delete path below (for cross-node propagation they wire
	// directChunkReplicator.DeleteChunk RPC fan-out separately).
	reconciler *VaultLifecycleReconciler

	// isLeader returns true if this node is the config leader for this instance.
	// Retention (expiry + transitions) only runs on the leader to prevent
	// all nodes from independently transitioning the same chunks.
	isLeader bool

	// followerTargets are the remote nodes that hold replicas of this instance's
	// chunks. Used to forward chunk deletions after retention expires them.
	followerTargets []system.ReplicationTarget

	// disposition is the resolved VaultConfig.RetentionDisposition value.
	// Refreshed on every sweep via retentionTargetForInstance so live config
	// edits take effect on the next tick. Branches the per-chunk path:
	// "delete" skips the routing engine entirely, "route" fans records
	// out for operator-configured routes to forward.
	disposition string

	now    func() time.Time
	logger *slog.Logger
}

type sweepTarget struct {
	runner *retentionRunner
	rules  []retentionRule
}

// retentionSweepAll is the single scheduled retention job. Runs on every node
// that hosts at least one vault instance.
//
// Per-instance role and readiness:
//   - Rule evaluation runs only when the instance is the leader (instance.IsLeader()).
//     Followers skip rule evaluation because rule results must be applied via
//     the vault control-plane Raft, which only the leader writes to.
//
// There is no per-vault Vault.ReadinessErr gate at this level because the
// per-vault IsLeader checks already cover the preconditions for each action.
// See vault_readiness.go for the canonical vault readiness definition used
// by ingest/query entry points.
//
// Catchup mechanisms (post gastrolog-51gme): pendingDeletes covers
// nodes that observed CmdRequestDelete (steady-state apply or post-
// snapshot ReconcileFromSnapshot). SweepLocalOrphans, on the
// pending-delete sweep cron, covers the snapshot-restore gap where a
// delete cycle finalized while this node was offline — the rejoining
// node receives a snapshot whose FSM has only the tombstone, with no
// pendingDeletes entry to drive cleanup.
func (o *Orchestrator) retentionSweepAll() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.retentionLogger.Error("retention: failed to load config", "error", err)
		return
	}
	if sys == nil {
		return
	}
	cfg := &sys.Config

	var targets []sweepTarget
	active := make(map[string]bool)

	o.mu.Lock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		vaultInst := vault.Instance
		if vaultInst != nil && vaultInst.IsLeader() {
			if t := o.retentionTargetForInstance(cfg, vaultCfg, vaultInst, active); t != nil {
				targets = append(targets, *t)
			}
		}
	}
	for key := range o.retention {
		if !active[key] {
			delete(o.retention, key)
		}
	}
	o.mu.Unlock()

	// Leader: evaluate retention rules.
	for _, t := range targets {
		t.runner.sweep(t.rules)
	}

	// Phase 4 (gastrolog-42f9z): confirmStreamedTransitions / the
	// transition-receipt protocol is gone. Retention firing routes
	// records through the routing engine which delivers them
	// immediately; there is no asynchronous "wait for destination
	// receipt" stage anymore.
	//
	// (Disk-vs-manifest orphan cleanup and missing-replica catchup are
	// done out-of-band on the instance-catchup sweep tick — see
	// vaultCatchupSweepAll. The retention sweep stays focused on rule
	// evaluation.)

	// Memory budget enforcement: transition oldest chunks when over budget.
	o.enforceMemoryBudgets(cfg)

	// Cache eviction: collect evictors under lock, run outside.
	// EvictCache does filesystem I/O — holding the lock would block Raft applies.
	var evictors []chunk.ChunkCacheEvictor
	o.mu.RLock()
	for _, vault := range o.vaults {
		if vaultInst := vault.Instance; vaultInst != nil {
			if evictor, ok := vaultInst.Chunks.(chunk.ChunkCacheEvictor); ok {
				evictors = append(evictors, evictor)
			}
		}
	}
	o.mu.RUnlock()
	for _, evictor := range evictors {
		evictor.EvictCache()
	}
}

// vaultCatchupSweepAll runs every 20 seconds (cron 13/33/53s, phase-
// offset from the retention sweep) on every node. For each (vault,
// instance) on this node it asks the lifecycle reconciler to run all
// three local-state catchup sweeps:
//
//  1. SweepPendingObligations — re-runs fulfillObligation for any
//     pendingDeletes entry where this node is still in ExpectedFrom.
//     Covers the case where the steady-state onRequestDelete callback
//     fired but didn't ack (apply-pump wedge, transient failure, etc.).
//     gastrolog-51gme.
//
//  2. SweepLocalOrphans — deletes local sealed chunks that the FSM has
//     positively tombstoned but no longer references in the manifest
//     or pendingDeletes. Covers the case where a delete cycle ran to
//     completion while this node was offline; snapshot install brings
//     the FSM forward to the post-finalize state, leaving the local
//     file orphaned with no receipt obligation to drive cleanup.
//     gastrolog-51gme.
//
//  3. SweepMissingReplicas — asks the placement leader to re-push
//     sealed chunks present in the FSM but missing locally. Covers
//     the create-side gap where the leader pushed a sealed chunk
//     during this node's pause/partition window and the gRPC failed
//     with no retry. gastrolog-2dgvj.
//
// All three sweeps are local-only on the originating side: each node
// consults its OWN replicated FSM state and decides independently.
// (1) and (2) take no remote actions. (3) sends a unary RPC to the
// placement leader, but the *decision* to send is local — the leader
// is just the transport for the response.
func (o *Orchestrator) vaultCatchupSweepAll() {
	o.mu.RLock()
	vaultInsts := make([]*VaultInstance, 0)
	for _, vault := range o.vaults {
		if t := vault.Instance; t != nil && t.Reconciler != nil {
			vaultInsts = append(vaultInsts, t)
		}
	}
	o.mu.RUnlock()
	for _, t := range vaultInsts {
		t.Reconciler.SweepPendingObligations()
		t.Reconciler.SweepLocalOrphans()
		t.Reconciler.SweepMissingReplicas()
		t.Reconciler.SweepStaleLeaderFSMEntries()
		t.Reconciler.SweepStalePendingDeleteAcks()
		t.Reconciler.SweepIdleActiveChunks()
	}
}

// enforceMemoryBudgets checks memory vaults for budget overruns and transitions
// the oldest sealed chunks to the next instance. Only runs on leaders.
func (o *Orchestrator) enforceMemoryBudgets(cfg *system.Config) {
	if cfg == nil {
		return
	}
	type budgetTarget struct {
		vaultID glid.GLID
		cm      chunk.ChunkManager
		excess  int64
	}

	var targets []budgetTarget
	o.mu.RLock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		vaultInst := vault.Instance
		if vaultInst == nil || !vaultInst.IsLeader() {
			continue
		}
		monitor, ok := vaultInst.Chunks.(chunk.ChunkBudgetMonitor)
		if !ok {
			continue
		}
		if excess := monitor.BudgetExceeded(); excess > 0 {
			targets = append(targets, budgetTarget{
				vaultID: vaultCfg.ID,
				cm:      vaultInst.Chunks,
				excess:  excess,
			})
		}
	}
	o.mu.RUnlock()

	for _, t := range targets {
		o.drainExcessChunks(t.vaultID, t.cm, t.excess)
	}
}

// drainExcessChunks fires retention events on the oldest sealed chunks
// of a memory vault until the excess bytes are reclaimed (or no more
// sealed chunks remain). Phase 4 (gastrolog-42f9z): these used to be
// "transitioned to the next instance"; now they're just retention events
// like any other, with the routing engine deciding their fate.
func (o *Orchestrator) drainExcessChunks(vaultID glid.GLID, cm chunk.ChunkManager, excess int64) {
	metas, err := cm.List()
	if err != nil {
		return
	}

	// Sort oldest first (by WriteStart).
	slices.SortFunc(metas, func(a, b chunk.ChunkMeta) int {
		return a.WriteStart.Compare(b.WriteStart)
	})

	// Find the index manager for this vault.
	var im index.IndexManager
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil {
		if vaultInst := vault.Instance; vaultInst != nil {
			im = vaultInst.Indexes
		}
	}
	o.mu.RUnlock()

	var reclaimed int64
	for _, m := range metas {
		if reclaimed >= excess {
			break
		}
		if !m.Sealed {
			continue
		}

		runner := &retentionRunner{
			isLeader: true,
			vaultID:  vaultID,
			cm:       cm,
			im:       im,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		runner.fireRetentionEvent(m.ID)
		runner.expireChunk(m.ID, "memory-budget-enforcement")
		reclaimed += m.Bytes
	}

	if reclaimed > 0 {
		o.retentionLogger.Info("memory budget enforcement: retention events fired",
			"vault", vaultID,
			"excess", excess, "reclaimed", reclaimed)
	}
}

// UnreadableChunks returns chunk IDs currently flagged as unreadable
// across all instance-instance retention runners for the vault. Used by
// the inspector to surface which chunks are in retry-with-backoff and
// by tests. Read-only accessor; safe to call from any node.
func (o *Orchestrator) UnreadableChunks(vaultID glid.GLID) []chunk.ChunkID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	var ids []chunk.ChunkID
	for _, runner := range o.retention {
		if runner.vaultID != vaultID {
			continue
		}
		runner.mu.Lock()
		for id := range runner.unreadable {
			ids = append(ids, id)
		}
		runner.mu.Unlock()
	}
	return ids
}

// RetryUnreadableChunks resets every unreadable chunk's retry backoff
// across all instance-instance retention runners for the vault, so the
// next retention sweep retries them all immediately. Returns the total
// count of entries reset across runners. Operator-driven recovery
// action exposed via the manual "Retry unreadable" inspector button
// — see gastrolog-25vur.
func (o *Orchestrator) RetryUnreadableChunks(vaultID glid.GLID) int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	total := 0
	for _, runner := range o.retention {
		if runner.vaultID != vaultID {
			continue
		}
		total += runner.retryUnreadableChunks()
	}
	return total
}


// RetentionPendingChunks returns chunk IDs marked as retention-pending in the
// vault-ctl FSM for a vault. Visible to all nodes via Raft replication.
//
// Read-only accessor, callable from any-node. No Vault.ReadinessErr gate —
// observational use only. Decision-making callers should gate on
// Vault.ReadinessErr first.
func (o *Orchestrator) RetentionPendingChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID]bool)
	if vaultInst := vault.Instance; vaultInst != nil && vaultInst.ListRetentionPending != nil {
		for _, id := range vaultInst.ListRetentionPending() {
			result[id] = true
		}
	}
	return result
}

// PendingDeleteAcks returns, for each chunk currently in any instance's
// receipt-protocol pendingDeletes map within the vault, the set of node
// IDs that have NOT yet acked the delete. As nodes ack, their entry is
// removed from ExpectedFrom; what's returned here is the still-owed
// set. Empty/missing entry means the chunk isn't pending a delete.
//
// Lets the inspector show operators which specific node is the laggard
// holding up a stuck delete (e.g., "pending-ack from: node-3"). The
// FSM is replicated, so any node's view is authoritative and the
// caller doesn't need to fan out.
//
// Read-only accessor, callable from any node. No readiness gating —
// observational use only.
func (o *Orchestrator) PendingDeleteAcks(vaultID glid.GLID) map[chunk.ChunkID][]string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID][]string)
	if vaultInst := vault.Instance; vaultInst != nil && vaultInst.Reconciler != nil && vaultInst.Reconciler.fsm != nil {
		for _, p := range vaultInst.Reconciler.fsm.PendingDeletes() {
			expected := make([]string, 0, len(p.ExpectedFrom))
			for nodeID := range p.ExpectedFrom {
				expected = append(expected, nodeID)
			}
			result[p.ChunkID] = expected
		}
	}
	return result
}

// retentionTargetForInstance resolves a single vault instance into a sweep target.
// Returns nil if the instance should be skipped (no rules, no leader, etc.).
func (o *Orchestrator) retentionTargetForInstance(cfg *system.Config, vaultCfg system.VaultConfig, vaultInst *VaultInstance, active map[string]bool) *sweepTarget {
	if vaultInst.HasRaftLeader != nil && !vaultInst.HasRaftLeader() {
		return nil
	}
	// IsRaftLeader check removed: the instance apply forwarder transparently
	// routes applies to the vault-ctl Raft leader. The config placement leader
	// always runs retention regardless of vault-ctl Raft leadership.
	if len(vaultCfg.RetentionRules) == 0 {
		return nil
	}
	rules, err := resolveRetentionRulesFromVault(cfg, vaultCfg)
	if err != nil {
		o.retentionLogger.Warn("retention: failed to resolve rules",
			"vault", vaultCfg.ID, "error", err)
		return nil
	}
	if len(rules) == 0 {
		return nil
	}

	key := retentionKey(vaultInst.VaultID, vaultInst.StorageID)
	active[key] = true

	runner := o.retention[key]
	if runner == nil {
		runner = &retentionRunner{
			vaultID: vaultCfg.ID,
			cm:      vaultInst.Chunks,
			im:      vaultInst.Indexes,
			orch:    o,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[key] = runner
	}
	runner.cm = vaultInst.Chunks
	runner.im = vaultInst.Indexes
	runner.applyRaftRetentionPending = vaultInst.ApplyRaftRetentionPending
	runner.reconciler = vaultInst.Reconciler
	runner.isLeader = vaultInst.IsLeader()
	runner.followerTargets = vaultInst.FollowerTargets
	runner.vaultName = vaultCfg.Name
	runner.vaultType = string(vaultCfg.Type)
	runner.disposition = vaultCfg.ResolveRetentionDisposition()
	return &sweepTarget{runner: runner, rules: rules}
}

// (Disk-vs-manifest orphan cleanup lives on VaultLifecycleReconciler now —
// see SweepLocalOrphans. It is tombstone-aware: only chunks the FSM has
// positively confirmed as finalize-deleted are eligible for cleanup, so
// freshly-created chunks with announce in flight are never racey-deleted.)

// sweep evaluates retention rules on a leader and applies expire/eject/transition.
func (r *retentionRunner) sweep(rules []retentionRule) {
	// Only the config placement leader runs retention. Raft applies are
	// forwarded transparently to the Raft leader via VaultCtlChunkApplyForwarder.
	// Config followers must not independently evaluate and transition chunks —
	// that causes N× duplication.
	if !r.isLeader {
		return
	}

	r.mu.Lock()
	if r.inflight == nil {
		r.inflight = make(map[chunk.ChunkID]bool)
	}
	r.mu.Unlock()

	if len(rules) == 0 {
		return
	}

	metas, err := r.cm.List()
	if err != nil {
		r.logger.Error("retention: failed to list chunks", "vault", r.vaultID, "error", err)
		return
	}

	r.mu.Lock()
	unreadable := r.unreadable
	vaultInst := r.findVaultInstance()
	r.mu.Unlock()

	// Phase 3 (gastrolog-1huz5): overlay each meta with FSM state so
	// selectRetentionCandidates' meta.Sealed gate reflects cluster
	// truth — Sealing chunks (active-form files closed locally but
	// GLCB not yet committed) must not be eligible.
	if vaultInst != nil && vaultInst.OverlayFromFSM != nil {
		for i := range metas {
			metas[i] = vaultInst.OverlayFromFSM(metas[i])
		}
	}

	// gastrolog-5sywa: the transition-streamed flag and the
	// destination-receipt set are gone. Phase 4 removed the receipt
	// protocol entirely — retention firing is synchronous, no chunks
	// linger in a "streamed but not yet confirmed" state.
	streamed := map[chunk.ChunkID]bool{}

	// Build a set of chunks already flagged retention-pending in the FSM.
	// We pass this down so tryRetainChunk skips the redundant
	// CmdRetentionPending Apply on chunks where the flag is already set,
	// which is critical when retention actions stall (transition
	// unreachable destination, receipt-protocol stuck) and the same
	// chunk gets re-evaluated every minute. See gastrolog-51gme.
	pendingFlag := make(map[chunk.ChunkID]bool)
	if vaultInst != nil && vaultInst.ListRetentionPending != nil {
		for _, id := range vaultInst.ListRetentionPending() {
			pendingFlag[id] = true
		}
	}

	manifest, manifestKnown := buildManifestSet(vaultInst)

	now := time.Now()
	sealed := selectRetentionCandidates(metas, streamed, manifest, manifestKnown, unreadable, now)

	if len(sealed) == 0 {
		return
	}

	state := chunk.VaultState{
		Chunks: sealed,
		Now:    r.now(),
	}

	processed := make(map[chunk.ChunkID]bool)

	for _, b := range rules {
		matched := b.policy.Apply(state)
		var chunkWG sync.WaitGroup
		chunkSem := make(chan struct{}, retentionChunkWorkers)
		for _, id := range matched {
			if processed[id] {
				continue
			}
			processed[id] = true
			chunkWG.Add(1)
			go func(id chunk.ChunkID, rule retentionRule, alreadyPending bool) {
				defer chunkWG.Done()
				chunkSem <- struct{}{}
				defer func() { <-chunkSem }()
				r.tryRetainChunk(id, rule, alreadyPending)
			}(id, b, pendingFlag[id])
		}
		chunkWG.Wait()
	}
}

// buildManifestSet returns the FSM-known chunk IDs for the given instance and a
// flag indicating whether the manifest is queryable. Any chunk on disk whose
// ID is NOT in the manifest is a ghost — its FSM entry was finalize-deleted
// but the disk file was never reaped. Filtering ghosts out of the retention
// sweep prevents repeated no-op transitions (the apply silently no-ops when
// f.chunks[id] is nil, the flag never sticks, and we re-stream the chunk's
// records to the next instance on every sweep). See gastrolog-66b7x.
func buildManifestSet(vaultInst *VaultInstance) (map[chunk.ChunkID]bool, bool) {
	manifest := make(map[chunk.ChunkID]bool)
	if vaultInst == nil || vaultInst.ListManifest == nil {
		return manifest, false
	}
	ids := vaultInst.ListManifest()
	if ids == nil {
		return manifest, false
	}
	for _, id := range ids {
		manifest[id] = true
	}
	return manifest, true
}

// selectRetentionCandidates filters chunk metas to the set retention can act
// on right now: sealed, not currently being streamed, recognized by the FSM
// manifest (when available), and past any unreadable-retry backoff window.
func selectRetentionCandidates(
	metas []chunk.ChunkMeta,
	streamed map[chunk.ChunkID]bool,
	manifest map[chunk.ChunkID]bool,
	manifestKnown bool,
	unreadable map[chunk.ChunkID]*unreadableEntry,
	now time.Time,
) []chunk.ChunkMeta {
	var sealed []chunk.ChunkMeta
	for _, meta := range metas {
		if !meta.Sealed || streamed[meta.ID] {
			continue
		}
		if manifestKnown && !manifest[meta.ID] {
			continue // ghost chunk: on disk but no FSM entry
		}
		if entry := unreadable[meta.ID]; entry != nil && now.Before(entry.nextRetry) {
			continue
		}
		sealed = append(sealed, meta)
	}
	return sealed
}

// tryRetainChunk attempts to apply a retention action to a single chunk.
// Acquires the inflight lock, marks retention-pending via Raft (only if
// the FSM doesn't already have the flag — repeated applies waste Raft
// capacity and were a major contributor to leader-queue saturation
// when retention actions stalled with hundreds of pending chunks; see
// gastrolog-51gme), and dispatches to the action handler.
func (r *retentionRunner) tryRetainChunk(id chunk.ChunkID, b retentionRule, alreadyPending bool) {
	r.mu.Lock()
	if r.inflight[id] {
		r.mu.Unlock()
		return
	}
	r.inflight[id] = true
	r.mu.Unlock()

	// Mark as retention-pending in vault-ctl Raft so all nodes see it —
	// but ONLY if the FSM doesn't already carry the flag. Skipping the
	// redundant Apply when the action stalls (transition unreachable
	// destination, receipt protocol stuck) avoids piling up no-op
	// CmdRetentionPending entries on every sweep tick.
	if r.applyRaftRetentionPending != nil && !alreadyPending {
		if !r.markRetentionPending(id) {
			return
		}
	}

	// gastrolog-18du3: the disposition flag controls whether records are
	// streamed through the routing engine before the chunk is destroyed.
	// "delete" (the safe default) skips routing entirely — records drop
	// and storage frees immediately. "route" preserves the prior Phase 5
	// behavior: records flow through the routing engine with synthetic
	// `_source = "retention"`, so operator-configured routes can forward
	// them to archive vaults, cold storage, etc. The original chunk is
	// always destroyed regardless of disposition.
	//
	// Gate routing on !alreadyPending: routing is a one-shot fire-and-
	// forget per chunk. When the source delete fails (receipt protocol
	// stuck, unreachable destination), subsequent sweeps re-enter this
	// path and must NOT re-route the records — without this guard, every
	// retention tick re-streams the same chunk's records to the
	// destination, multiplying storage at the route target each cycle.
	// Operator footgun from gastrolog-2eclw-fix: first-vault with route
	// `_source="retention" AND _vault="<first>"` → second-vault grew 50-100
	// MB/s with no active ingesters because every sweep re-routed the
	// 11 stuck retention-pending chunks. Routing once + retrying only
	// the delete is the correct shape: the chunk gets routed exactly
	// once at the moment retention decides it should be retired.
	defer r.clearInflight(id)
	if !alreadyPending {
		r.applyRetentionDispositionToChunk(id)
	}
	r.expireChunk(id, "retention-trigger")
}

// markRetentionPending applies CmdRetentionPending via Raft and emits a
// SEALED event so subscribers see the flag flip without a ListChunks
// refetch. Returns true on success, false if the Raft apply failed (in
// which case the caller has already been backed off by clearInflight).
// Extracted from tryRetainChunk to keep the parent function's nesting
// shallow.
func (r *retentionRunner) markRetentionPending(id chunk.ChunkID) bool {
	if err := r.applyRaftRetentionPending(id); err != nil {
		r.logger.Error("retention: failed to apply raft retention-pending",
			"vault", r.vaultID, "chunk", id, "error", err)
		r.clearInflight(id)
		return false
	}
	// Carry the post-flag meta so subscribers can patch their cache
	// without a ListChunks refetch.
	if r.orch != nil {
		if meta, err := r.cm.Meta(id); err == nil {
			r.orch.EmitChunkSealed(r.vaultID, meta)
		} else {
			r.orch.NotifyChunkChange()
		}
	}
	return true
}

// applyRetentionDispositionToChunk runs the chunk's records through the
// routing engine when the vault's disposition is "route"; otherwise it
// is a no-op. The caller is still responsible for destroying the chunk
// via expireChunk regardless of disposition. Extracted so tests can
// verify the disposition gate without standing up the full
// expire-chunk machinery (which needs a reconciler, Raft, etc.). See
// gastrolog-18du3.
func (r *retentionRunner) applyRetentionDispositionToChunk(id chunk.ChunkID) {
	if r.disposition == system.RetentionDispositionRoute {
		r.fireRetentionEvent(id)
	}
}

// fireRetentionEvent streams the chunk's records through the routing
// engine with synthetic `_source = "retention"` and `_vault = vaultID`.
// Each matched route writes the records to its destinations. The
// original chunk is destroyed in the caller regardless of routing
// outcome — records with no matching route drop silently, the same
// observable behavior as the legacy expire action.
//
// gastrolog-4kkoo (Phase 5): replaces the Phase-4 stub that consulted
// nothing. The `_reason` synthetic attribute is left empty for now —
// distinguishing age vs. size vs. count requires splitting composite
// retention policies into per-trigger rules, which lands in a
// follow-up. Routes targeting retention today match on
// `_source = "retention"` and `_vault = "<id>"`.
//
// SubmitRetentionRecord routes each record through the pipeline: route
// evaluation against the published table, then durable segment write on
// every matched vault's home (cross-node delivery via segment
// distribution/collection). A record with no matching destination is a
// counted, silent drop.
//
// Operator footgun: a route that matches `_source = "retention"`
// AND lists the source vault as a destination creates a cascade —
// re-ingested records produce new chunks that themselves expire on
// the next sweep. Routes for retention should target a different
// vault (cold storage, archive, etc.).
func (r *retentionRunner) fireRetentionEvent(id chunk.ChunkID) {
	if r.orch == nil {
		return
	}
	vaultInst := r.findVaultInstance()
	if vaultInst == nil || vaultInst.Chunks == nil {
		return
	}

	cur, err := vaultInst.Chunks.OpenCursor(id)
	if err != nil {
		r.logger.Warn("retention: open cursor for fan-out failed",
			"vault", r.vaultID, "chunk", id, "error", err)
		return
	}
	defer func() {
		if cerr := cur.Close(); cerr != nil {
			r.logger.Warn("retention: close cursor after fan-out failed",
				"vault", r.vaultID, "chunk", id, "error", cerr)
		}
	}()

	jobs := make(chan chunk.Record, retentionFanOutWorkers)
	var submitWG sync.WaitGroup
	for range retentionFanOutWorkers {
		submitWG.Go(func() {
			for rec := range jobs {
				if subErr := r.orch.SubmitRetentionRecord(context.Background(), r.vaultID, rec, ""); subErr != nil {
					r.logger.Warn("retention: fan-out submit error",
						"vault", r.vaultID, "chunk", id, "error", subErr)
					// Continue — partial fan-out is acceptable; the original
					// chunk will still be destroyed by the caller.
				}
			}
		})
	}

	fanned := 0
	for {
		rec, _, err := cur.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			close(jobs)
			submitWG.Wait()
			r.logger.Warn("retention: fan-out cursor error",
				"vault", r.vaultID, "chunk", id, "error", err)
			return
		}
		// Cursor records may reference mmap'd memory that becomes
		// invalid once we move on. Copy so the pipeline can persist
		// independently of the cursor lifecycle.
		jobs <- rec.Copy()
		fanned++
	}
	close(jobs)
	submitWG.Wait()
	r.logger.Debug("retention: fanned out chunk records via pipeline routing",
		"vault", r.vaultID, "chunk", id, "count", fanned)
}

// findVaultInstance looks up this runner's instance in the orchestrator's vault registry.
func (r *retentionRunner) findVaultInstance() *VaultInstance {
	if r.orch == nil {
		return nil
	}
	vault := r.orch.vaults[r.vaultID]
	if vault == nil || vault.Instance == nil {
		return nil
	}
	if vault.Instance.VaultID != r.vaultID {
		return nil
	}
	return vault.Instance
}

// clearInflight removes a chunk from the in-flight set.
func (r *retentionRunner) clearInflight(id chunk.ChunkID) {
	r.mu.Lock()
	delete(r.inflight, id)
	r.mu.Unlock()
}

// unreadableEntry tracks per-chunk retry scheduling for chunks that
// failed to read. Each retention sweep checks nextRetry; the chunk is
// skipped while now is before nextRetry. After the deadline, the next
// sweep retries via transitionChunk; success clears the entry, failure
// schedules the next retry further out via exponential backoff (capped
// at 24h). Replaces the prior boolean "unreadable forever" semantics.
// See gastrolog-25vur.
type unreadableEntry struct {
	failCount int
	nextRetry time.Time
}

// unreadableBackoff returns the wait time before the next retry given
// the current cumulative fail count. Schedule: 5m, 15m, 1h, 6h, 24h
// (cap). Picked so transient cloud blips clear within minutes while
// genuine corruption doesn't churn excessive cloud requests.
func unreadableBackoff(failCount int) time.Duration {
	schedule := []time.Duration{
		5 * time.Minute,
		15 * time.Minute,
		1 * time.Hour,
		6 * time.Hour,
		24 * time.Hour,
	}
	if failCount < 1 {
		return schedule[0]
	}
	if failCount-1 >= len(schedule) {
		return schedule[len(schedule)-1]
	}
	return schedule[failCount-1]
}

// markUnreadable flags a chunk as unreadable and schedules its next
// retry. Each successive failure pushes the next retry further out
// per unreadableBackoff. The chunk-unreadable alert is set; it stays
// up while the entry exists and is cleared by clearUnreadable.
func (r *retentionRunner) markUnreadable(id chunk.ChunkID, reason error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unreadable == nil {
		r.unreadable = make(map[chunk.ChunkID]*unreadableEntry)
	}
	entry := r.unreadable[id]
	if entry == nil {
		entry = &unreadableEntry{}
		r.unreadable[id] = entry
	}
	entry.failCount++
	entry.nextRetry = time.Now().Add(unreadableBackoff(entry.failCount))
	if r.orch.alerts != nil {
		r.orch.alerts.Set(
			fmt.Sprintf("chunk-unreadable:%s", id),
			alert.Error, "retention",
			fmt.Sprintf("Chunk %s unreadable: %v (next retry %s)", id, reason, entry.nextRetry.Format(time.RFC3339)),
		)
	}
}

// clearUnreadable removes a chunk's unreadable entry — used either
// after a successful retry (transition.go) or by an operator-driven
// "retry now" action (RetryUnreadableChunks). Also clears the alert.
func (r *retentionRunner) clearUnreadable(id chunk.ChunkID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.unreadable[id]; !ok {
		return
	}
	delete(r.unreadable, id)
	if r.orch.alerts != nil {
		r.orch.alerts.Clear(fmt.Sprintf("chunk-unreadable:%s", id))
	}
}

// retryUnreadableChunks resets every unreadable entry's nextRetry to
// now so the next retention sweep retries them all immediately.
// Returns the count of entries reset. Used by the manual-recovery
// action operators trigger from the inspector. See gastrolog-25vur.
func (r *retentionRunner) retryUnreadableChunks() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	count := 0
	for _, entry := range r.unreadable {
		entry.nextRetry = now
		count++
	}
	return count
}

// expireChunk routes a chunk deletion through the lifecycle reconciler's
// receipt protocol when cluster Raft is wired, and falls back to a direct
// local delete otherwise. reason ends up in the FSM's pendingDeletes
// entry and in audit logs — see deleteChunk for the canonical reason
// catalog.
//
// Cluster path (gastrolog-51gme step 4):
//   reconciler.deleteChunk → CmdRequestDelete → onRequestDelete fires on
//   every node in expectedFrom (including this leader) and each one
//   deletes its local copy + acks. Once expectedFrom is empty the leader
//   proposes CmdFinalizeDelete and the FSM entry is removed. The
//   reconciler bumps NotifyChunkChange and walks same-node sibling TIs
//   itself, so retention only owns the rate-alert side-effect here.
//
// Single-node path:
//   reconciler.deleteChunk's local-only fallback handles the direct
//   delete (indexes + chunk + sibling TIs + chunk-change notify).
func (r *retentionRunner) expireChunk(id chunk.ChunkID, reason string) {
	if r.reconciler != nil {
		expectedFrom := r.expectedFromForExpire()
		if err := r.reconciler.deleteChunk(id, reason, expectedFrom); err != nil {
			r.logger.Warn("retention: reconciler deleteChunk failed, will retry",
				"vault", r.vaultID, "chunk", id.String(), "reason", reason, "error", err)
			return
		}
		if r.orch != nil && r.orch.retentionRates != nil {
			// Per-instance rate alert (see gastrolog-47qyw). Only counted on
			// the leader path (this function only runs on instance leaders)
			// so the rate reflects active expiration decisions, not
			// follower delete-cascade applications.
			r.orch.retentionRates.Record(r.vaultID, r.orch.now())
		}
		r.logger.Debug("retention: requested chunk delete via reconciler",
			"vault", r.vaultID, "chunk", id.String(), "reason", reason)
		return
	}

	// Reconciler-less fallback: legacy direct-delete path.
	//
	// Reached only by older test harnesses that build a retentionRunner
	// without going through buildInstance (so instance.Reconciler is nil).
	// They wire cross-node propagation via directChunkReplicator.DeleteChunk
	// RPC fan-out (forwardDeletionToFollowers below) instead of vault-ctl
	// Raft. Production has no path into here after gastrolog-51gme step 11
	// — ApplyRaftDelete / CmdDeleteChunk producers are gone.
	if err := r.im.DeleteIndexes(id); err != nil {
		r.logger.Error("retention: failed to delete indexes",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}
	if err := r.cm.Delete(id); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
		r.logger.Error("retention: failed to delete chunk",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}
	if r.orch != nil {
		if r.orch.retentionRates != nil {
			r.orch.retentionRates.Record(r.vaultID, r.orch.now())
		}
		r.orch.logChunkExpunged(r.vaultID, id, reason)
		r.orch.EmitChunkDeleted(r.vaultID, id)
		r.orch.deleteFromFollowers(r.vaultID, id)
		r.forwardDeletionToFollowers(id)
	}
	r.logger.Debug("retention: deleted chunk (no-reconciler fallback)",
		"vault", r.vaultID, "chunk", id.String())
}

// expectedFromForExpire returns the placement-membership-at-decision-time
// for a retention-driven delete: the local node (always the leader, since
// retention only runs on leaders) plus every follower target's node ID.
// Duplicate node IDs (same-node follower placements) collapse on the FSM
// side via the map[string]bool encoding in MarshalRequestDelete.
//
// localNodeID is sourced from the reconciler — that is the canonical
// identity for "who fulfills obligations on this node". Sourcing it
// from the orchestrator would couple the retention test path to the
// orchestrator construction; the reconciler is already required for
// this code path so reusing its localNodeID is strictly tighter.
func (r *retentionRunner) expectedFromForExpire() []string {
	if r.reconciler == nil {
		return nil
	}
	localNodeID := r.reconciler.localNodeID
	expected := make([]string, 0, 1+len(r.followerTargets))
	expected = append(expected, localNodeID)
	for _, t := range r.followerTargets {
		if t.NodeID == "" || t.NodeID == localNodeID {
			continue
		}
		expected = append(expected, t.NodeID)
	}
	return expected
}

// (placementMembership lives on Orchestrator in vault_ops.go and serves
// the cluster paths that aren't routed through a retention runner —
// archival sweep, the cloud reconciliation suspect-expiry, etc.)

// forwardDeletionToFollowers sends an explicit delete RPC to each remote
// follower. Used only by the reconciler-less fallback path in expireChunk
// (test harnesses without a vault-ctl Raft group / VaultLifecycleReconciler).
// Production runs through the receipt protocol and never reaches here.
// The directChunkReplicator.DeleteChunk RPC chain stays for that harness;
// removing it requires migrating the harness onto the reconciler with a
// fake-FSM-applier — a follow-up refactor outside the scope of step 11.
func (r *retentionRunner) forwardDeletionToFollowers(id chunk.ChunkID) {
	for _, target := range r.followerTargets {
		if target.NodeID == r.orch.localNodeID {
			continue // already handled by deleteFromFollowers
		}
		r.forwardDeleteWithRetry(target.NodeID, id)
	}
}

// forwardDeleteWithRetry sends a chunk-delete RPC to a follower with up to
// 3 retries on transient failures. "chunk not found" means the chunk is
// already gone on the follower — goal achieved, no retry needed.
func (r *retentionRunner) forwardDeleteWithRetry(nodeID string, id chunk.ChunkID) {
	const maxAttempts = 3
	for attempt := range maxAttempts {
		err := r.sendDeleteToFollower(nodeID, id)
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "chunk not found") {
			r.logger.Debug("retention: chunk already gone on follower",
				"vault", r.vaultID, "chunk", id.String(), "follower", nodeID)
			return
		}
		if attempt < maxAttempts-1 {
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
			continue
		}
		r.logger.Warn("retention: failed to forward chunk deletion to follower",
			"vault", r.vaultID, "chunk", id.String(),
			"follower", nodeID, "error", err, "attempts", maxAttempts)
	}
}

// sendDeleteToFollower issues a single chunk-delete RPC via the instance
// replicator. Returns nil when no replicator is configured (single-node mode).
func (r *retentionRunner) sendDeleteToFollower(followerID string, id chunk.ChunkID) error {
	if r.orch.chunkReplicator == nil {
		return nil
	}
	return r.orch.chunkReplicator.DeleteChunk(
		context.Background(), followerID, r.vaultID, id,
	)
}
