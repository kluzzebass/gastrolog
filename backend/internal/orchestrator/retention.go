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
	defaultRetentionSchedule = "* * * * *" // every minute
	retentionJobName         = "retention"

	// Tier catchup sweep — runs every 20 seconds (cron: 13/33/53s of
	// each minute) on every node, with a phase offset that doesn't
	// collide with the retention sweep at second 0. Each node consults
	// its OWN replicated FSM and reconciles local disk state in both
	// directions — no leader involvement. Drives three independent
	// catchup mechanisms per tier instance:
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
	tierCatchupSweepJobName  = "tier-catchup-sweep"
	tierCatchupSweepSchedule = "13,33,53 * * * * *"
)

// retentionKey returns a unique map key for a tier instance's retention state.
func retentionKey(tierID glid.GLID, storageID string) string {
	if storageID == "" {
		return tierID.String()
	}
	return tierID.String() + ":" + storageID
}

// retentionRule is a resolved rule: a compiled policy paired with an action.
type retentionRule struct {
	policy        chunk.RetentionPolicy
	action        system.RetentionAction
	ejectRouteIDs []glid.GLID // target route IDs, only for eject
}

// retentionRunner holds per-tier-instance state that persists across sweeps.
// Only leaders get runners — followers react to the tier FSM manifest
// via the ChunkFSM.OnDelete callback.
type retentionRunner struct {
	mu      sync.Mutex
	vaultID glid.GLID
	tierID  glid.GLID
	// Cached for job descriptions so the Jobs inspector can tell sweep
	// sub-jobs (transitions) apart by their vault/tier. Refreshed from
	// config on every sweep via retentionTargetForTier.
	vaultName    string
	tierPosition int
	tierType     string
	cm           chunk.ChunkManager
	im           index.IndexManager
	inflight     map[chunk.ChunkID]bool // chunks currently being processed
	unreadable   map[chunk.ChunkID]*unreadableEntry // chunks that failed to read — retried with exponential backoff (gastrolog-25vur)
	orch         *Orchestrator          // for eject/transition callbacks

	applyRaftRetentionPending   func(id chunk.ChunkID) error
	applyRaftTransitionStreamed func(id chunk.ChunkID) error

	// reconciler is the tier lifecycle reconciler that owns chunk-lifecycle
	// execution. All production deletes route through reconciler.deleteChunk
	// → CmdRequestDelete (gastrolog-51gme steps 4-7). Nil only in older test
	// harnesses that build TierInstances directly without going through
	// buildTierInstance; those harnesses fall through to the legacy
	// direct-delete path below (for cross-node propagation they wire
	// directChunkReplicator.DeleteChunk RPC fan-out separately).
	reconciler *VaultLifecycleReconciler

	// isLeader returns true if this node is the config leader for this tier.
	// Retention (expiry + transitions) only runs on the leader to prevent
	// all nodes from independently transitioning the same chunks.
	isLeader bool

	// followerTargets are the remote nodes that hold replicas of this tier's
	// chunks. Used to forward chunk deletions after retention expires them.
	followerTargets []system.ReplicationTarget

	now    func() time.Time
	logger *slog.Logger
}

type sweepTarget struct {
	runner *retentionRunner
	rules  []retentionRule
}

// retentionSweepAll is the single scheduled retention job. Runs on every node
// that hosts at least one tier instance.
//
// Per-tier role and readiness:
//   - Rule evaluation runs only when the tier is the leader (tier.IsLeader()).
//     Followers skip rule evaluation because rule results must be applied via
//     the vault control-plane Raft, which only the leader writes to.
//
// There is no per-vault Vault.ReadinessErr gate at this level because the
// per-tier IsLeader checks already cover the preconditions for each action.
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
		o.logger.Error("retention: failed to load config", "error", err)
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
		for _, tier := range vault.Tiers {
			// Only tier leaders evaluate retention rules.
			if !tier.IsLeader() {
				continue
			}
			if t := o.retentionTargetForTier(cfg, vaultCfg, tier, active); t != nil {
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

	// Leader: confirm streamed transitions. Chunks marked as
	// transitionStreamed have been streamed to the next tier but not yet
	// deleted — we wait for the destination to have adequate replicas
	// before expiring the source. See gastrolog-4913n.
	for _, t := range targets {
		t.runner.confirmStreamedTransitions(cfg)
	}

	// (Disk-vs-manifest orphan cleanup and missing-replica catchup are
	// done out-of-band on the tier-catchup sweep tick — see
	// tierCatchupSweepAll. The retention sweep stays focused on rule
	// evaluation and confirmStreamedTransitions on leaders only.)

	// Memory budget enforcement: transition oldest chunks when over budget.
	o.enforceMemoryBudgets(cfg)

	// Cache eviction: collect evictors under lock, run outside.
	// EvictCache does filesystem I/O — holding the lock would block Raft applies.
	var evictors []chunk.ChunkCacheEvictor
	o.mu.RLock()
	for _, vault := range o.vaults {
		for _, tier := range vault.Tiers {
			if evictor, ok := tier.Chunks.(chunk.ChunkCacheEvictor); ok {
				evictors = append(evictors, evictor)
			}
		}
	}
	o.mu.RUnlock()
	for _, evictor := range evictors {
		evictor.EvictCache()
	}
}

// tierCatchupSweepAll runs every 20 seconds (cron 13/33/53s, phase-
// offset from the retention sweep) on every node. For each (vault,
// tier) on this node it asks the lifecycle reconciler to run all
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
func (o *Orchestrator) tierCatchupSweepAll() {
	o.mu.RLock()
	tiers := make([]*VaultInstance, 0)
	for _, vault := range o.vaults {
		for _, t := range vault.Tiers {
			if t.Reconciler != nil {
				tiers = append(tiers, t)
			}
		}
	}
	o.mu.RUnlock()
	for _, t := range tiers {
		t.Reconciler.SweepPendingObligations()
		t.Reconciler.SweepLocalOrphans()
		t.Reconciler.SweepMissingReplicas()
		t.Reconciler.SweepStaleLeaderFSMEntries()
	}
}

// enforceMemoryBudgets checks memory tiers for budget overruns and transitions
// the oldest sealed chunks to the next tier. Only runs on leaders.
func (o *Orchestrator) enforceMemoryBudgets(cfg *system.Config) {
	if cfg == nil {
		return
	}
	type budgetTarget struct {
		vaultID glid.GLID
		tierID  glid.GLID
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
		for _, tier := range vault.Tiers {
			if !tier.IsLeader() {
				continue
			}
			monitor, ok := tier.Chunks.(chunk.ChunkBudgetMonitor)
			if !ok {
				continue
			}
			if excess := monitor.BudgetExceeded(); excess > 0 {
				targets = append(targets, budgetTarget{
					vaultID: vaultCfg.ID,
					tierID:  tier.TierID,
					cm:      tier.Chunks,
					excess:  excess,
				})
			}
		}
	}
	o.mu.RUnlock()

	for _, t := range targets {
		o.drainExcessChunks(t.vaultID, t.tierID, t.cm, t.excess)
	}
}

// drainExcessChunks transitions the oldest sealed chunks from a memory tier
// until the excess bytes are reclaimed (or no more sealed chunks remain).
func (o *Orchestrator) drainExcessChunks(vaultID, tierID glid.GLID, cm chunk.ChunkManager, excess int64) {
	metas, err := cm.List()
	if err != nil {
		return
	}

	// Sort oldest first (by WriteStart).
	slices.SortFunc(metas, func(a, b chunk.ChunkMeta) int {
		return a.WriteStart.Compare(b.WriteStart)
	})

	// Find the index manager for this tier.
	var im index.IndexManager
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil {
		for _, tier := range vault.Tiers {
			if tier.TierID == tierID {
				im = tier.Indexes
				break
			}
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
			tierID:   tierID,
			cm:       cm,
			im:       im,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		runner.transitionChunk(m.ID)
		reclaimed += m.Bytes
	}

	if reclaimed > 0 {
		o.logger.Info("memory budget enforcement: transitioned chunks",
			"vault", vaultID, "tier", tierID,
			"excess", excess, "reclaimed", reclaimed)
	}
}

// UnreadableChunks returns chunk IDs currently flagged as unreadable
// across all tier-instance retention runners for the vault. Used by
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
// across all tier-instance retention runners for the vault, so the
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

// tryEventDrivenExpire is invoked from a source-tier reconciler's
// onTransitionStreamed callback (run on a goroutine to avoid the
// apply-pump deadlock). The streamed flag was just committed for
// chunkID on the (vaultID, tierID) source. If the destination has
// already acked the receipt, expire the source immediately instead
// of waiting for the next retention sweep tick. Common case path —
// streaming and the receipt apply often complete within the same
// Raft round-trip burst. See gastrolog-1g6br.
func (o *Orchestrator) tryEventDrivenExpire(vaultID, tierID glid.GLID, chunkID chunk.ChunkID) {
	o.mu.RLock()
	var runner *retentionRunner
	for _, r := range o.retention {
		if r.vaultID == vaultID && r.tierID == tierID {
			runner = r
			break
		}
	}
	o.mu.RUnlock()
	if runner == nil {
		return
	}
	sys, err := o.loadSystem(context.Background())
	if err != nil || sys == nil {
		return
	}
	runner.confirmStreamedOne(&sys.Config, chunkID)
}

// notifyReceiptConfirmed is invoked from a destination-tier
// reconciler's onTransitionReceived callback. The receipt apply just
// committed for sourceChunkID. Find every source-tier runner in the
// same vault and trigger their event-driven confirm-and-expire path —
// the runner that holds the chunk in transitionStreamed state will
// expire it; others bail cheaply. See gastrolog-1g6br.
func (o *Orchestrator) notifyReceiptConfirmed(vaultID glid.GLID, sourceChunkID chunk.ChunkID) {
	o.mu.RLock()
	candidates := make([]*retentionRunner, 0)
	for _, r := range o.retention {
		if r.vaultID == vaultID {
			candidates = append(candidates, r)
		}
	}
	o.mu.RUnlock()
	if len(candidates) == 0 {
		return
	}
	sys, err := o.loadSystem(context.Background())
	if err != nil || sys == nil {
		return
	}
	for _, r := range candidates {
		r.confirmStreamedOne(&sys.Config, sourceChunkID)
	}
}

// RetentionPendingChunks returns chunk IDs marked as retention-pending in the
// tier FSM for a vault. Visible to all nodes via Raft replication.
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
	for _, tier := range vault.Tiers {
		if tier.ListRetentionPending != nil {
			for _, id := range tier.ListRetentionPending() {
				result[id] = true
			}
		}
	}
	return result
}

// PendingDeleteAcks returns, for each chunk currently in any tier's
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
	for _, tier := range vault.Tiers {
		if tier.Reconciler == nil || tier.Reconciler.fsm == nil {
			continue
		}
		for _, p := range tier.Reconciler.fsm.PendingDeletes() {
			expected := make([]string, 0, len(p.ExpectedFrom))
			for nodeID := range p.ExpectedFrom {
				expected = append(expected, nodeID)
			}
			result[p.ChunkID] = expected
		}
	}
	return result
}

// TransitionStreamedChunks returns chunk IDs on source tiers where records
// were streamed to the next tier but the local copy is not yet deleted
// (awaiting destination receipt). See gastrolog-4913n.
//
// Read-only accessor, callable from any-node. No Vault.ReadinessErr gate —
// results reflect whatever the local FSM has applied, which is acceptable for
// inspector/observational callers. For any path that makes decisions based
// on this set, gate on Vault.ReadinessErr first.
func (o *Orchestrator) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID]bool)
	for _, tier := range vault.Tiers {
		if tier.ListTransitionStreamed != nil {
			for _, id := range tier.ListTransitionStreamed() {
				result[id] = true
			}
		}
	}
	return result
}

// retentionTargetForTier resolves a single tier instance into a sweep target.
// Returns nil if the tier should be skipped (no rules, no leader, etc.).
func (o *Orchestrator) retentionTargetForTier(cfg *system.Config, vaultCfg system.VaultConfig, tier *VaultInstance, active map[string]bool) *sweepTarget {
	if tier.HasRaftLeader != nil && !tier.HasRaftLeader() {
		return nil
	}
	// IsRaftLeader check removed: the tier apply forwarder transparently
	// routes applies to the vault-ctl Raft leader. The config placement leader
	// always runs retention regardless of vault-ctl Raft leadership.
	tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
	if tierCfg == nil || len(tierCfg.RetentionRules) == 0 {
		return nil
	}
	rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
	if err != nil {
		o.logger.Warn("retention: failed to resolve rules",
			"vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return nil
	}
	if len(rules) == 0 {
		return nil
	}

	key := retentionKey(tier.TierID, tier.StorageID)
	active[key] = true

	runner := o.retention[key]
	if runner == nil {
		runner = &retentionRunner{
			vaultID: vaultCfg.ID,
			tierID:  tier.TierID,
			cm:      tier.Chunks,
			im:      tier.Indexes,
			orch:    o,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[key] = runner
	}
	runner.cm = tier.Chunks
	runner.im = tier.Indexes
	runner.applyRaftRetentionPending = tier.ApplyRaftRetentionPending
	runner.applyRaftTransitionStreamed = tier.ApplyRaftTransitionStreamed
	runner.reconciler = tier.Reconciler
	runner.isLeader = tier.IsLeader()
	runner.followerTargets = tier.FollowerTargets
	runner.vaultName = vaultCfg.Name
	runner.tierType = string(tierCfg.Type)
	runner.tierPosition = tierPositionInVault(cfg, vaultCfg.ID, tier.TierID)
	return &sweepTarget{runner: runner, rules: rules}
}

// tierPositionInVault returns the 0-based index of tierID in the vault's
// ordered tier list, or -1 if the tier isn't found (shouldn't happen for
// an active sweep target).
func tierPositionInVault(cfg *system.Config, vaultID, tierID glid.GLID) int {
	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultID)
	for i, id := range tierIDs {
		if id == tierID {
			return i
		}
	}
	return -1
}

// (Disk-vs-manifest orphan cleanup lives on VaultLifecycleReconciler now —
// see SweepLocalOrphans. It is tombstone-aware: only chunks the FSM has
// positively confirmed as finalize-deleted are eligible for cleanup, so
// freshly-created chunks with announce in flight are never racey-deleted.)

// sweep evaluates retention rules on a leader and applies expire/eject/transition.
func (r *retentionRunner) sweep(rules []retentionRule) {
	// Only the config placement leader runs retention. Raft applies are
	// forwarded transparently to the Raft leader via TierApplyForwarder.
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
	tier := r.findTierInstance()
	r.mu.Unlock()

	// Build a set of chunks awaiting destination replication confirmation
	// so we don't re-stream them on every sweep. See gastrolog-4913n.
	streamed := make(map[chunk.ChunkID]bool)
	if tier != nil && tier.ListTransitionStreamed != nil {
		for _, id := range tier.ListTransitionStreamed() {
			streamed[id] = true
		}
	}

	// Build a set of chunks already flagged retention-pending in the FSM.
	// We pass this down so tryRetainChunk skips the redundant
	// CmdRetentionPending Apply on chunks where the flag is already set,
	// which is critical when retention actions stall (transition
	// unreachable destination, receipt-protocol stuck) and the same
	// chunk gets re-evaluated every minute. See gastrolog-51gme.
	pendingFlag := make(map[chunk.ChunkID]bool)
	if tier != nil && tier.ListRetentionPending != nil {
		for _, id := range tier.ListRetentionPending() {
			pendingFlag[id] = true
		}
	}

	manifest, manifestKnown := buildManifestSet(tier)

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
		for _, id := range matched {
			if processed[id] {
				continue
			}
			processed[id] = true
			r.tryRetainChunk(id, b, pendingFlag[id])
		}
	}
}

// buildManifestSet returns the FSM-known chunk IDs for the given tier and a
// flag indicating whether the manifest is queryable. Any chunk on disk whose
// ID is NOT in the manifest is a ghost — its FSM entry was finalize-deleted
// but the disk file was never reaped. Filtering ghosts out of the retention
// sweep prevents repeated no-op transitions (the apply silently no-ops when
// f.chunks[id] is nil, the flag never sticks, and we re-stream the chunk's
// records to the next tier on every sweep). See gastrolog-66b7x.
func buildManifestSet(tier *VaultInstance) (map[chunk.ChunkID]bool, bool) {
	manifest := make(map[chunk.ChunkID]bool)
	if tier == nil || tier.ListManifest == nil {
		return manifest, false
	}
	ids := tier.ListManifest()
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
		if err := r.applyRaftRetentionPending(id); err != nil {
			r.logger.Error("retention: failed to apply raft retention-pending",
				"vault", r.vaultID, "chunk", id, "error", err)
			r.clearInflight(id)
			return
		}
		// Notify: retention_pending flag changed — inspector shows this.
		if r.orch != nil {
			r.orch.NotifyChunkChange()
		}
	}

	switch b.action {
	case system.RetentionActionExpire:
		defer r.clearInflight(id)
		r.expireChunk(id, "retention-ttl")
	case system.RetentionActionEject:
		defer r.clearInflight(id)
		r.ejectChunk(id, b.ejectRouteIDs)
	case system.RetentionActionTransition:
		// Transitions can be slow (streaming large chunks to slow tiers).
		// Run as a one-shot scheduler job so the sweep isn't blocked.
		// The inflight guard stays set until the job completes — the
		// sweep won't re-schedule the same chunk.
		r.scheduleTransition(id)
	default:
		r.clearInflight(id)
		r.logger.Error("retention: unknown action", "vault", r.vaultID, "action", b.action)
	}
}

// scheduleTransition dispatches a tier transition as a one-shot scheduler job
// so that slow transitions don't block the retention sweep from processing
// other chunks. The inflight guard is cleared when the job completes.
func (r *retentionRunner) scheduleTransition(id chunk.ChunkID) {
	if r.orch == nil {
		// No orchestrator (test mode) — run inline.
		defer r.clearInflight(id)
		r.transitionChunk(id)
		return
	}
	name := fmt.Sprintf("transition:%s:%s:%s", r.vaultID, r.tierID, id)
	if err := r.orch.scheduler.RunOnce(name, func() {
		defer r.clearInflight(id)
		r.transitionChunk(id)
	}); err != nil {
		r.clearInflight(id)
		r.logger.Warn("retention: failed to schedule transition",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
	}
	r.orch.scheduler.Describe(name, r.transitionJobDescription(id))
}

// transitionJobDescription formats a transition job's description for the
// Jobs inspector, including vault name and tier position/type so concurrent
// transitions from different vaults or tiers are distinguishable.
func (r *retentionRunner) transitionJobDescription(id chunk.ChunkID) string {
	vaultName := r.vaultName
	if vaultName == "" {
		vaultName = r.vaultID.String()
	}
	if r.tierPosition < 0 || r.tierType == "" {
		return fmt.Sprintf("Transition chunk %s in %q to next tier", id, vaultName)
	}
	return fmt.Sprintf("Transition chunk %s in %q tier %d (%s) to next tier",
		id, vaultName, r.tierPosition, r.tierType)
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
			// Per-tier rate alert (see gastrolog-47qyw). Only counted on
			// the leader path (this function only runs on tier leaders)
			// so the rate reflects active expiration decisions, not
			// follower delete-cascade applications.
			r.orch.retentionRates.Record(r.tierID, r.orch.now())
		}
		r.logger.Debug("retention: requested chunk delete via reconciler",
			"vault", r.vaultID, "chunk", id.String(), "reason", reason)
		return
	}

	// Reconciler-less fallback: legacy direct-delete path.
	//
	// Reached only by older test harnesses that build a retentionRunner
	// without going through buildTierInstance (so tier.Reconciler is nil).
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
			r.orch.retentionRates.Record(r.tierID, r.orch.now())
		}
		r.orch.NotifyChunkChange()
		r.orch.deleteFromFollowers(r.vaultID, r.tierID, id)
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

// sendDeleteToFollower issues a single chunk-delete RPC via the tier
// replicator. Returns nil when no replicator is configured (single-node mode).
func (r *retentionRunner) sendDeleteToFollower(followerID string, id chunk.ChunkID) error {
	if r.orch.chunkReplicator == nil {
		return nil
	}
	return r.orch.chunkReplicator.DeleteChunk(
		context.Background(), followerID, r.vaultID, r.tierID, id,
	)
}
