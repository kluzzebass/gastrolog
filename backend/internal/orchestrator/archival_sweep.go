package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

const (
	archivalSweepJobName     = "archival-sweep"
	archivalSweepSchedule    = "0 * * * *" // every hour, at minute 0
	reconcileSweepJobName    = "cloud-reconcile"
	defaultReconcileSchedule = "0 3 * * *" // daily at 3 AM
	defaultSuspectGraceDays  = 7
)

// suspectTracker records when cloud chunks were first observed missing.
// In-memory only — resets on restart (conservative: forces re-verification).
type suspectTracker struct {
	mu      sync.Mutex
	entries map[chunk.ChunkID]time.Time // chunkID → first seen missing
}

func newSuspectTracker() *suspectTracker {
	return &suspectTracker{entries: make(map[chunk.ChunkID]time.Time)}
}

func (s *suspectTracker) mark(id chunk.ChunkID, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[id]; !ok {
		s.entries[id] = now
	}
}

func (s *suspectTracker) clear(id chunk.ChunkID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, id)
}

func (s *suspectTracker) suspectSince(id chunk.ChunkID) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.entries[id]
	return t, ok
}

// startArchivalSweep registers the hourly archival sweep job.
func (o *Orchestrator) startArchivalSweep() error {
	if err := o.scheduler.AddJob(archivalSweepJobName, archivalSweepSchedule, o.archivalSweepAll); err != nil {
		return err
	}
	o.scheduler.Describe(archivalSweepJobName, "Archive cloud chunks per lifecycle policy")
	return nil
}

// archivalSweepAll is the hourly job that transitions cloud chunks to
// archive storage classes based on age and the cloud service's transition chain.
func (o *Orchestrator) archivalSweepAll() {
	sys, err := o.loadSystem(context.Background())

	if err != nil || sys == nil {
		return
	}

	// Build map of active cloud services (archivalMode == "active").
	activeCS := make(map[glid.GLID]*system.CloudService)
	for i := range sys.Config.CloudServices {
		cs := &sys.Config.CloudServices[i]
		if cs.ArchivalMode == "active" && len(cs.Transitions) > 0 {
			activeCS[cs.ID] = cs
		}
	}
	if len(activeCS) == 0 {
		return
	}

	now := o.now()

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vaultCfg := range sys.Config.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			if !tier.IsLeader() {
				continue
			}
			tierCfg := findTierConfig(sys.Config.Tiers, tier.TierID)
			if tierCfg == nil || tierCfg.CloudServiceID == nil {
				continue
			}
			cs, ok := activeCS[*tierCfg.CloudServiceID]
			if !ok {
				continue
			}
			o.archivalSweepTier(tier, cs, now)
		}
	}
}

// archivalSweepTier evaluates one tier's cloud chunks against the transition chain.
func (o *Orchestrator) archivalSweepTier(tier *VaultInstance, cs *system.CloudService, now time.Time) {
	metas, err := tier.Chunks.List()
	if err != nil {
		o.logger.Warn("archival sweep: list chunks failed", "tier", tier.TierID, "error", err)
		return
	}

	archiver, ok := tier.Chunks.(chunk.ChunkArchiver)
	if !ok {
		return
	}

	for _, m := range metas {
		if !m.Sealed || !m.CloudBacked {
			continue
		}

		age := now.Sub(m.WriteEnd)
		if age < 0 {
			continue
		}

		target := resolveTransitionTarget(cs.Transitions, age)
		if target == nil {
			continue
		}

		if target.StorageClass == "" {
			o.archivalExpire(tier, m.ID, age)
			continue
		}

		// Skip if already at the target class.
		if m.StorageClass == target.StorageClass {
			continue
		}

		if err := archiver.ArchiveChunk(context.Background(), m.ID, target.StorageClass); err != nil {
			o.logger.Warn("archival sweep: archive failed",
				"chunk", m.ID.String(), "class", target.StorageClass, "error", err)
		} else {
			o.logger.Debug("archival sweep: archived chunk",
				"chunk", m.ID.String(), "class", target.StorageClass, "age", age)
		}
	}
}

// archivalExpire deletes a cloud chunk that has aged past its lifecycle's
// terminal "delete" transition. Routes through the receipt protocol when a
// reconciler is wired (every node drops its index entry symmetrically);
// falls back to the local Manager.Delete path for memory-mode tiers without
// Raft. See gastrolog-51gme step 6.
func (o *Orchestrator) archivalExpire(tier *VaultInstance, id chunk.ChunkID, age time.Duration) {
	if tier.Reconciler != nil {
		if err := tier.Reconciler.deleteChunk(id, "archived-to-glacier", o.placementMembership(tier)); err != nil {
			o.logger.Warn("archival sweep: reconciler delete failed",
				"chunk", id.String(), "error", err)
			return
		}
		o.logger.Info("archival sweep: expired chunk",
			"chunk", id.String(), "age", age)
		return
	}
	if err := tier.Chunks.Delete(id); err != nil {
		o.logger.Warn("archival sweep: delete failed",
			"chunk", id.String(), "error", err)
		return
	}
	o.logger.Info("archival sweep: expired chunk",
		"chunk", id.String(), "age", age)
}

// resolveTransitionTarget finds the highest-matching transition for a chunk's age.
// Parses each transition's After duration and returns the last one the age exceeds.
// Returns nil if no transition applies yet.
func resolveTransitionTarget(transitions []system.CloudStorageTransition, age time.Duration) *system.CloudStorageTransition {
	var best *system.CloudStorageTransition
	for i := range transitions {
		threshold, err := system.ParseDuration(transitions[i].After)
		if err != nil {
			continue // skip unparseable
		}
		if age >= threshold {
			best = &transitions[i]
		}
	}
	return best
}

// startReconcileSweep registers the daily cloud reconciliation job.
func (o *Orchestrator) startReconcileSweep() error {
	if err := o.scheduler.AddJob(reconcileSweepJobName, defaultReconcileSchedule, o.reconcileSweepAll); err != nil {
		return err
	}
	o.scheduler.Describe(reconcileSweepJobName, "Reconcile cloud index vs blob store")
	return nil
}

// reconcileSweepAll compares cloud index entries against actual blobs in the store.
// Only the reconciliation sweep removes cloud index entries — never the hot path.
func (o *Orchestrator) reconcileSweepAll() {
	sys, err := o.loadSystem(context.Background())

	if err != nil || sys == nil {
		return
	}

	now := o.now()

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vaultCfg := range sys.Config.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			if !tier.IsLeader() {
				continue
			}
			tierCfg := findTierConfig(sys.Config.Tiers, tier.TierID)
			if tierCfg == nil || tierCfg.CloudServiceID == nil {
				continue
			}
			cs := findCloudService(&sys.Config, *tierCfg.CloudServiceID)
			if cs == nil {
				continue
			}
			o.reconcileTier(tier, cs, now)
		}
	}
}

// reconcileTier checks one tier's cloud chunks against the blob store.
func (o *Orchestrator) reconcileTier(tier *VaultInstance, cs *system.CloudService, now time.Time) {
	metas, err := tier.Chunks.List()
	if err != nil {
		return
	}

	graceDays := cs.SuspectGraceDays
	if graceDays == 0 {
		graceDays = defaultSuspectGraceDays
	}

	for _, m := range metas {
		if !m.CloudBacked {
			continue
		}
		// Skip chunks our own retention just deleted. The blob is gone by
		// design — reading it would 404, we'd mark it suspect, and the
		// operator would see a flood of alerts for a chunk WE deleted.
		// Tombstone propagation is faster than local cloud-index purge,
		// so checking the tombstone here catches the gap. See
		// gastrolog-2c96i.
		if tier.IsTombstoned != nil && tier.IsTombstoned(m.ID) {
			continue
		}
		o.reconcileCloudChunk(tier, m.ID, graceDays, now)
	}
}

// reconcileCloudChunk probes one cloud chunk against the blob store and
// advances its suspect-tracking state. Uses HeadCloudBlob so the probe hits
// the authoritative copy in S3 — OpenCursor would happily serve from the
// in-tree warm cache (gastrolog-24m1t step 7j) and miss out-of-band lifecycle
// deletions. Falls back to OpenCursor for managers that don't implement
// CloudBlobChecker (no cloud store configured / non-file backends).
func (o *Orchestrator) reconcileCloudChunk(tier *VaultInstance, id chunk.ChunkID, graceDays uint32, now time.Time) {
	var readErr error
	if checker, ok := tier.Chunks.(chunk.CloudBlobChecker); ok {
		readErr = checker.HeadCloudBlob(id)
	} else {
		cursor, err := tier.Chunks.OpenCursor(id)
		if err == nil {
			_ = cursor.Close()
		}
		readErr = err
	}
	if readErr == nil {
		o.clearSuspect(id)
		return
	}
	if !isChunkSuspect(readErr) {
		return // transient error or archived — not a missing blob
	}
	if o.suspects == nil {
		return
	}

	since, wasSuspect := o.suspects.suspectSince(id)
	if !wasSuspect {
		o.markSuspect(tier, id, now)
		return
	}

	suspectDays := uint32(now.Sub(since).Hours() / 24)
	if suspectDays < graceDays {
		o.logger.Info("reconcile: chunk still suspect",
			"tier", tier.TierID, "chunk", id.String(),
			"suspectDays", suspectDays, "graceDays", graceDays)
		return
	}
	o.expireSuspect(tier, id, suspectDays)
}

// clearSuspect drops any suspect bookkeeping for a chunk that just proved
// readable.
func (o *Orchestrator) clearSuspect(id chunk.ChunkID) {
	if o.suspects != nil {
		o.suspects.clear(id)
	}
	if o.alerts != nil {
		o.alerts.Clear("chunk-suspect:" + id.String())
	}
}

// markSuspect records a first-time missing-blob observation.
func (o *Orchestrator) markSuspect(tier *VaultInstance, id chunk.ChunkID, now time.Time) {
	o.suspects.mark(id, now)
	if o.alerts != nil {
		o.alerts.Set(
			"chunk-suspect:"+id.String(),
			1, // Warning
			"cloud-reconcile",
			"Cloud chunk "+id.String()+" not found in blob store — monitoring",
		)
	}
	o.logger.Warn("reconcile: chunk not found, marking suspect",
		"tier", tier.TierID, "chunk", id.String())
}

// expireSuspect removes a chunk from the index after its grace period has
// elapsed without the blob reappearing. Routes through the receipt protocol
// when a reconciler is wired (every node drops its index entry symmetrically);
// falls back to the local Manager.Delete path for memory-mode tiers without
// Raft. See gastrolog-51gme step 6.
func (o *Orchestrator) expireSuspect(tier *VaultInstance, id chunk.ChunkID, suspectDays uint32) {
	if tier.Reconciler != nil {
		if err := tier.Reconciler.deleteChunk(id, "cloud-blob-missing", o.placementMembership(tier)); err != nil {
			o.logger.Error("reconcile: reconciler delete failed",
				"tier", tier.TierID, "chunk", id.String(), "error", err)
			return
		}
	} else if err := tier.Chunks.Delete(id); err != nil {
		o.logger.Error("reconcile: failed to remove suspect chunk from index",
			"tier", tier.TierID, "chunk", id.String(), "error", err)
		return
	}
	o.suspects.clear(id)
	if o.alerts != nil {
		o.alerts.Set(
			"chunk-suspect:"+id.String(),
			2, // Error
			"cloud-reconcile",
			fmt.Sprintf("Cloud chunk %s removed from index after %d days missing", id, suspectDays),
		)
	}
	o.logger.Warn("reconcile: removed chunk from index after grace period",
		"tier", tier.TierID, "chunk", id.String(), "suspectDays", suspectDays)
}

// isChunkSuspect returns true if the error indicates a 404 (blob not found).
func isChunkSuspect(err error) bool {
	return errors.Is(err, chunk.ErrChunkSuspect)
}
