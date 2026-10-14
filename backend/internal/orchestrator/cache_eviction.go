package orchestrator

// CacheEvictor is implemented by chunk managers that hold cached copies of
// cloud-backed chunks (chunk/file.Manager). The orchestrator's
// scheduled sweep calls EvictCache to apply whatever LRU + TTL policies
// the manager was configured with at construction time.
type CacheEvictor interface {
	EvictCache() (evicted int, freedBytes int64)
}

const (
	// cacheEvictionSchedule runs once per minute, second 23 — phase-offset
	// from retention (second 0) and instance-catchup (13/33/53s) so the sweeps
	// don't pile up on the same wall-clock tick.
	cacheEvictionSchedule = "23 * * * * *"
	cacheEvictionJobName  = "cache-eviction"
)

// cacheEvictionSweepAll fans out EvictCache across every vault instance on
// this node: every home holds local copies of cloud-backed chunks, so the
// cache policy applies on each one, not only on the placement leader.
// No-op for managers that don't implement CacheEvictor (memory-mode vaults)
// or that have no eviction policy configured (every EvictCache call
// short-circuits when CacheBudgetBytes and CacheTTL are both zero).
//
// An eviction that removed pipeline GLCBs leaves this home's holder receipts
// claiming bytes it no longer has; the reconciler's receipt pass withdraws
// them on this edge rather than at its next periodic tick.
func (o *Orchestrator) cacheEvictionSweepAll() {
	o.mu.RLock()
	type evictTarget struct {
		evictor    CacheEvictor
		reconciler *VaultLifecycleReconciler
		vaultID    string
	}
	var targets []evictTarget
	for _, v := range o.vaults {
		t := v.Instance
		if t == nil {
			continue
		}
		ev, ok := t.Chunks.(CacheEvictor)
		if !ok {
			continue
		}
		targets = append(targets, evictTarget{
			evictor:    ev,
			reconciler: t.Reconciler,
			vaultID:    v.ID.String(),
		})
	}
	o.mu.RUnlock()

	for _, tgt := range targets {
		evicted, freed := tgt.evictor.EvictCache()
		if evicted == 0 {
			continue
		}
		if o.logger != nil {
			o.cacheEvictionLogger.Debug("cache eviction sweep",
				"vault", tgt.vaultID,
				"evicted", evicted,
				"freed_bytes", freed)
		}
		if tgt.reconciler != nil {
			tgt.reconciler.SyncHolderReceipts()
		}
	}
}

// startCacheEvictionSweep registers the periodic cached-copy eviction
// sweep. Each tick walks every vault instance on this node and asks its
// chunk manager to apply its configured eviction policies.
func (o *Orchestrator) startCacheEvictionSweep() error {
	if err := o.scheduler.AddJob(cacheEvictionJobName, sweepCron(cacheEvictionSchedule), o.cacheEvictionSweepAll); err != nil {
		return err
	}
	o.scheduler.Describe(cacheEvictionJobName, "Cached-copy eviction (LRU + TTL on cloud-backed chunks)")
	return nil
}
