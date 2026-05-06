package orchestrator

// CacheEvictor is implemented by chunk managers that hold a warm cache of
// cloud-backed chunk content (chunk/file.Manager). The orchestrator's
// scheduled sweep calls EvictCache to apply whatever LRU + TTL policies
// the manager was configured with at construction time. See
// gastrolog-2idw8.
type CacheEvictor interface {
	EvictCache() (evicted int, freedBytes int64)
}

const (
	// cacheEvictionSchedule runs once per minute, second 23 — phase-offset
	// from retention (second 0) and tier-catchup (13/33/53s) so the sweeps
	// don't pile up on the same wall-clock tick.
	cacheEvictionSchedule = "23 * * * * *"
	cacheEvictionJobName  = "cache-eviction"
)

// cacheEvictionSweepAll fans out EvictCache across every chunk manager in
// the orchestrator. No-op for managers that don't implement CacheEvictor
// (memory-mode tiers) or that have no eviction policy configured (every
// EvictCache call short-circuits when CacheBudgetBytes and CacheTTL are
// both zero).
func (o *Orchestrator) cacheEvictionSweepAll() {
	o.mu.RLock()
	type evictTarget struct {
		evictor CacheEvictor
		vaultID string
		tierID  string
	}
	var targets []evictTarget
	for _, v := range o.vaults {
		t := v.Instance
		if t == nil || !t.IsLeader() {
			continue
		}
		ev, ok := t.Chunks.(CacheEvictor)
		if !ok {
			continue
		}
		targets = append(targets, evictTarget{
			evictor: ev,
			vaultID: v.ID.String(),
			tierID:  t.TierID.String(),
		})
	}
	o.mu.RUnlock()

	for _, tgt := range targets {
		evicted, freed := tgt.evictor.EvictCache()
		if evicted > 0 && o.logger != nil {
			o.logger.Debug("cache eviction sweep",
				"vault", tgt.vaultID,
				"tier", tgt.tierID,
				"evicted", evicted,
				"freed_bytes", freed)
		}
	}
}

// startCacheEvictionSweep registers the periodic warm-cache eviction
// sweep. Each tick walks every leader tier and asks its chunk manager to
// apply its configured eviction policies. See gastrolog-2idw8.
func (o *Orchestrator) startCacheEvictionSweep() error {
	if err := o.scheduler.AddJob(cacheEvictionJobName, cacheEvictionSchedule, o.cacheEvictionSweepAll); err != nil {
		return err
	}
	o.scheduler.Describe(cacheEvictionJobName, "Warm-cache eviction (LRU + TTL on cloud-backed chunks)")
	return nil
}

