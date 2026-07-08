package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// glcbPullTimeout bounds one replica pull; GLCBs are hundreds of MB and the
// pull streams to disk, so this is generous without letting a dead peer pin
// the claim forever.
const glcbPullTimeout = 5 * time.Minute

// maxConcurrentGLCBPulls bounds in-flight replica pulls across all vaults.
// A home recovering a large backlog (hundreds of chunks) tops up from the
// 20s catch-up sweep instead of saturating disk and peer bandwidth at once.
const maxConcurrentGLCBPulls = 4

// pullMissingGLCB schedules recovery of this home's copy of a sealed chunk
// whose GLCB is absent from local disk: pull from a peer home, verify the
// assembled file's seal metadata against the manifest entry, rename into
// place, register. Pipeline vaults release source segments after the build
// window, so a home that missed the build (wedged, down) has no other path
// back to holding its replica — without this, retention starves forever
// while the manifest still reports the home as a holder.
//
// No-op when the file exists, the vault has no pipeline home registration
// here, a pull for the chunk is already in flight, or the in-flight cap is
// reached (the next sweep tick retries). The pull itself runs as a one-time
// scheduler job so the sweep never blocks behind a slow peer.
func (o *Orchestrator) pullMissingGLCB(vaultID glid.GLID, e vaultctlfsm.ManifestEntry) {
	if o.chunkGLCBPuller == nil || o.scheduler == nil {
		return // single-node: every chunk this node should hold, it built
	}
	if o.diskProtectActive() {
		return // below the free-space floor: recovery writes wait for space
	}
	if o.diskGuard != nil && o.diskGuard.vaultProtectActive(vaultID) {
		return // this vault's own backing volume is below its floor
	}
	if o.diskGuard != nil && o.diskGuard.vaultSizeCapped(vaultID) {
		return // at the max-size budget: replica pulls grow the local claim
	}
	root, ok := o.pipelineVaultChunkRoot(vaultID)
	if !ok {
		return
	}
	glcbPath := chunking.ChunkGLCBPath(root, e.ID)
	if _, err := os.Stat(glcbPath); err == nil {
		return // bytes present; registration is the sweep's other half
	}
	if o.retentionMootsPull(vaultID, e) {
		// Already past its age window with a destroy-only disposition:
		// recovery would stream hundreds of MB for a faster funeral, and
		// the failed attempts raced retention as log noise during backlog
		// recovery. Route-disposition vaults never take this branch — the
		// leader fans records out from its LOCAL copy before destruction,
		// so a leader missing bytes must still pull or the chunk would be
		// destroyed unrouted.
		if n, ok := o.registerSkipLog.Allow(vaultID.String() + ":pull-moot"); ok {
			o.logger.Debug("GLCB replica pull skipped — chunk already past its retention window",
				"vault", vaultID, "chunk", e.ID, "sealedAt", e.SealedAt, "suppressed", n)
		}
		return
	}

	o.glcbPullMu.Lock()
	if o.glcbPullInflight == nil {
		o.glcbPullInflight = make(map[chunk.ChunkID]bool)
	}
	if o.glcbPullInflight[e.ID] || len(o.glcbPullInflight) >= maxConcurrentGLCBPulls {
		o.glcbPullMu.Unlock()
		return
	}
	o.glcbPullInflight[e.ID] = true
	o.glcbPullMu.Unlock()

	name := fmt.Sprintf("glcb-catchup:%s:%s", vaultID, e.ID)
	err := o.scheduler.RunOnce(name, func() {
		defer func() {
			o.glcbPullMu.Lock()
			delete(o.glcbPullInflight, e.ID)
			o.glcbPullMu.Unlock()
		}()
		o.runGLCBPull(vaultID, e, glcbPath)
	})
	if err != nil {
		o.glcbPullMu.Lock()
		delete(o.glcbPullInflight, e.ID)
		o.glcbPullMu.Unlock()
		return
	}
	o.scheduler.Describe(name, fmt.Sprintf("Replica catch-up pull of chunk %s from a peer home", e.ID))
}

// runGLCBPull tries each peer home in placement order until one supplies a
// verified GLCB, then registers the chunk so query and retention see it.
func (o *Orchestrator) runGLCBPull(vaultID glid.GLID, e vaultctlfsm.ManifestEntry, glcbPath string) {
	sources := o.glcbPullSources(vaultID)
	if len(sources) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), glcbPullTimeout)
	defer cancel()

	var lastErr error
	for _, node := range sources {
		if err := o.pullGLCBFromNode(ctx, node, vaultID, e, glcbPath); err != nil {
			lastErr = err
			continue
		}
		o.logger.Info("GLCB replica recovered from peer",
			"vault", vaultID, "chunk", e.ID, "from", node)
		o.mu.RLock()
		var rec *VaultLifecycleReconciler
		if vault := o.vaults[vaultID]; vault != nil && vault.Instance != nil {
			rec = vault.Instance.Reconciler
		}
		o.mu.RUnlock()
		if rec != nil {
			rec.registerPipelineGLCB(e)
		}
		return
	}
	// Every peer failed. Transient causes (peer down, connection warming)
	// heal on the next sweep tick; a chunk NO home can supply is a durability
	// incident that shows up here repeatedly until someone restores a copy.
	if n, ok := o.registerSkipLog.Allow(vaultID.String() + ":glcb-pull"); ok {
		o.logger.Warn("GLCB replica pull failed from every peer",
			"vault", vaultID, "chunk", e.ID, "peers", len(sources),
			"error", lastErr, "suppressed", n)
	}
}

// pullGLCBFromNode streams one GLCB to a temp file next to its final
// location (same convention as BuildGLCBFile's dot-prefixed temps, which
// sweeps ignore), verifies, and promotes.
func (o *Orchestrator) pullGLCBFromNode(ctx context.Context, node string, vaultID glid.GLID, e vaultctlfsm.ManifestEntry, glcbPath string) error {
	if err := os.MkdirAll(filepath.Dir(glcbPath), 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(glcbPath), ".glcb.pull.*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	pullErr := o.chunkGLCBPuller.Pull(ctx, node, vaultID, e.ID, f)
	if pullErr == nil {
		pullErr = f.Sync()
	}
	closeErr := f.Close()
	if pullErr == nil {
		pullErr = closeErr
	}
	if pullErr != nil {
		_ = os.Remove(tmp) //nolint:gosec // G703: CreateTemp name in our own chunk dir, not untrusted input
		return pullErr
	}
	return verifyAndPromoteGLCB(tmp, glcbPath, e)
}

// verifyAndPromoteGLCB checks a pulled blob against the cluster manifest
// entry and renames it into place. A torn or wrong blob must never be
// registered as a replica: the GLCB's own seal metadata must parse and its
// record count must agree with the manifest. The temp file is consumed —
// removed on any failure, renamed on success.
func verifyAndPromoteGLCB(tmp, glcbPath string, e vaultctlfsm.ManifestEntry) error {
	res, err := chunking.BuildResultFromExistingGLCB(tmp, e.SealedAt)
	if err != nil {
		_ = os.Remove(tmp) //nolint:gosec // G703: CreateTemp name in our own chunk dir, not untrusted input
		return fmt.Errorf("pulled GLCB failed verification: %w", err)
	}
	if int64(res.RecordCount) != e.RecordCount {
		_ = os.Remove(tmp) //nolint:gosec // G703: CreateTemp name in our own chunk dir, not untrusted input
		return fmt.Errorf("pulled GLCB record count %d != manifest %d", res.RecordCount, e.RecordCount)
	}
	return os.Rename(tmp, glcbPath) //nolint:gosec // G703: both paths derive from the local chunk root, not untrusted input
}

// retentionMootsPull reports whether pulling a missing GLCB is pointless:
// some age-based retention rule already has the chunk past its window AND
// the vault's disposition destroys without routing. Consulted from the
// catch-up sweep before scheduling a pull. Only TTL policies participate —
// size/count policies need whole-vault context and cannot be evaluated
// per-chunk. Any route-disposition runner vetoes the skip: route fan-out
// reads the leader's local copy, so bytes must be recoverable there.
func (o *Orchestrator) retentionMootsPull(vaultID glid.GLID, e vaultctlfsm.ManifestEntry) bool {
	if e.SealedAt.IsZero() {
		return false
	}
	o.mu.RLock()
	var runners []*retentionRunner
	for _, r := range o.retention {
		if r.vaultID == vaultID {
			runners = append(runners, r)
		}
	}
	o.mu.RUnlock()
	if len(runners) == 0 {
		return false
	}

	state := chunk.VaultState{
		Chunks: []chunk.ChunkMeta{{
			ID:       e.ID,
			Sealed:   true,
			State:    e.State,
			SealedAt: e.SealedAt,
			WriteEnd: e.WriteEnd,
		}},
		Now: o.now(),
	}
	expired := false
	for _, r := range runners {
		r.mu.Lock()
		rules := r.rules
		disposition := r.disposition
		r.mu.Unlock()
		if disposition != system.RetentionDispositionDelete {
			return false
		}
		for _, rule := range rules {
			ttl, ok := rule.policy.(*chunk.TTLRetentionPolicy)
			if !ok {
				continue
			}
			if len(ttl.Apply(state)) > 0 {
				expired = true
			}
		}
	}
	return expired
}

// glcbPullSources lists peer homes to try, placement order, self excluded.
func (o *Orchestrator) glcbPullSources(vaultID glid.GLID) []string {
	var out []string
	for _, n := range o.vaultPlacementNodeIDs(vaultID) {
		if n != "" && n != o.localNodeID {
			out = append(out, n)
		}
	}
	return out
}
