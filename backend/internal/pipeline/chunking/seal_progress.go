package chunking

import (
	"slices"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// buildKey identifies one sealed-manifest build cycle.
type buildKey struct {
	chunkID  chunk.ChunkID
	sealedAt time.Time
}

// cachedBuild caches the most recent GLCB build for seal retries without
// re-reading every segment on each wake/tick while sealedManifest is pending.
type cachedBuild struct {
	key    buildKey
	result BuildResult
	ok     bool
}

// sealProgress is the per-vault exactly-once state machine for the sealed-
// manifest lifecycle: per build cycle the GLCB is built once, CmdSealChunk
// is proposed once, post-seal purge/release work runs once, and OnBuilt fires
// once. It owns its lock; every once-guard has exactly one definition here.
// Guards are single slots, not sets, because at most one sealed manifest is
// in flight per vault at a time.
type sealProgress struct {
	mu sync.Mutex
	// built marks the cycle whose GLCB this home has materialized.
	built buildKey
	// sealProposed stops vault-ctl CmdSealChunk replays after the first
	// successful Apply for a sealed manifest; without it every wake/tick on
	// every home floods Raft while sealedManifest is still pending locally.
	sealProposed buildKey
	// postSeal marks head-purge + release-queue work CLAIMED for a sealed
	// manifest so seal retries do not re-purge or re-enqueue segments. The
	// claim is won at entry but the work runs after it; postSealDoneCh closes
	// when the claimant finishes, so a caller refused the claim can wait for
	// completion instead of returning while the purge is still in flight
	// (gastrolog-4cxvdi).
	postSeal       buildKey
	postSealDoneCh chan struct{}
	// onBuiltFired ensures OnBuilt fires once per sealed manifest build.
	onBuiltFired buildKey
	cached       cachedBuild
	// pending is a copy of the sealed manifest retained after CmdSealChunk
	// clears sealedManifest cluster-wide so follower homes can still build.
	pending *vaultctlfsm.OpenChunkManifest
}

// alreadyBuilt reports whether the GLCB for this cycle is materialized locally.
func (p *sealProgress) alreadyBuilt(key buildKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.built == key
}

// markBuilt records a completed (or adopted) GLCB build and caches its result.
func (p *sealProgress) markBuilt(key buildKey, result BuildResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.built = key
	p.cached = cachedBuild{key: key, result: result, ok: true}
}

// cachedResult returns the prior build output for this cycle, if any.
func (p *sealProgress) cachedResult(key buildKey) (BuildResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cached.ok || p.cached.key != key {
		return BuildResult{}, false
	}
	return p.cached.result, true
}

// shouldPropose reports whether CmdSealChunk has not yet been proposed for
// this cycle.
func (p *sealProgress) shouldPropose(key buildKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sealProposed != key
}

// markProposed records that CmdSealChunk was proposed for this cycle.
func (p *sealProgress) markProposed(key buildKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealProposed = key
}

// resetSealProposal clears the seal-proposed guard so the next build pass can
// propose CmdSealChunk again (leadership change, seal cleared, hot restore).
func (p *sealProgress) resetSealProposal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealProposed = buildKey{}
}

// resetSealProposalIf clears the seal-proposed guard only when it is set for
// this cycle, reporting whether it did. Atomic check-and-clear for the stale
// leader path.
func (p *sealProgress) resetSealProposalIf(key buildKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealProposed != key {
		return false
	}
	p.sealProposed = buildKey{}
	return true
}

// claimOnBuilt returns true exactly once per cycle; the caller that wins the
// claim fires OnBuilt.
func (p *sealProgress) claimOnBuilt(key buildKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.onBuiltFired == key {
		return false
	}
	p.onBuiltFired = key
	return true
}

// postSealDone reports whether post-seal purge/release work was claimed for
// this cycle (it may still be in flight on the claimant; afterSealBuild's
// refusal path waits for completion).
func (p *sealProgress) postSealDone(key buildKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.postSeal == key
}

// claimPostSeal wins the post-seal work slot exactly once per cycle, and only
// after markBuilt. OnSealedManifestCleared can fire on follower homes before
// local GLCB build finishes; refusing the claim until built keeps that early
// callback a no-op so finishBuildOnce can run the post-seal work later
// (gastrolog-3vlse).
//
// A caller refused because the cycle is already claimed receives the
// claimant's done channel (closed by finishPostSeal) so it can wait for the
// work to COMPLETE — a refused claim means "someone owns this", not "this is
// done", and treating the two as the same let BuildOnce return while the
// OnSealedManifestCleared-spawned claimant was still mid-purge
// (gastrolog-4cxvdi). The channel is nil when there is nothing to wait for
// (no claim exists for this cycle yet).
func (p *sealProgress) claimPostSeal(key buildKey) (bool, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.postSeal == key {
		return false, p.postSealDoneCh
	}
	if p.built != key {
		return false, nil
	}
	p.postSeal = key
	// A new cycle's claim releases any waiter still parked on a superseded
	// cycle's channel — that cycle's slot is gone; hanging its waiters
	// forever is worse than letting them proceed.
	closeDoneChLocked(p.postSealDoneCh)
	p.postSealDoneCh = make(chan struct{})
	return true, nil
}

// finishPostSeal marks the claimed post-seal work complete, releasing every
// caller waiting on this cycle's done channel. Claimant-only; deferred by
// afterSealBuild so the channel closes even on a panicking purge path.
func (p *sealProgress) finishPostSeal(key buildKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.postSeal != key {
		return
	}
	closeDoneChLocked(p.postSealDoneCh)
}

// closeDoneChLocked closes ch unless it is nil or already closed. Caller
// holds p.mu, so the check-then-close cannot race another closer.
func closeDoneChLocked(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// setPending records the sealed manifest awaiting build on this home.
func (p *sealProgress) setPending(m *vaultctlfsm.OpenChunkManifest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = m
}

// pendingManifest returns the retained sealed manifest, if any.
func (p *sealProgress) pendingManifest() *vaultctlfsm.OpenChunkManifest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}

// noteSealCleared handles CmdSealChunk applying cluster-wide: the seal-proposed
// and OnBuilt guards reset for the next cycle, and a copy of the retained
// pending manifest (nil when none) is returned for post-seal work. The pending
// manifest itself stays retained so follower homes that have not built yet
// still can.
func (p *sealProgress) noteSealCleared() *vaultctlfsm.OpenChunkManifest {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealProposed = buildKey{}
	p.onBuiltFired = buildKey{}
	if p.pending == nil {
		return nil
	}
	sealedCopy := *p.pending
	sealedCopy.Refs = slices.Clone(p.pending.Refs)
	return &sealedCopy
}

// clearPendingAfterBuilt drops the retained pending manifest once this home
// has built the cycle's GLCB. No-op when the chunk does not match or the
// build has not landed — pendingSeal must survive until then so
// OnSealedManifestCleared can run afterSealBuild on follower homes.
func (p *sealProgress) clearPendingAfterBuilt(chunkID chunk.ChunkID, key buildKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil || p.pending.ChunkID != chunkID || p.built != key {
		return
	}
	p.pending = nil
}
