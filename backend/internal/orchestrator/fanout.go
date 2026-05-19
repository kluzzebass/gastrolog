// Fan-out write coordinator for the FanOut WriteModel (gastrolog-2ujjh /
// gastrolog-5pn44). Implements W-of-N ack accounting with the live-
// Receiving membership de-escalation that closes the spurious-failure
// hole during multi-node drains.
//
// See docs/fan-out-data-plane-design.md § "W-of-N implementation: new
// primitive needed" for the full design rationale.

package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// NodeResult carries the outcome of a single per-Receiving-member
// write goroutine launched during a fan-out send. waitWOfN consumes
// these from a shared channel; producers send one per snapshot member
// (or never, if the goroutine never completes — waitWOfN tolerates
// that via the context deadline).
type NodeResult struct {
	NodeID string
	Err    error // nil = ack success
}

// ErrWOfNUnreachable means too many snapshot members failed (or were
// removed from Receiving) for the write to ever reach W acks. Wrapped
// errors retain the first failing peer's error for telemetry.
var ErrWOfNUnreachable = errors.New("fan-out: W-of-N unreachable")

// IsStillReceivingFn returns true iff nodeID is currently in the live
// Receiving set for the chunk being written. waitWOfN consults this
// only when a snapshot member fails or times out — a "failure" from a
// node that has since been removed via CmdRemoveReceiving is
// reclassified as "not required" rather than counted against the
// failure budget.
//
// The callback is invoked with the FSM state from the orchestrator
// the caller is running on — every orchestrator has a local copy (see
// project_orchestrator_pipeline / project_vault_ctl_raft_membership).
// So this is a cheap local read; no Raft round-trip.
type IsStillReceivingFn func(nodeID string) bool

// waitWOfN reads per-node outcomes from results until one of:
//   - acks ≥ effectiveW → return nil (success).
//   - acks + stillExpected < effectiveW → return ErrWOfNUnreachable.
//   - ctx.Err() != nil → return that.
//
// snapshot is the Receiving membership stamped at fan-out time
// (immutable for this write — that's the snapshot-at-fan-out semantics
// resolved in gastrolog-16msa). n is len(snapshot); w is the per-vault
// W-of-N policy resolved against n. results MUST be buffered with at
// least n slots so background goroutines don't block on send after
// waitWOfN returns.
//
// failureClassifier is the per-node de-escalation hook. When a
// snapshot member fails, waitWOfN calls failureClassifier(nodeID); if
// it returns false (the node has since left Receiving), the result is
// classified as "not required" — it lowers the effective W target by
// one (clamped at a 1-ack minimum so 0 acks is never durable). This
// closes the spurious-failure hole during multi-node drains: a peer
// that has legitimately left the new write quorum no longer pins the
// ack budget against it.
//
// Implementation note: the receiving channel should have capacity ≥ n
// so producer goroutines completing after waitWOfN returns don't
// block. Callers MUST own the goroutine lifecycle separately — this
// helper only reads results; it does not launch or cancel goroutines.
func waitWOfN(ctx context.Context, n, w int, results <-chan NodeResult, failureClassifier IsStillReceivingFn) error {
	if w <= 0 {
		// W = 0 is a trivial success; document for completeness even
		// though VaultConfig.WOfN validation should reject this.
		return nil
	}
	if w > n {
		// Impossible: validated at config time (see gastrolog-4xdvm).
		// Defensive guard for tests / future bugs.
		return fmt.Errorf("fan-out: W=%d > N=%d (invalid W-of-N config)", w, n)
	}
	state := &wofnState{n: n, w: w, classifier: failureClassifier}
	return state.run(ctx, results)
}

// wofnState bundles the running counters + parameters so the loop
// dispatch can stay readable. Holds no goroutine lifecycle —
// goroutines are owned by the caller; this is pure result-consumer
// state.
type wofnState struct {
	n          int
	w          int
	classifier IsStillReceivingFn

	acks        int
	failures    int
	notRequired int
	firstErr    error
}

// effectiveW is the running W target. Clamped at 1 so 0 acks is never
// durable even when every peer is de-escalated.
func (s *wofnState) effectiveW() int {
	eff := s.w - s.notRequired
	if eff < 1 {
		return 1
	}
	return eff
}

// verdict returns (done, err): done=true if the W-of-N decision is
// settled either way; err is non-nil only for failure. Called at the
// top of each loop iteration AND after the results channel closes.
func (s *wofnState) verdict() (bool, error) {
	if s.acks >= s.effectiveW() {
		return true, nil
	}
	stillExpected := s.n - s.acks - s.failures - s.notRequired
	if s.acks+stillExpected >= s.effectiveW() {
		return false, nil
	}
	if s.firstErr != nil {
		return true, fmt.Errorf("%w: acks=%d, failures=%d, not-required=%d, snapshot=%d, W=%d (effective=%d), first error: %w",
			ErrWOfNUnreachable, s.acks, s.failures, s.notRequired, s.n, s.w, s.effectiveW(), s.firstErr)
	}
	return true, fmt.Errorf("%w: acks=%d, failures=%d, not-required=%d, snapshot=%d, W=%d (effective=%d)",
		ErrWOfNUnreachable, s.acks, s.failures, s.notRequired, s.n, s.w, s.effectiveW())
}

// observe folds a single NodeResult into the running counters.
func (s *wofnState) observe(r NodeResult) {
	if r.Err == nil {
		s.acks++
		return
	}
	if s.classifier != nil && !s.classifier(r.NodeID) {
		s.notRequired++
		return
	}
	s.failures++
	if s.firstErr == nil {
		s.firstErr = fmt.Errorf("fan-out to %s: %w", r.NodeID, r.Err)
	}
}

// run consumes results until the verdict is decidable. Splits the
// channel-closed and context-cancelled branches out of the main loop
// to keep the cognitive complexity manageable.
func (s *wofnState) run(ctx context.Context, results <-chan NodeResult) error {
	for {
		if done, err := s.verdict(); done {
			return err
		}
		select {
		case <-ctx.Done():
			if s.firstErr != nil {
				return fmt.Errorf("%w: acks=%d before deadline; first error: %w", ctx.Err(), s.acks, s.firstErr)
			}
			return ctx.Err()
		case r, ok := <-results:
			if !ok {
				return s.verdictOnClose()
			}
			s.observe(r)
		}
	}
}

// verdictOnClose forces the final verdict when the results channel
// closes before the threshold. Re-checks against the final tally so a
// late ack that arrived just before close still counts.
func (s *wofnState) verdictOnClose() error {
	if s.acks >= s.effectiveW() {
		return nil
	}
	if s.firstErr != nil {
		return fmt.Errorf("%w: results channel closed with acks=%d (effective W=%d); first error: %w", ErrWOfNUnreachable, s.acks, s.effectiveW(), s.firstErr)
	}
	return fmt.Errorf("%w: results channel closed with acks=%d (effective W=%d)", ErrWOfNUnreachable, s.acks, s.effectiveW())
}

// fanOutAppend dispatches one record to every member of snapshot in
// parallel and resolves W-of-N via waitWOfN. The local-node member (if
// present in snapshot) appends to localCM directly; remote members go
// through o.chunkReplicator.AppendRecords.
//
// snapshot is the active chunk's Receiving set, captured at the moment
// the fan-out begins (snapshot-at-fan-out semantics, gastrolog-16msa).
// isStillReceiving is the failure-de-escalation hook — a snapshot
// member whose write fails but has since left Receiving is classified
// as "not required" rather than as a failure (gastrolog-5pn44
// challenge-me tweak #1).
//
// localCM is non-nil iff o.localNodeID is in snapshot. The caller
// (typically appendRecord under the placement lookup) is responsible
// for that pairing — fanOutAppend does NOT consult o.vaults to derive
// it, so it can be called from contexts that already hold o.mu or that
// have a pre-resolved chunk manager.
//
// Stragglers: goroutines that complete after waitWOfN returns send
// into a buffered channel (cap = len(snapshot)) so they don't block.
// Their writes still land — late acks contribute to background
// convergence; the seal-time set-diff reconcile (gastrolog-37k2b)
// catches any remaining divergence.
func (o *Orchestrator) fanOutAppend(
	ctx context.Context,
	vaultID glid.GLID,
	chunkID chunk.ChunkID,
	rec chunk.Record,
	snapshot []string,
	w int,
	isStillReceiving IsStillReceivingFn,
	localCM chunk.ChunkManager,
) error {
	if len(snapshot) == 0 {
		return errors.New("fan-out: empty Receiving snapshot")
	}
	results := make(chan NodeResult, len(snapshot))
	for _, nodeID := range snapshot {
		go o.fanOutOne(ctx, vaultID, chunkID, rec, nodeID, localCM, results)
	}
	return waitWOfN(ctx, len(snapshot), w, results, isStillReceiving)
}

// fanOutOne is the per-snapshot-member worker: appends locally if the
// member is self (and localCM is non-nil), otherwise sends the record
// to the remote member via o.chunkReplicator.AppendRecords. Always
// reports its outcome on results — the buffered channel ensures sends
// after waitWOfN returns don't block.
func (o *Orchestrator) fanOutOne(
	ctx context.Context,
	vaultID glid.GLID,
	chunkID chunk.ChunkID,
	rec chunk.Record,
	nodeID string,
	localCM chunk.ChunkManager,
	results chan<- NodeResult,
) {
	var err error
	switch {
	case nodeID == o.localNodeID && localCM != nil:
		_, _, err = localCM.Append(rec)
	case o.chunkReplicator != nil:
		err = o.chunkReplicator.AppendRecords(ctx, nodeID, vaultID, chunkID, []chunk.Record{rec})
	default:
		err = fmt.Errorf("fan-out: no chunkReplicator configured for remote node %s", nodeID)
	}
	results <- NodeResult{NodeID: nodeID, Err: err}
}
