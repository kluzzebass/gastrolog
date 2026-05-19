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
