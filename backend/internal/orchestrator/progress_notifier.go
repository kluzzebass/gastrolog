package orchestrator

import (
	"context"
	"time"
)

// progressNotifier coalesces high-rate "active-chunk progressed"
// signals into bounded NotifyChunkChange calls. Append paths call
// Signal() per record (cheap, non-blocking). A single throttle goroutine
// fires NotifyChunkChange:
//
//   - Leading edge: immediately on the first signal after a quiet
//     period — operators see counter movement starting promptly.
//   - Trailing edge: once at the end of the throttle window if any
//     more signals arrived during it — captures the final state of
//     a burst.
//
// Idle cluster: zero work (the goroutine blocks on the trigger channel).
// Busy cluster: at most one fan-out per window per orchestrator. See
// gastrolog-4y03v.
type progressNotifier struct {
	// Buffered=1 so coincident Signal() calls collapse to a single
	// pending token. The throttle loop reads tokens to detect
	// activity; the count of dropped tokens is irrelevant — any
	// non-zero count means "at least one append happened, fire."
	trigger chan struct{}
}

func newProgressNotifier() *progressNotifier {
	return &progressNotifier{trigger: make(chan struct{}, 1)}
}

// Signal marks that an active-chunk counter has progressed. Safe to
// call concurrently from any append path; non-blocking.
func (p *progressNotifier) Signal() {
	if p == nil {
		return
	}
	select {
	case p.trigger <- struct{}{}:
	default:
		// Token already pending; coalesce.
	}
}

// runProgressNotifier drives the throttle loop. Exits when ctx is
// cancelled. Wired into auxWg from lifecycle.Start.
func (o *Orchestrator) runProgressNotifier(ctx context.Context, window time.Duration) {
	if o.progressTrigger == nil {
		return
	}
	p := o.progressTrigger
	for {
		// Quiet wait — no work until something happens.
		select {
		case <-ctx.Done():
			return
		case <-p.trigger:
		}
		// Leading edge: fire immediately on first signal after quiet.
		o.NotifyChunkChange()

		// Throttle window: collect more signals; emit a trailing-edge
		// fire if any arrived during the window. Buffered=1 means at
		// most one trigger token is observable per window regardless
		// of how many Signal() calls happened.
		timer := time.NewTimer(window)
		moreCame := false
	windowLoop:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				break windowLoop
			case <-p.trigger:
				moreCame = true
			}
		}
		if moreCame {
			o.NotifyChunkChange()
		}
		// Don't drain a token that arrived between the trailing fire
		// and the next loop iteration — that signal kicks off a fresh
		// leading-edge fire on the very next iteration, which is the
		// correct behavior for a new burst after a brief quiet.
	}
}
