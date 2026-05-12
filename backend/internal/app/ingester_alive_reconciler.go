package app

import (
	"context"
	"log/slog"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// AliveReconciler serializes SetIngesterAlive Raft applies and retries
// transient failures with bounded exponential backoff. Solves the gap
// where the orchestrator's runIngester goroutine fires OnIngesterAlive
// once and an unlucky Raft-startup race drops the apply — leaving the
// goroutine running but the FSM alive map empty (gastrolog-1ox8z).
//
// Design notes:
//   - Fire-and-forget from runIngester's perspective: OnIngesterAlive
//     enqueues and returns. The ingester doesn't block on Raft latency.
//   - Single-goroutine worker preserves event order: alive=true followed
//     by alive=false applies in that sequence even with retries, so the
//     FSM converges to the last value the orchestrator declared.
//   - Errors are LOGGED (the original code silently dropped them with
//     `_ =`). Persistent failures escalate to ERROR after the retry
//     budget is exhausted; transient retries log at WARN.
type AliveReconciler struct {
	queue  chan aliveEvent
	store  system.Store
	nodeID string
	logger *slog.Logger
}

type aliveEvent struct {
	ingesterID glid.GLID
	alive      bool
}

const aliveQueueCapacity = 256

func NewAliveReconciler(store system.Store, nodeID string, logger *slog.Logger) *AliveReconciler {
	return &AliveReconciler{
		queue:  make(chan aliveEvent, aliveQueueCapacity),
		store:  store,
		nodeID: nodeID,
		logger: logger,
	}
}

// Enqueue submits an alive-state change. Non-blocking; if the queue is
// full (extremely unlikely outside pathological storms) the event is
// dropped with a warning. Callers must not block on this.
func (r *AliveReconciler) Enqueue(ingesterID glid.GLID, alive bool) {
	select {
	case r.queue <- aliveEvent{ingesterID: ingesterID, alive: alive}:
	default:
		r.logger.Warn("ingester alive reconciler queue full; dropping event",
			"ingester", ingesterID, "alive", alive)
	}
}

// Run blocks until ctx is cancelled, draining the event queue. Apply
// retries with exponential backoff (200ms → 30s cap, 10 attempts ≈ 5 min
// total budget) so transient Raft hiccups don't leave the FSM stale.
func (r *AliveReconciler) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-r.queue:
			r.apply(ctx, ev)
		}
	}
}

func (r *AliveReconciler) apply(ctx context.Context, ev aliveEvent) {
	const maxAttempts = 10
	const initialBackoff = 200 * time.Millisecond
	const maxBackoff = 30 * time.Second

	backoff := initialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		applyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := r.store.SetIngesterAlive(applyCtx, ev.ingesterID, r.nodeID, ev.alive)
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			// App shutting down — give up quietly.
			return
		}
		r.logger.Warn("SetIngesterAlive failed; will retry",
			"ingester", ev.ingesterID, "node", r.nodeID, "alive", ev.alive,
			"attempt", attempt, "max_attempts", maxAttempts, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	r.logger.Error("SetIngesterAlive permanently failed after retries — inspector will show stale state until next change",
		"ingester", ev.ingesterID, "node", r.nodeID, "alive", ev.alive)
}
