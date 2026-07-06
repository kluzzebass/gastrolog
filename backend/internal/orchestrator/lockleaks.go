package orchestrator

import (
	"context"
	"time"

	"gastrolog/internal/alert"
)

const (
	// lockLeakSweepInterval is how often tracked o.mu state is scanned.
	lockLeakSweepInterval = 15 * time.Second
	// lockLeakThreshold is the age at which a hold or a stuck write wait
	// is reported. Normal critical sections are microseconds; sweeps and
	// reconfig hold for milliseconds. A minute-old hold is a leak, and a
	// minute-old write wait means the node is already wedging.
	lockLeakThreshold = time.Minute
	lockLeakAlertID   = "orchestrator-lock-leak"
)

// runLockLeakReporter surfaces orphaned o.mu holds and stuck write
// waiters with their acquisition stacks (gastrolog-1ug3rq). Deliberately
// a raw ticker goroutine rather than a scheduler job: the scheduler and
// nearly every other subsystem block on o.mu when the lock wedges — a
// deadlock detector must not depend on the machinery it diagnoses. The
// alert is never cleared: a leaked hold cannot be released by anything
// short of a restart, so a sticky alert is the truth.
func (o *Orchestrator) runLockLeakReporter(ctx context.Context) {
	if !o.mu.TrackingEnabled() {
		return
	}
	ticker := time.NewTicker(lockLeakSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, leak := range o.mu.Leaks(lockLeakThreshold) {
			o.logger.Error("orchestrator lock leak detected — acquisition site follows",
				"kind", string(leak.Kind),
				"age", leak.Age.Round(time.Second),
				"stack", leak.Stack)
			if o.alerts != nil {
				o.alerts.Set(lockLeakAlertID, alert.Error, "orchestrator",
					string(leak.Kind)+" on the orchestrator registry lock held/stuck for "+
						leak.Age.Round(time.Second).String()+
						" — node is likely wedging; the acquisition stack is in this node's log. Restart the node to clear; report the stack")
			}
		}
	}
}
