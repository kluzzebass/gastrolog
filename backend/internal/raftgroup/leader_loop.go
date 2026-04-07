package raftgroup

import (
	"context"
	"log/slog"
	"time"

	"gastrolog/internal/logging"
)

// LeaderLoop runs a leader-only worker for a single Raft group. It watches
// the group's LeaderCh() and, on every transition into leadership, spawns a
// fresh "leader epoch" goroutine that runs the supplied OnLead callback.
// On every transition out of leadership (or shutdown), the previous epoch's
// context is cancelled and the LeaderLoop waits for the OnLead callback to
// return before allowing a new epoch to start.
//
// The dispatch goroutine is non-blocking by construction: spawning the epoch
// is just `go onLead(ctx)`, never a synchronous call. This is critical —
// hraft.Raft.LeaderCh() is buffered with capacity 1 and uses drop-on-full
// semantics. If the dispatch loop blocked inside OnLead, a rapid
// gain → loss → gain transition could miss the intermediate `false` and
// run two leader epochs concurrently.
//
// The OnLead callback is called AFTER raft.Barrier() returns successfully,
// so the local FSM is guaranteed to reflect every entry committed at the
// moment of election. If Barrier() fails (timeout, lost leadership, or
// shutdown), OnLead is never called for that epoch.
//
// The callback owns the entire leader epoch — it should run its periodic
// reconcile loop, listen for events, and exit when its context is cancelled.
// LeaderLoop does not impose any periodic ticker; that's the callback's job.
type LeaderLoop struct {
	group  *Group
	name   string
	onLead func(ctx context.Context)
	logger *slog.Logger

	barrierTimeout time.Duration
}

// LeaderLoopConfig describes a leader loop to construct.
type LeaderLoopConfig struct {
	// Group is the Raft group whose leadership transitions we watch.
	Group *Group

	// Name identifies this loop in log messages (e.g. the tier ID).
	Name string

	// OnLead is invoked once per leader epoch, after Barrier() returns.
	// It receives a context that is cancelled when leadership is lost or
	// the parent context is cancelled. OnLead should return promptly when
	// its context is cancelled — the dispatch loop blocks on its return
	// before allowing a new epoch to start.
	OnLead func(ctx context.Context)

	// BarrierTimeout bounds the wait for Barrier() in each new epoch.
	// Defaults to 10s if zero.
	BarrierTimeout time.Duration

	// Logger for structured logging. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewLeaderLoop constructs a LeaderLoop. Call Run to start the dispatch
// goroutine; Run returns when the supplied context is cancelled.
func NewLeaderLoop(cfg LeaderLoopConfig) *LeaderLoop {
	timeout := cfg.BarrierTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &LeaderLoop{
		group:          cfg.Group,
		name:           cfg.Name,
		onLead:         cfg.OnLead,
		logger:         logging.Default(cfg.Logger).With("component", "leader-loop", "group", cfg.Name),
		barrierTimeout: timeout,
	}
}

// Run watches the group's LeaderCh and dispatches leader epochs. It blocks
// until the context is cancelled. Run is intended to be called in its own
// goroutine.
func (l *LeaderLoop) Run(ctx context.Context) {
	var (
		epochCancel context.CancelFunc
		epochDone   chan struct{}
	)

	// stopEpoch cancels any in-flight epoch and waits for its onLead
	// callback to return. Idempotent.
	stopEpoch := func() {
		if epochCancel == nil {
			return
		}
		epochCancel()
		<-epochDone
		epochCancel = nil
		epochDone = nil
	}
	defer stopEpoch()

	for {
		select {
		case <-ctx.Done():
			return
		case isLeader := <-l.group.Raft.LeaderCh():
			// Always tear down the previous epoch before deciding what
			// to do next. This guarantees that two epochs are never
			// alive at the same time, even on a rapid gain/loss/gain
			// transition.
			stopEpoch()

			if !isLeader {
				l.logger.Debug("leadership lost")
				continue
			}

			l.logger.Debug("leadership gained, starting epoch")
			epochCtx, cancel := context.WithCancel(ctx)
			epochCancel = cancel
			epochDone = make(chan struct{})

			go l.runEpoch(epochCtx, epochDone)
		}
	}
}

// runEpoch executes one leader epoch: barrier, then onLead. It always closes
// epochDone before returning so the dispatch loop can resume cleanly.
func (l *LeaderLoop) runEpoch(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	if err := l.group.Raft.Barrier(l.barrierTimeout).Error(); err != nil {
		// Lost leadership during the barrier wait, or shutting down.
		// OnLead is not invoked.
		l.logger.Debug("barrier failed, skipping epoch", "error", err)
		return
	}

	// Re-check that our context is still alive after the barrier wait.
	// (The dispatch loop may have cancelled us while we were blocked.)
	if ctx.Err() != nil {
		return
	}

	if l.onLead == nil {
		return
	}
	l.onLead(ctx)
	l.logger.Debug("epoch ended")
}
