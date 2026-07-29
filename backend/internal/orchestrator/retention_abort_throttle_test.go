package orchestrator

// A standing condition — a destination parked at its max-size bound — hits every
// chunk of every sweep, and the abort warn used to be emitted per chunk. On the
// dev cluster that was tens of identical lines per second, indefinitely. The
// transfer disposition already throttles the same event; the route path did not
// (gastrolog-4dr79b).

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
)

func TestFanOutAbortWarnIsThrottledAcrossChunks(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	capped := fx.archiveID
	fx.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })

	logSink := &syncBuffer{}
	now := time.Now()
	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
		idleLog: logging.Throttle{Interval: 10 * time.Minute, Now: func() time.Time { return now }},
	}

	// The sweep walking several chunks against the same full destination.
	for range 5 {
		if r.fireRetentionEvent(fx.sealedID) {
			t.Fatal("fireRetentionEvent must report non-completion against a capped destination")
		}
	}

	logs := logSink.String()
	if got := strings.Count(logs, "fan-out aborted"); got != 1 {
		t.Errorf("want 1 abort warn for 5 chunks hitting one standing condition, got %d\nlogs:\n%s",
			got, logs)
	}

	// Past the interval the condition is named again, carrying what it hid.
	now = now.Add(11 * time.Minute)
	if r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fireRetentionEvent must still report non-completion")
	}
	logs = logSink.String()
	if got := strings.Count(logs, "fan-out aborted"); got != 2 {
		t.Errorf("the condition must be re-reported after the throttle interval; got %d lines\nlogs:\n%s",
			got, logs)
	}
	if !strings.Contains(logs, "suppressed=") {
		t.Error("the re-report must say how many lines it stood in for")
	}
}

// The alarm must not be throttled with the log: it is the surface an operator is
// supposed to act on, and it is fed on every abort regardless.
func TestFanOutAbortAlwaysNotesTheDeferral(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	capped := fx.archiveID
	fx.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })

	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(&syncBuffer{}, nil)),
		idleLog: logging.Throttle{Interval: time.Hour},
	}

	for range 3 {
		r.fireRetentionEvent(fx.sealedID)
		// Reset the per-sweep flag the way a real sweep boundary does, so each
		// iteration stands for its own sweep.
		r.mu.Lock()
		deferred := r.sweepDeferred
		cause := r.lastDeferralCause
		r.sweepDeferred = false
		r.mu.Unlock()

		if !deferred {
			t.Fatal("an aborted fan-out must mark the sweep deferred even when its log line is throttled")
		}
		if cause == "" {
			t.Error("the deferral carries no cause; the alarm detail would name nothing")
		}
	}
}
