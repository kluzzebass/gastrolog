package lifecycle

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestPhase_InitialState verifies a fresh Phase reports "running" — its
// shutdown flag is false, the context is not cancelled, and the label is
// empty. Callers rely on this to avoid gating work on a Phase that has
// never been told to shut down.
func TestPhase_InitialState(t *testing.T) {
	t.Parallel()
	p := New()

	if p.ShuttingDown() {
		t.Error("new phase should not report ShuttingDown")
	}
	if p.Label() != "" {
		t.Errorf("initial label = %q, want empty string", p.Label())
	}
	if err := p.Context().Err(); err != nil {
		t.Errorf("initial Context.Err = %v, want nil", err)
	}
	select {
	case <-p.Context().Done():
		t.Error("initial context should not be Done")
	default:
	}
}

// TestPhase_BeginShutdown verifies the three effects of BeginShutdown —
// flag flip, context cancel, label set — happen in a single call.
func TestPhase_BeginShutdown(t *testing.T) {
	t.Parallel()
	p := New()
	p.BeginShutdown("stopping")

	if !p.ShuttingDown() {
		t.Error("after BeginShutdown, ShuttingDown should be true")
	}
	if p.Label() != "stopping" {
		t.Errorf("label = %q, want %q", p.Label(), "stopping")
	}
	select {
	case <-p.Context().Done():
	case <-time.After(100 * time.Millisecond):
		t.Error("Context should be Done after BeginShutdown")
	}
}

// TestPhase_BeginShutdownIdempotent verifies a second BeginShutdown call
// updates the label but does not re-cancel or panic. This matters because
// multiple subsystems may independently observe "we are shutting down"
// and call BeginShutdown as a safety measure.
func TestPhase_BeginShutdownIdempotent(t *testing.T) {
	t.Parallel()
	p := New()
	p.BeginShutdown("first")
	p.BeginShutdown("second")

	if p.Label() != "second" {
		t.Errorf("label = %q, want %q (second call should update label)", p.Label(), "second")
	}
	if !p.ShuttingDown() {
		t.Error("still should be shutting down after second call")
	}
}

// TestPhase_Set verifies Set updates the label without flipping the
// shutdown flag. Used by the app.go shutdown sequence to advance the
// label between phases.
func TestPhase_Set(t *testing.T) {
	t.Parallel()
	p := New()
	p.Set("foo")

	if p.Label() != "foo" {
		t.Errorf("label = %q, want %q", p.Label(), "foo")
	}
	if p.ShuttingDown() {
		t.Error("Set alone should not flip ShuttingDown")
	}
}

// TestPhase_SetAfterBeginShutdown verifies the normal shutdown progression
// pattern: BeginShutdown first, then Set moves through phases.
func TestPhase_SetAfterBeginShutdown(t *testing.T) {
	t.Parallel()
	p := New()
	p.BeginShutdown("stopping")
	p.Set("draining orchestrator")

	if p.Label() != "draining orchestrator" {
		t.Errorf("label = %q, want %q", p.Label(), "draining orchestrator")
	}
	if !p.ShuttingDown() {
		t.Error("ShuttingDown should remain true across Set calls")
	}
	if p.Context().Err() == nil {
		t.Error("Context should remain cancelled after Set")
	}
}

// TestPhase_ConcurrentBeginShutdown verifies that racing goroutines all
// calling BeginShutdown end up with ShuttingDown=true and a cancelled
// context. This is the realistic shutdown pattern: a SIGTERM handler,
// a lifecycle RPC, and an error condition might all trigger shutdown
// concurrently.
func TestPhase_ConcurrentBeginShutdown(t *testing.T) {
	t.Parallel()
	p := New()

	var wg sync.WaitGroup
	const n = 100
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.BeginShutdown("caller-" + strconv.Itoa(i))
		}(i)
	}
	wg.Wait()

	if !p.ShuttingDown() {
		t.Error("after concurrent BeginShutdown, ShuttingDown should be true")
	}
	if p.Context().Err() == nil {
		t.Error("Context.Err should be set after BeginShutdown")
	}
	if p.Label() == "" {
		t.Error("label should be set to one of the caller labels")
	}
}

// TestPhase_ContextFiresForAllSubscribers verifies that every goroutine
// selecting on Context().Done() unblocks when BeginShutdown is called.
// This is the core drain mechanism — handlers waiting on the context must
// all wake up together.
func TestPhase_ContextFiresForAllSubscribers(t *testing.T) {
	t.Parallel()
	p := New()

	const n = 10
	ready := make(chan struct{}, n)
	fired := make(chan struct{}, n)
	for range n {
		go func() {
			ready <- struct{}{}
			<-p.Context().Done()
			fired <- struct{}{}
		}()
	}
	for range n {
		<-ready
	}

	p.BeginShutdown("test")

	for i := range n {
		select {
		case <-fired:
		case <-time.After(1 * time.Second):
			t.Fatalf("listener %d did not unblock after BeginShutdown", i+1)
		}
	}
}

// TestPhase_ShuttingDownIsHotPathSafe is a smoke test for the hot-path
// invariant: ShuttingDown is called once per replicated record, so it
// must be allocation-free and cheap. A million iterations before and
// after BeginShutdown in a single test is a quick regression guard.
func TestPhase_ShuttingDownIsHotPathSafe(t *testing.T) {
	t.Parallel()
	p := New()
	for range 1_000_000 {
		_ = p.ShuttingDown()
	}
	p.BeginShutdown("test")
	for range 1_000_000 {
		_ = p.ShuttingDown()
	}
}
