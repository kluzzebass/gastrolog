package orchestrator_test

// An ingester config rebuild under a saturated pipeline must leave the alive
// surface truthful. The field incident: the old run, parked in a send on the
// full ingest-digest queue, woke on cancel AFTER the successor had stored
// alive=true on the SAME shared IngesterStats (reused across rebuilds by
// design so counters survive), and its deferred alive-false clobbered it —
// IsIngesterRunning lied false forever and the convergence sweep reported
// divergence every 15s on 3 of 4 nodes.
//
// These tests run the real orchestrator + pipeline: a digester that blocks
// every record pins the digestion stage exactly like the saturated field
// pipeline, the inner ingester floods until the chain is provably parked
// (send-count arithmetic, no sleeps), and rebuilds are driven through
// ReconcileIngesters. Alive transitions are observed through the
// OnIngesterAlive callback — the same channel the Raft/UI surface rides on —
// so event order in the test equals the order the cluster sees.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/waittest"
)

// blockedDigester holds every record until release is closed, pinning the
// digestion workers the way the saturated field pipeline was pinned.
type blockedDigester struct {
	entered chan struct{}
	release chan struct{}
}

func (d *blockedDigester) Digest(*ingestion.IngestMessage) error {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	<-d.release
	return nil
}

// saturatingIngester emits until the pipeline is saturated, tokenizing each
// successful send; it exits only on ctx cancellation.
type saturatingIngester struct {
	sent chan struct{}
}

func (f *saturatingIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	for {
		select {
		case out <- ingestion.IngesterMessage{Raw: []byte("x")}:
			select {
			case f.sent <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ingesterProgress snapshots the shared IngesterStats of id, the progress
// measure for waits on that ingester's runs.
func ingesterProgress(orch *orchestrator.Orchestrator, id glid.GLID) func() string {
	return func() string {
		st := orch.GetIngesterStats(id)
		if st == nil {
			return "no stats"
		}
		return fmt.Sprintf("ingested=%d errors=%d alive=%v",
			st.MessagesIngested.Load(), st.Errors.Load(), st.Alive.Load())
	}
}

func expectAlive(t *testing.T, events <-chan bool, want bool, progress func() string) {
	t.Helper()
	if got := waittest.Recv(t, fmt.Sprintf("alive=%v event", want), events, progress); got != want {
		t.Fatalf("alive event = %v, want %v", got, want)
	}
}

// saturatedFloodDesired returns a desired-set for one flood ingester whose
// params carry a marker so changing it forces a rebuild.
func saturatedFloodDesired(id glid.GLID, burst string, build func() (ingestion.Ingester, error)) []orchestrator.IngesterDesired {
	return []orchestrator.IngesterDesired{{
		ID:     id,
		Name:   "flood",
		Type:   "mock",
		Params: map[string]string{"burst": burst},
		Build:  build,
	}}
}

func TestIngesterRebuildUnderSaturationKeepsAliveTruth(t *testing.T) {
	t.Parallel()

	aliveCh := make(chan bool, 32)
	dig := &blockedDigester{entered: make(chan struct{}, 16), release: make(chan struct{})}

	orch := mustNewTestOrch(t, orchestrator.Config{
		IngestChannelSize: 1,
		Digesters:         []digestion.Digester{dig},
		OnIngesterAlive:   func(_ glid.GLID, alive bool) { aliveCh <- alive },
	})
	t.Cleanup(orch.Close)
	t.Cleanup(func() { _ = orch.Stop() })
	// Registered last so it runs first: unpin the digestion workers before
	// Stop waits for the pipeline to drain.
	t.Cleanup(func() { close(dig.release) })

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	id := glid.New()
	progress := ingesterProgress(orch, id)
	first := &saturatingIngester{sent: make(chan struct{}, 64)}
	if err := orch.ReconcileIngesters(saturatedFloodDesired(id, "1", func() (ingestion.Ingester, error) {
		return first, nil
	})); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	expectAlive(t, aliveCh, true, progress)

	// Saturate deterministically. Absorption capacity of the chain: the 4
	// digestion workers hold one record each (pinned in Digest), the
	// ingest-digest queue holds 1 (IngestChannelSize), the ingestion pump
	// holds 1 (blocked in the queue send), and the adapter holds 1 (blocked
	// forwarding to the pump). Exactly 7 sends complete; the 8th parks the
	// run mid-send, wakeable only by cancellation — the field state.
	for i := range 7 {
		waittest.Recv(t, fmt.Sprintf("pipeline absorbs send %d of 7 (did a stage capacity change?)", i+1), first.sent, progress)
	}
	waittest.Recv(t, "a record reaches the digester", dig.entered, progress)

	// Rebuild under saturation: param change forces stop+start on the same
	// shared IngesterStats.
	second := &saturatingIngester{sent: make(chan struct{}, 64)}
	if err := orch.ReconcileIngesters(saturatedFloodDesired(id, "2", func() (ingestion.Ingester, error) {
		return second, nil
	})); err != nil {
		t.Fatalf("rebuild reconcile: %v", err)
	}

	// Ordering is the fix: the old run's alive-false must land strictly
	// before the new run's alive-true. Pre-fix the stream ended
	// true,false — and IsIngesterRunning lied until the next rebuild.
	expectAlive(t, aliveCh, false, progress)
	expectAlive(t, aliveCh, true, progress)
	if !orch.IsIngesterRunning(id) {
		t.Fatal("rebuilt ingester must report running (stale alive-false clobbered the successor)")
	}
	select {
	case v := <-aliveCh:
		t.Fatalf("unexpected trailing alive event %v after rebuild", v)
	default:
	}

	// Stop-only: removing the ingester ends not-running, with the exit
	// having fully landed before ReconcileIngesters returns.
	if err := orch.ReconcileIngesters(nil); err != nil {
		t.Fatalf("removal reconcile: %v", err)
	}
	expectAlive(t, aliveCh, false, progress)
	if orch.IsIngesterRunning(id) {
		t.Fatal("removed ingester must report not running")
	}
}

// failingIngester fails every attempt immediately.
type failingIngester struct{}

func (failingIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	return errors.New("source unavailable")
}

// TestIngesterAliveFalseDuringRetryBackoff pins the alarm contract the
// convergence sweep depends on (and which the rebuild fix must NOT change): a
// genuinely failing ingester reports not-running while parked in retry
// backoff between attempts.
func TestIngesterAliveFalseDuringRetryBackoff(t *testing.T) {
	t.Parallel()

	aliveCh := make(chan bool, 32)
	delayCalled := make(chan struct{}, 4)

	orch := mustNewTestOrch(t, orchestrator.Config{
		OnIngesterAlive: func(_ glid.GLID, alive bool) { aliveCh <- alive },
		IngesterRetryDelay: func(int) time.Duration {
			delayCalled <- struct{}{}
			return time.Hour
		},
	})
	t.Cleanup(orch.Close)
	t.Cleanup(func() { _ = orch.Stop() })

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	id := glid.New()
	if err := orch.ReconcileIngesters(saturatedFloodDesired(id, "1", func() (ingestion.Ingester, error) {
		return failingIngester{}, nil
	})); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Attempt runs (alive toggles true then false on the run's failure);
	// the manager then consults the retry delay and parks for an hour.
	progress := ingesterProgress(orch, id)
	expectAlive(t, aliveCh, true, progress)
	expectAlive(t, aliveCh, false, progress)
	waittest.Recv(t, "retry delay is consulted", delayCalled, progress)
	if orch.IsIngesterRunning(id) {
		t.Fatal("failing ingester must report not running during retry backoff")
	}
}
