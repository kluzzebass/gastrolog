package orchestrator

// The route-disposition fan-out was shutdown-blind. A stopped pipeline
// supervisor turned every record submit into an identical per-record WARN
// (tens of thousands of lines per expiring chunk on every shutdown), and —
// worse — the aborted fan-out still burned the one-shot
// routing gate: the pending flag was applied before routing, so the next
// sweep skipped routing and destroyed the chunk unrouted. These tests pin
// the fixed behavior: single-warn abort, gate preserved on abort, pending
// flag applied only after the fan-out completes.

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/system"
)

// syncBuffer is a goroutine-safe log sink (fan-out workers warn concurrently).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestFireRetentionEventAbortsOnceOnStoppedPipeline reproduces the shutdown
// flood: many records, stopped supervisor. The fan-out must abort on the
// FIRST terminal submit error with exactly one warn — not one per record —
// and report non-completion so the caller retries on a later sweep.
func TestFireRetentionEventAbortsOnceOnStoppedPipeline(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	if err := fx.orch.pipeline.Stop(); err != nil {
		t.Fatalf("pipeline Stop: %v", err)
	}

	logSink := &syncBuffer{}
	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
	}

	if r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fireRetentionEvent must report non-completion when the pipeline is stopped")
	}

	logs := logSink.String()
	if got := strings.Count(logs, "fan-out aborted"); got != 1 {
		t.Errorf("want exactly 1 abort warn, got %d\nlogs:\n%s", got, logs)
	}
	if got := strings.Count(logs, "fan-out submit error"); got != 0 {
		t.Errorf("want 0 per-record submit warns (the flood), got %d\nlogs:\n%s", got, logs)
	}
}

// TestTryRetainChunkPreservesRouteGateOnAbort verifies the reorder: when the
// fan-out aborts, the retention-pending flag must NOT be applied and the
// chunk must NOT be expired — the one-shot routing gate stays unburned so
// the sweep after restart re-routes the records instead of destroying them.
func TestTryRetainChunkPreservesRouteGateOnAbort(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	if err := fx.orch.pipeline.Stop(); err != nil {
		t.Fatalf("pipeline Stop: %v", err)
	}

	var markCalls int
	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.New(slog.NewTextHandler(&syncBuffer{}, nil)),
		disposition: system.RetentionDispositionRoute,
		inflight:    make(map[chunk.ChunkID]bool),
		applyRaftRetentionPending: func(chunk.ChunkID) error {
			markCalls++
			return nil
		},
		// No reconciler / cm / im: expireChunk would nil-deref. It must
		// never be reached on the abort path, so no recover wrapper —
		// a panic here IS the regression.
	}

	r.tryRetainChunk(fx.sealedID, retentionRule{}, false)

	if markCalls != 0 {
		t.Errorf("aborted fan-out must not apply retention-pending (gate burned); markCalls=%d", markCalls)
	}
	r.mu.Lock()
	stillInflight := r.inflight[fx.sealedID]
	r.mu.Unlock()
	if stillInflight {
		t.Error("inflight must be released after abort so a later sweep can retry")
	}
}

// TestTryRetainChunkMarksPendingAfterSuccessfulRoute pins the new ordering on
// the happy path: the fan-out completes into a running pipeline FIRST, then
// the retention-pending flag is applied exactly once.
func TestTryRetainChunkMarksPendingAfterSuccessfulRoute(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	var mu sync.Mutex
	var markCalls int
	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionRoute,
		inflight:    make(map[chunk.ChunkID]bool),
		applyRaftRetentionPending: func(chunk.ChunkID) error {
			mu.Lock()
			markCalls++
			mu.Unlock()
			return nil
		},
	}

	// expireChunk nil-derefs in this slim harness (reconciler/cm are nil);
	// it runs AFTER the mark, so recover to keep the assertions reachable.
	func() {
		defer func() { _ = recover() }()
		r.tryRetainChunk(fx.sealedID, retentionRule{}, false)
	}()

	waitForRouteStats(t, fx.orch, "3 routed records before mark", func(s *RouteStats) bool {
		return s.Matched == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if markCalls != 1 {
		t.Fatalf("want exactly 1 retention-pending apply after completed fan-out, got %d", markCalls)
	}
}

// TestTryRetainChunkSkipsEverythingDuringShutdown: once BeginShutdown is
// signalled, a sweep tick must not route, mark, or expire anything — the
// post-restart sweep owns the chunk.
func TestTryRetainChunkSkipsEverythingDuringShutdown(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	phase := lifecycle.New()
	phase.BeginShutdown("test drain")
	fx.orch.phase = phase

	var markCalls int
	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionRoute,
		inflight:    make(map[chunk.ChunkID]bool),
		applyRaftRetentionPending: func(chunk.ChunkID) error {
			markCalls++
			return nil
		},
		// No reconciler / cm: reaching expireChunk would panic. During
		// shutdown tryRetainChunk must return before ANY of it.
	}

	r.tryRetainChunk(fx.sealedID, retentionRule{}, false)

	time.Sleep(50 * time.Millisecond)
	if s := fx.orch.GetRouteStats(); s.Routed != 0 {
		t.Errorf("shutdown sweep must not fan out records; ingested=%d", s.Routed)
	}
	if markCalls != 0 {
		t.Errorf("shutdown sweep must not apply retention-pending; markCalls=%d", markCalls)
	}
}
