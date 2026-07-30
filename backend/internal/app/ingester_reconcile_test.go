package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/system"
)

const (
	divergenceLogMsg = "desired ingester(s) not running"
	convergedLogMsg  = "ingester convergence restored"
)

func countMessages(h *captureHandler, substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

// lastAttr returns the string value of the named attr on the most recent
// record whose message contains substr; "" if absent.
func lastAttr(h *captureHandler, substr, key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	val := ""
	for _, r := range h.records {
		if !strings.Contains(r.Message, substr) {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				val = a.Value.String()
			}
			return true
		})
	}
	return val
}

// TestReportIngesterDivergence pins the log half of the convergence sweep — a
// log line rather than an alarm, per the operator razor. A
// desired-but-not-running ingester logs ONE divergence line — once per state
// change, never per 15s tick — convergence logs one restored line, and
// ingesters this node should NOT run (disabled, other-node pinned) never count
// as missing.
func TestReportIngesterDivergence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idRunning, idMissing, idDisabled, idOtherNode := glid.New(), glid.New(), glid.New(), glid.New()

	store := &stubCfgStore{ingesterList: []system.IngesterConfig{
		{ID: idRunning, Name: "running", Type: "scatterbox", Enabled: true, AllNodes: true},
		{ID: idMissing, Name: "missing", Type: "scatterbox", Enabled: true, AllNodes: true},
		{ID: idDisabled, Name: "disabled", Type: "scatterbox", Enabled: false, AllNodes: true},
		{ID: idOtherNode, Name: "pinned-elsewhere", Type: "scatterbox", Enabled: true, NodeIDs: []string{"other"}},
	}}
	orchMock := &mockOrch{running: map[glid.GLID]bool{idRunning: true}}
	capture := &captureHandler{}
	logger := slog.New(capture)
	d := newTestDispatcher(orchMock, store, capture)
	d.factories.IngesterTypes["scatterbox"] = orchestrator.IngesterRegistration{}

	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg); n != 1 {
		t.Fatalf("missing desired ingester must log divergence once, got %d", n)
	}
	if got := lastAttr(capture, divergenceLogMsg, "missing"); got != "missing" {
		t.Fatalf("divergence line must carry the missing set, got %q", got)
	}

	// Same state on the next tick: no repeat line.
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg); n != 1 {
		t.Fatalf("unchanged divergence must not re-log, got %d lines", n)
	}

	// Converge: the missing one starts — one restored line.
	orchMock.running[idMissing] = true
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, convergedLogMsg); n != 1 {
		t.Fatalf("convergence must log restored once, got %d", n)
	}
	// Converged steady state: silent.
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, convergedLogMsg); n != 1 {
		t.Fatalf("steady converged state must stay silent, got %d restored lines", n)
	}

	// Re-diverge: a second divergence line.
	orchMock.running[idMissing] = false
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg); n != 2 {
		t.Fatalf("re-divergence must log again, got %d lines", n)
	}

	// Transient store error must not flap the reported state: no new lines,
	// and the next successful converged tick still logs restored.
	store.ingesterListErr = context.DeadlineExceeded
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg) + countMessages(capture, convergedLogMsg); n != 3 {
		t.Fatalf("store error must log nothing, got %d state lines", n)
	}
	store.ingesterListErr = nil
	orchMock.running[idMissing] = true
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, convergedLogMsg); n != 2 {
		t.Fatalf("recovery after store error must log restored, got %d", n)
	}
}

// pinnedDigester holds every record until release is closed, pinning the
// digestion workers the way a saturated pipeline does.
type pinnedDigester struct {
	entered chan struct{}
	release chan struct{}
}

func (d *pinnedDigester) Digest(*ingestion.IngestMessage) error {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	<-d.release
	return nil
}

// saturatingSource emits until the pipeline is saturated, tokenizing each
// successful send; it exits only on ctx cancellation.
type saturatingSource struct {
	sent chan struct{}
}

func (s *saturatingSource) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	for {
		select {
		case out <- ingestion.IngesterMessage{Raw: []byte("x")}:
			select {
			case s.sent <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func awaitAlive(t *testing.T, events <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("alive event = %v, want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for alive=%v event", want)
	}
}

// TestReportIngesterDivergence_ClearsAfterRebuildUnderSaturation is the
// sweep-level regression for a stale shared Alive flag: a config rebuild under
// a saturated pipeline can leave Alive false for an ingester that is running,
// and then this sweep reports divergence every 15s — the surface an operator
// must trust during an incident, lying. Here the sweep runs against a REAL
// orchestrator + pipeline (not a mock) that just rebuilt an ingester while the
// digestion stage was pinned: the sweep must report convergence, not
// divergence.
//
// A full multi-node reproduction is impractical without timing games — the
// multinode harness drives rebuilds through Raft config dispatch and cannot
// deterministically pin one node's digest queue at the rebuild instant — so
// this focused sweep-against-real-manager test carries the multi-dimension
// intent: the exact component chain the failure traverses (manager rebuild →
// shared IngesterStats → IsIngesterRunning → sweep log).
func TestReportIngesterDivergence_ClearsAfterRebuildUnderSaturation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	aliveCh := make(chan bool, 32)
	dig := &pinnedDigester{entered: make(chan struct{}, 16), release: make(chan struct{})}

	orch, err := orchestrator.New(orchestrator.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		IngestChannelSize: 1,
		Digesters:         []digestion.Digester{dig},
		OnIngesterAlive:   func(_ glid.GLID, alive bool) { aliveCh <- alive },
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(orch.Close)
	t.Cleanup(func() { _ = orch.Stop() })
	// Registered last so it runs first: unpin the digestion workers before
	// Stop waits for the pipeline to drain.
	t.Cleanup(func() { close(dig.release) })

	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	id := glid.New()
	store := &stubCfgStore{ingesterList: []system.IngesterConfig{
		{ID: id, Name: "flood", Type: "mock", Enabled: true, AllNodes: true},
	}}
	capture := &captureHandler{}
	logger := slog.New(capture)
	d := newTestDispatcher(orch, store, capture)
	d.factories.IngesterTypes["mock"] = orchestrator.IngesterRegistration{}

	// Desired but not yet running: the sweep must log divergence.
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg); n != 1 {
		t.Fatalf("desired-but-not-started ingester must log divergence, got %d", n)
	}

	desired := func(burst string, build func() (ingestion.Ingester, error)) []orchestrator.IngesterDesired {
		return []orchestrator.IngesterDesired{{
			ID: id, Name: "flood", Type: "mock",
			Params: map[string]string{"burst": burst},
			Build:  build,
		}}
	}

	first := &saturatingSource{sent: make(chan struct{}, 64)}
	if err := orch.ReconcileIngesters(desired("1", func() (ingestion.Ingester, error) {
		return first, nil
	})); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	awaitAlive(t, aliveCh, true)
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, convergedLogMsg); n != 1 {
		t.Fatalf("running ingester must log convergence restored, got %d", n)
	}

	// Saturate: 4 pinned digestion workers + 1 queue slot + 1 pump + 1
	// adapter absorb exactly 7 sends; the run is then parked mid-send.
	for range 7 {
		select {
		case <-first.sent:
		case <-time.After(10 * time.Second):
			t.Fatal("pipeline did not absorb the expected sends")
		}
	}
	select {
	case <-dig.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no record reached the digester")
	}

	// Config rebuild under saturation — the field trigger (scatterbox
	// burst/interval change at 100 percent ingest-digest).
	second := &saturatingSource{sent: make(chan struct{}, 64)}
	if err := orch.ReconcileIngesters(desired("2", func() (ingestion.Ingester, error) {
		return second, nil
	})); err != nil {
		t.Fatalf("rebuild reconcile: %v", err)
	}
	awaitAlive(t, aliveCh, false)
	awaitAlive(t, aliveCh, true)

	// The sweep after the rebuild: a stale Alive flag here would report
	// divergence forever on a healthy ingester.
	d.reportIngesterDivergence(ctx, logger)
	if n := countMessages(capture, divergenceLogMsg); n != 1 {
		t.Fatalf("sweep must not report divergence on a running ingester after a rebuild under saturation (got %d divergence lines)", n)
	}
}
