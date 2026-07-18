package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/system"
)

// TestReportIngesterDivergence pins the alert half of the convergence sweep
// (gastrolog-3mnjlo): a desired-but-not-running ingester raises the
// ingester-not-running alert; convergence clears it; ingesters this node
// should NOT run (disabled, other-node pinned) never count as missing.
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
	d := newTestDispatcher(orchMock, store, &captureHandler{})
	d.factories.IngesterTypes["scatterbox"] = orchestrator.IngesterRegistration{}

	alerts := alert.New()
	d.reportIngesterDivergence(ctx, alerts)
	if !hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("missing desired ingester must raise the alert")
	}

	// Converge: the missing one starts.
	orchMock.running[idMissing] = true
	d.reportIngesterDivergence(ctx, alerts)
	if hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("alert must clear once every desired ingester runs")
	}

	// Transient store error must not flap the standing state.
	orchMock.running[idMissing] = false
	d.reportIngesterDivergence(ctx, alerts)
	if !hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("re-diverged: alert must raise again")
	}
	store.ingesterListErr = context.DeadlineExceeded
	d.reportIngesterDivergence(ctx, alerts)
	if !hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("transient list error must keep the standing alert, not clear it")
	}
}

// pinnedDigester holds every record until release is closed, pinning the
// digestion workers like the saturated field pipeline (gastrolog-4rdb9f).
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

func (s *saturatingSource) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	for {
		select {
		case out <- orchestrator.IngestMessage{Raw: []byte("x")}:
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
// gastrolog-4rdb9f sweep-level regression: on the live cluster, a config
// rebuild under a saturated pipeline left the shared Alive flag false for a
// running ingester, so this sweep re-raised ingester-not-running every 15s
// on 3 of 4 nodes — the alarm an operator must trust during an incident was
// lying. Here the sweep runs against a REAL orchestrator + pipeline (not a
// mock) that just rebuilt an ingester while the digestion stage was pinned:
// the alarm must clear.
//
// A full multi-node reproduction is impractical without timing games — the
// multinode harness drives rebuilds through Raft config dispatch and cannot
// deterministically pin one node's digest queue at the rebuild instant — so
// this focused sweep-against-real-manager test carries the multi-dimension
// intent: the exact component chain the field incident traversed
// (manager rebuild → shared IngesterStats → IsIngesterRunning → sweep alarm).
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
	d := newTestDispatcher(orch, store, &captureHandler{})
	d.factories.IngesterTypes["mock"] = orchestrator.IngesterRegistration{}
	alerts := alert.New()

	// Desired but not yet running: the sweep must alarm.
	d.reportIngesterDivergence(ctx, alerts)
	if !hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("desired-but-not-started ingester must raise the alert")
	}

	desired := func(burst string, build func() (orchestrator.Ingester, error)) []orchestrator.IngesterDesired {
		return []orchestrator.IngesterDesired{{
			ID: id, Name: "flood", Type: "mock",
			Params: map[string]string{"burst": burst},
			Build:  build,
		}}
	}

	first := &saturatingSource{sent: make(chan struct{}, 64)}
	if err := orch.ReconcileIngesters(desired("1", func() (orchestrator.Ingester, error) {
		return first, nil
	})); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	awaitAlive(t, aliveCh, true)
	d.reportIngesterDivergence(ctx, alerts)
	if hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("alert must clear once the ingester runs")
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
	if err := orch.ReconcileIngesters(desired("2", func() (orchestrator.Ingester, error) {
		return second, nil
	})); err != nil {
		t.Fatalf("rebuild reconcile: %v", err)
	}
	awaitAlive(t, aliveCh, false)
	awaitAlive(t, aliveCh, true)

	// The sweep after the rebuild: pre-fix this stayed raised forever.
	d.reportIngesterDivergence(ctx, alerts)
	if hasActiveAlert(alerts, ingesterNotRunningAlertID) {
		t.Fatal("gastrolog-4rdb9f: sweep must not alarm on a running ingester after a rebuild under saturation")
	}
}

func hasActiveAlert(alerts *alert.Collector, id string) bool {
	for _, a := range alerts.Standing() {
		if a.ID == id {
			return true
		}
	}
	return false
}
