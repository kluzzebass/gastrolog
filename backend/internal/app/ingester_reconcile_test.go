package app

import (
	"context"
	"testing"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
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

func hasActiveAlert(alerts *alert.Collector, id string) bool {
	for _, a := range alerts.Active() {
		if a.ID == id {
			return true
		}
	}
	return false
}
