package orchestrator_test

import (
	"context"
	"sync/atomic"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

type staticIngester struct{}

func (staticIngester) Run(context.Context, chan<- orchestrator.IngestMessage) error {
	return nil
}

func TestReconcileIngesters_RebuildsOnParamChange(t *testing.T) {
	t.Parallel()

	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	var builds atomic.Int32
	var lastBurst atomic.Value

	desired := func(burst string) []orchestrator.IngesterDesired {
		return []orchestrator.IngesterDesired{{
			ID:      id,
			Name:    "scatterbox",
			Type:    "scatterbox",
			Params:  map[string]string{"burst": burst, "interval": "10ms"},
			Build: func() (orchestrator.Ingester, error) {
				builds.Add(1)
				lastBurst.Store(burst)
				return staticIngester{}, nil
			},
		}}
	}

	if err := orch.ReconcileIngesters(desired("20")); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("builds after first reconcile: got %d want 1", got)
	}

	if err := orch.ReconcileIngesters(desired("20")); err != nil {
		t.Fatalf("second reconcile same params: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("builds after unchanged reconcile: got %d want 1", got)
	}

	if err := orch.ReconcileIngesters(desired("40")); err != nil {
		t.Fatalf("third reconcile param change: %v", err)
	}
	if got := builds.Load(); got != 2 {
		t.Fatalf("builds after param change: got %d want 2", got)
	}
	if got := lastBurst.Load(); got != "40" {
		t.Fatalf("last burst: got %v want 40", got)
	}
}

func TestReconcileIngesters_IgnoresParamKeyOrder(t *testing.T) {
	t.Parallel()

	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	var builds atomic.Int32
	base := orchestrator.IngesterDesired{
		ID:   id,
		Name: "scatterbox",
		Type: "scatterbox",
		Build: func() (orchestrator.Ingester, error) {
			builds.Add(1)
			return staticIngester{}, nil
		},
	}

	first := base
	first.Params = map[string]string{"burst": "20", "interval": "10ms"}
	if err := orch.ReconcileIngesters([]orchestrator.IngesterDesired{first}); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	second := base
	second.Params = map[string]string{"interval": "10ms", "burst": "20"}
	if err := orch.ReconcileIngesters([]orchestrator.IngesterDesired{second}); err != nil {
		t.Fatalf("second reconcile reordered keys: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("builds: got %d want 1 (key order must not trigger rebuild)", got)
	}
}
