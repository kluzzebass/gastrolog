package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
)

// newExecutorVault returns an orchestrator leading one memory vault holding
// errs level=error and infos level=info records, one second apart from t0.
func newExecutorVault(t *testing.T, t0 time.Time, errs, infos int) (*orchestrator.Orchestrator, glid.GLID) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "peer"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })
	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
	vaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, v.CM, v.IM, v.QE))
	for i := range errs + infos {
		level := "error"
		if i >= errs {
			level = "info"
		}
		ts := t0.Add(time.Duration(i) * time.Second)
		if _, _, err := v.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "rec-%d", i),
			Attrs:    chunk.Attributes{"level": level},
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return orch, vaultID
}

// A histogram_only ForwardSearch answers with the vault's page histogram over
// the forwarded filter and no records; a record request carries no histogram,
// so a record stream never waits on a histogram's scan of the window.
func TestSearchExecutorAnswersHistogramOnlyRequests(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	const errs, infos = 7, 13
	orch, vaultID := newExecutorVault(t, t0, errs, infos)
	exec := newSearchExecutor(orch)
	const window = "start=2025-06-15T09:59:00Z end=2025-06-15T10:01:00Z"

	for _, tc := range []struct {
		name string
		expr string
		want int64
	}{
		{"unfiltered", window, errs + infos},
		{"filtered", "level=error " + window, errs},
		{"filter matching nothing", "level=fatal " + window, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it, getToken, table, hist, err := exec(context.Background(), &gastrologv1.ForwardSearchRequest{
				VaultId:       vaultID.ToProto(),
				Query:         tc.expr,
				HistogramOnly: true,
			})
			if err != nil {
				t.Fatalf("executor: %v", err)
			}
			if it != nil || getToken != nil || table != nil {
				t.Fatalf("histogram_only answered with records or a table")
			}
			var total int64
			for _, b := range hist {
				total += b.Count
			}
			if len(hist) != 50 || total != tc.want {
				t.Errorf("histogram: %d buckets totalling %d, want 50 totalling %d", len(hist), total, tc.want)
			}

			it, _, _, hist, err = exec(context.Background(), &gastrologv1.ForwardSearchRequest{
				VaultId: vaultID.ToProto(),
				Query:   tc.expr,
			})
			if err != nil {
				t.Fatalf("executor: %v", err)
			}
			if hist != nil {
				t.Errorf("record request carried a %d-bucket histogram", len(hist))
			}
			var records int64
			for _, iterErr := range it {
				if iterErr != nil {
					t.Fatalf("record stream: %v", iterErr)
				}
				records++
			}
			if records != tc.want {
				t.Errorf("record stream yielded %d records, want %d", records, tc.want)
			}
		})
	}
}
