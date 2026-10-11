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

// A ForwardSearch carrying a histogram grid answers with the vault's page
// histogram over the forwarded filter and no records; a record request carries
// no histogram, so a record stream never waits on a histogram's scan of the
// window.
func TestSearchExecutorAnswersHistogramRequests(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	const errs, infos = 7, 13
	orch, vaultID := newExecutorVault(t, t0, errs, infos)
	exec := newSearchExecutor(orch)
	const window = "start=2025-06-15T09:59:00Z end=2025-06-15T10:01:00Z"
	grid := &gastrologv1.HistogramGrid{
		StartNanos: time.Date(2025, 6, 15, 9, 59, 0, 0, time.UTC).UnixNano(),
		EndNanos:   time.Date(2025, 6, 15, 10, 1, 0, 0, time.UTC).UnixNano(),
		Buckets:    50,
	}

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
				HistogramGrid: grid,
			})
			if err != nil {
				t.Fatalf("executor: %v", err)
			}
			if it != nil || getToken != nil || table != nil {
				t.Fatalf("histogram request answered with records or a table")
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

// A histogram request is counted on the grid it carries, bucket for bucket,
// whatever window the vault's own data would give: shares counted on
// different grids do not sum.
func TestSearchExecutorCountsHistogramOnTheRequestedGrid(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	const errs, infos = 7, 13
	orch, vaultID := newExecutorVault(t, t0, errs, infos)
	exec := newSearchExecutor(orch)

	// The vault's records span 20 seconds; the grid spans a day around them,
	// cut into 24 buckets.
	gridStart := t0.Add(-10 * time.Hour)
	grid := &gastrologv1.HistogramGrid{
		StartNanos: gridStart.UnixNano(),
		EndNanos:   gridStart.Add(24 * time.Hour).UnixNano(),
		Buckets:    24,
	}
	for _, tc := range []struct {
		expr string
		want int64
	}{
		{"", errs + infos},
		{"level=error", errs},
	} {
		_, _, _, hist, err := exec(context.Background(), &gastrologv1.ForwardSearchRequest{
			VaultId:       vaultID.ToProto(),
			Query:         tc.expr,
			HistogramGrid: grid,
		})
		if err != nil {
			t.Fatalf("%q: executor: %v", tc.expr, err)
		}
		if len(hist) != 24 {
			t.Fatalf("%q: %d buckets, want the grid's 24", tc.expr, len(hist))
		}
		for i, b := range hist {
			if want := gridStart.Add(time.Duration(i) * time.Hour).UnixMilli(); b.TimestampMs != want {
				t.Fatalf("%q: bucket %d starts at %d ms, want the grid's %d ms", tc.expr, i, b.TimestampMs, want)
			}
		}
		if hist[10].Count != tc.want {
			t.Errorf("%q: the records' hour holds %d, want %d", tc.expr, hist[10].Count, tc.want)
		}
	}
}

// A histogram request whose grid no node could count on bucket for bucket is
// refused rather than answered on a grid this node chooses.
func TestSearchExecutorRefusesAGridItCannotCountOn(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	orch, vaultID := newExecutorVault(t, t0, 7, 13)
	exec := newSearchExecutor(orch)
	start := t0.Add(-time.Minute).UnixNano()
	end := t0.Add(time.Minute).UnixNano()

	for _, tc := range []struct {
		name string
		grid *gastrologv1.HistogramGrid
	}{
		{"no buckets", &gastrologv1.HistogramGrid{StartNanos: start, EndNanos: end}},
		{"negative buckets", &gastrologv1.HistogramGrid{StartNanos: start, EndNanos: end, Buckets: -50}},
		{"more buckets than any histogram counts", &gastrologv1.HistogramGrid{StartNanos: start, EndNanos: end, Buckets: 501}},
		{"end before start", &gastrologv1.HistogramGrid{StartNanos: end, EndNanos: start, Buckets: 50}},
		{"empty window", &gastrologv1.HistogramGrid{StartNanos: start, EndNanos: start, Buckets: 50}},
		{"narrower than a nanosecond per bucket", &gastrologv1.HistogramGrid{StartNanos: start, EndNanos: start + 49, Buckets: 50}},
		{"no window", &gastrologv1.HistogramGrid{Buckets: 50}},
	} {
		_, _, _, hist, err := exec(context.Background(), &gastrologv1.ForwardSearchRequest{
			VaultId:       vaultID.ToProto(),
			HistogramGrid: tc.grid,
		})
		if err == nil {
			t.Errorf("%s: answered with %d buckets, want the grid refused", tc.name, len(hist))
		}
	}
}
