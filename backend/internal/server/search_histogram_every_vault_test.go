package server_test

import (
	"context"
	"io"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"connectrpc.com/connect"
)

const (
	histWindow       = "start=2025-06-15T09:59:00Z end=2025-06-15T10:03:00Z"
	histErrPerVault  = 7
	histInfoPerVault = 13
)

// seedHistogramVaults gives every node that has a vault histErrPerVault
// level=error and histInfoPerVault level=info records inside histWindow, one
// second apart and on no timestamp another vault uses, and returns how many
// vaults it seeded.
func seedHistogramVaults(t *testing.T, h *multiNodeHarness, nodeIDs []string) int {
	t.Helper()
	const perVault = histErrPerVault + histInfoPerVault
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	vaults := 0
	for _, id := range nodeIDs {
		n := h.Node(t, id)
		if n.vaultID.IsZero() {
			continue
		}
		start := t0.Add(time.Duration(vaults*perVault) * time.Second)
		addMNRecordsFromTime(t, n, id+"-err", histErrPerVault, start, map[string]string{"level": "error"})
		addMNRecordsFromTime(t, n, id+"-info", histInfoPerVault, start.Add(histErrPerVault*time.Second), map[string]string{"level": "info"})
		vaults++
	}
	return vaults
}

type histogramSearch struct {
	records   int
	buckets   int
	histTotal int64
	sawHist   bool
}

// searchWithHistogram runs a Search on the coordinator and returns the
// streamed record count and the histogram it carried.
func searchWithHistogram(t *testing.T, h *multiNodeHarness, expr string) (histogramSearch, error) {
	t.Helper()
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		return histogramSearch{}, err
	}
	var got histogramSearch
	for stream.Receive() {
		got.records += len(stream.Msg().Records)
		if hist := stream.Msg().Histogram; len(hist) > 0 {
			got.sawHist = true
			got.buckets = len(hist)
			for _, b := range hist {
				got.histTotal += b.Count
			}
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		return got, err
	}
	return got, nil
}

// A search's histogram counts the matches in every vault in scope, wherever
// each vault is led, with or without a filter: on a page that holds every
// match, the histogram total equals the records returned.
func TestMultiNode_SearchHistogramCountsEveryVault(t *testing.T) {
	t.Parallel()

	topologies := []struct {
		name  string
		nodes []string
		opts  []mnOption
	}{
		{"coordinator without vault, in-process forward", []string{"coord", "data-1", "data-2"}, []mnOption{WithoutVault("coord")}},
		{"coordinator without vault, loopback gRPC forward", []string{"coord", "data-1", "data-2"}, []mnOption{WithoutVault("coord"), WithClusterSearch()}},
		{"coordinator leads its own vault, in-process forward", []string{"coord", "data-1", "data-2"}, nil},
		{"coordinator leads its own vault, loopback gRPC forward", []string{"coord", "data-1", "data-2"}, []mnOption{WithClusterSearch()}},
		{"five nodes, in-process forward", []string{"coord", "data-1", "data-2", "data-3", "data-4"}, nil},
		{"five nodes, loopback gRPC forward", []string{"coord", "data-1", "data-2", "data-3", "data-4"}, []mnOption{WithClusterSearch()}},
		// Every vault is led by the coordinator: the histogram never leaves
		// the node.
		{"coordinator leads every vault", []string{"coord", "idle"}, []mnOption{WithoutVault("idle")}},
	}

	for _, topo := range topologies {
		t.Run(topo.name, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, topo.nodes, topo.opts...)
			vaults := seedHistogramVaults(t, h, topo.nodes)

			queries := []struct {
				name string
				expr string
				want int
				// unbounded leaves the window to each node when the
				// coordinator cannot see a remote vault's open chunk, so the
				// grids may differ; the counts still must not.
				unbounded bool
			}{
				{"unfiltered", histWindow, vaults * (histErrPerVault + histInfoPerVault), false},
				{"filtered", "level=error " + histWindow, vaults * histErrPerVault, false},
				{"filtered, negated", "not level=error " + histWindow, vaults * histInfoPerVault, false},
				{"filtered, unbounded", "level=error", vaults * histErrPerVault, true},
				{"filter matching nothing", "level=fatal " + histWindow, 0, false},
			}
			for _, qc := range queries {
				got, err := searchWithHistogram(t, h, qc.expr)
				if err != nil {
					t.Fatalf("%s: search %q: %v", qc.name, qc.expr, err)
				}
				if got.records != qc.want {
					t.Fatalf("%s: %d records, want %d — the fan-out under test is not happening", qc.name, got.records, qc.want)
				}
				if got.histTotal != int64(got.records) {
					t.Errorf("%s: histogram total %d != %d records returned", qc.name, got.histTotal, got.records)
				}
				if !qc.unbounded && got.sawHist && got.buckets != 50 {
					t.Errorf("%s: histogram has %d buckets, want 50 on one grid", qc.name, got.buckets)
				}
			}
		})
	}
}

// The histogram is the window's, not the page's: on a paged filtered search,
// the first page's histogram counts the matches every page will return, and
// only the first page asks the remote vaults for one.
func TestMultiNode_PagedFilteredSearchHistogramCountsEveryPage(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes, WithoutVault("coord"))
	vaults := seedHistogramVaults(t, h, nodes)
	want := vaults * histErrPerVault

	var resume []byte
	var records, pages int
	var histTotal int64
	for {
		stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query:       &gastrologv1.Query{Expression: "level=error limit=4 reverse=true " + histWindow},
			ResumeToken: resume,
		}))
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		var next []byte
		for stream.Receive() {
			records += len(stream.Msg().Records)
			for _, b := range stream.Msg().Histogram {
				if pages > 0 {
					t.Fatalf("page %d carries a histogram; only the first page does", pages)
				}
				histTotal += b.Count
			}
			if stream.Msg().HasMore {
				next = stream.Msg().ResumeToken
			}
		}
		if err := stream.Err(); err != nil && err != io.EOF {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		if pages == 1 {
			if got := h.remoteSearcher.histogramRequests.Load(); got != int64(vaults) {
				t.Errorf("first page asked for %d remote histograms, want one per remote vault (%d)", got, vaults)
			}
		}
		if next == nil {
			break
		}
		if pages > want {
			t.Fatalf("still paging after %d pages", pages)
		}
		resume = next
	}

	if pages < 2 {
		t.Fatalf("search fit on %d page; the test needs several", pages)
	}
	if records != want {
		t.Fatalf("%d records across %d pages, want %d", records, pages, want)
	}
	if histTotal != int64(want) {
		t.Errorf("first page's histogram total %d, want every page's %d matches", histTotal, want)
	}
	if got := h.remoteSearcher.histogramRequests.Load(); got != int64(vaults) {
		t.Errorf("%d remote histogram requests over %d pages, want %d — later pages must not ask", got, pages, vaults)
	}
}

// A remote vault that cannot answer for its histogram leaves the search with
// no histogram rather than one that silently omits that vault's share. The
// records, which answered, still arrive.
func TestMultiNode_SearchHistogramOmittedWhenARemoteVaultCannotAnswer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		expr string
	}{
		{"unfiltered", histWindow},
		{"filtered", "level=error " + histWindow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := []string{"coord", "data-1", "data-2", "data-3"}
			h := setupMultiNode(t, nodes)
			seedHistogramVaults(t, h, nodes)

			// Premise: with every vault answering, the histogram is complete.
			whole, err := searchWithHistogram(t, h, tc.expr)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if whole.histTotal == 0 || whole.histTotal != int64(whole.records) {
				t.Fatalf("baseline histogram total %d over %d records", whole.histTotal, whole.records)
			}

			h.remoteSearcher.histogramFails = map[string]bool{"data-2": true}
			got, err := searchWithHistogram(t, h, tc.expr)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if got.records != whole.records {
				t.Errorf("%d records with one remote histogram failing, want all %d", got.records, whole.records)
			}
			if got.sawHist {
				t.Errorf("histogram totalling %d of %d records returned with data-2's share missing; want none", got.histTotal, got.records)
			}
		})
	}
}

// A filtered search over a vault led on an unreachable node fails, rather
// than returning the other vaults' records under a histogram that agrees with
// them and so hides the missing vault.
func TestMultiNode_FilteredSearchFailsWhenAVaultLeaderIsUnreachable(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes)
	vaults := seedHistogramVaults(t, h, nodes)
	expr := "level=error " + histWindow

	got, err := searchWithHistogram(t, h, expr)
	if err != nil || got.records != vaults*histErrPerVault {
		t.Fatalf("premise: %d records (err %v), want %d with every node reachable", got.records, err, vaults*histErrPerVault)
	}

	delete(h.remoteSearcher.nodes, "data-2")
	got, err = searchWithHistogram(t, h, expr)
	if err == nil {
		t.Fatalf("search with data-2 unreachable succeeded: %d records, histogram total %d", got.records, got.histTotal)
	}
}

// A pipeline's histogram counts the filter's matches in every vault, like a
// search's: both the per-node table path and the gathered-records path.
func TestMultiNode_PipelineHistogramCountsEveryVault(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes, WithoutVault("coord"))
	vaults := seedHistogramVaults(t, h, nodes)
	want := int64(vaults * histErrPerVault)

	for _, expr := range []string{
		"level=error " + histWindow + " | stats count",
		"level=error " + histWindow + " | head 3 | stats count",
	} {
		got, err := searchWithHistogram(t, h, expr)
		if err != nil {
			t.Fatalf("search %q: %v", expr, err)
		}
		if got.histTotal != want {
			t.Errorf("%q: histogram total %d, want %d matches across %d vaults", expr, got.histTotal, want, vaults)
		}
	}
}
