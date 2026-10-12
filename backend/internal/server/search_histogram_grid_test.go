package server_test

import (
	"context"
	"io"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"connectrpc.com/connect"
)

const pageHistogramBuckets = 50

type gridSearch struct {
	records int
	hist    []*gastrologv1.HistogramBucket
}

func (g gridSearch) histTotal() int64 {
	var total int64
	for _, b := range g.hist {
		total += b.Count
	}
	return total
}

// searchGrid runs a Search on the coordinator and returns the records it
// streamed and the histogram it carried.
func searchGrid(t *testing.T, h *multiNodeHarness, expr string) (gridSearch, error) {
	t.Helper()
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		return gridSearch{}, err
	}
	var got gridSearch
	for stream.Receive() {
		got.records += len(stream.Msg().Records)
		got.records += len(stream.Msg().GetTableResult().GetRows())
		if hist := stream.Msg().Histogram; len(hist) > 0 {
			got.hist = hist
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		return got, err
	}
	return got, nil
}

// assertOneGrid fails unless hist is a single grid of pageHistogramBuckets
// buckets one fixed width apart.
func assertOneGrid(t *testing.T, label string, hist []*gastrologv1.HistogramBucket) {
	t.Helper()
	if len(hist) != pageHistogramBuckets {
		t.Errorf("%s: histogram has %d buckets, want %d on one grid", label, len(hist), pageHistogramBuckets)
		return
	}
	step := hist[1].TimestampMs - hist[0].TimestampMs
	if step <= 0 {
		t.Errorf("%s: first two buckets at %d and %d ms", label, hist[0].TimestampMs, hist[1].TimestampMs)
		return
	}
	for i := 1; i < len(hist); i++ {
		if d := hist[i].TimestampMs - hist[i-1].TimestampMs; d != step && d != step+1 && d != step-1 {
			t.Errorf("%s: bucket %d is %d ms after bucket %d, want the grid's %d ms", label, i, d, i-1, step)
			return
		}
	}
}

// A search with no time window builds its histogram on one grid — one window
// over every vault in scope, cut into the page histogram's bucket count — even
// when the vaults' data sits in open chunks on nodes other than the
// coordinator. Each node counts its vaults' share on that grid, so the shares
// sum bucket by bucket.
func TestMultiNode_WindowlessSearchHistogramIsOneGrid(t *testing.T) {
	t.Parallel()

	topologies := []struct {
		name  string
		nodes []string
		opts  []mnOption
	}{
		{"coordinator without vault, in-process forward", []string{"coord", "data-1", "data-2", "data-3"}, []mnOption{WithoutVault("coord")}},
		{"coordinator without vault, loopback gRPC forward", []string{"coord", "data-1", "data-2", "data-3"}, []mnOption{WithoutVault("coord"), WithClusterSearch()}},
		{"coordinator leads its own vault, in-process forward", []string{"coord", "data-1", "data-2", "data-3"}, nil},
		{"coordinator leads its own vault, loopback gRPC forward", []string{"coord", "data-1", "data-2", "data-3"}, []mnOption{WithClusterSearch()}},
		{"five nodes", []string{"coord", "data-1", "data-2", "data-3", "data-4"}, nil},
	}

	for _, topo := range topologies {
		t.Run(topo.name, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, topo.nodes, topo.opts...)
			vaults := seedHistogramVaults(t, h, topo.nodes)
			all := vaults * (histErrPerVault + histInfoPerVault)

			for _, qc := range []struct {
				name     string
				expr     string
				want     int
				pipeline bool
			}{
				{"unfiltered, no window", "", all, false},
				{"filtered, no window", "level=error", vaults * histErrPerVault, false},
				{"negated filter, no window", "not level=error", vaults * histInfoPerVault, false},
				{"start only", "start=2025-06-15T09:59:00Z", all, false},
				{"pipeline, no window", "level=error | stats count", 1, true},
				{"unfiltered, windowed", histWindow, all, false},
				{"filtered, windowed", "level=error " + histWindow, vaults * histErrPerVault, false},
			} {
				got, err := searchGrid(t, h, qc.expr)
				if err != nil {
					t.Fatalf("%s: search %q: %v", qc.name, qc.expr, err)
				}
				if got.records != qc.want {
					t.Fatalf("%s: %d records, want %d", qc.name, got.records, qc.want)
				}
				matches := int64(got.records)
				if qc.pipeline {
					matches = int64(vaults * histErrPerVault)
				}
				if got.histTotal() != matches {
					t.Errorf("%s: histogram total %d, want the %d matches", qc.name, got.histTotal(), matches)
				}
				assertOneGrid(t, qc.name, got.hist)
			}

			// A window the operator gives is the grid itself.
			got, err := searchGrid(t, h, histWindow)
			if err != nil {
				t.Fatalf("windowed search: %v", err)
			}
			wantFirst := time.Date(2025, 6, 15, 9, 59, 0, 0, time.UTC).UnixMilli()
			if len(got.hist) > 0 && got.hist[0].TimestampMs != wantFirst {
				t.Errorf("windowed search: first bucket at %d ms, want the window's start %d ms", got.hist[0].TimestampMs, wantFirst)
			}
		})
	}
}

// A window-less search whose oldest data sits in a remote vault, while the
// coordinator's own vault holds only newer data, still returns every record
// and counts every one of them in its histogram: the window comes from every
// vault's bounds, not from the vaults the coordinator happens to see.
func TestMultiNode_WindowlessSearchCoversRemoteVaultsOlderData(t *testing.T) {
	t.Parallel()
	for _, topo := range []struct {
		name string
		opts []mnOption
	}{
		{"in-process forward", nil},
		{"loopback gRPC forward", []mnOption{WithClusterSearch()}},
	} {
		t.Run(topo.name, func(t *testing.T) {
			t.Parallel()
			nodes := []string{"coord", "data-1", "data-2", "data-3"}
			h := setupMultiNode(t, nodes, topo.opts...)
			recent := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
			addMNRecordsFromTime(t, h.Node(t, "coord"), "coord", 10, recent, nil)
			addMNRecordsFromTime(t, h.Node(t, "data-1"), "old", 10, recent.Add(-48*time.Hour), nil)
			addMNRecordsFromTime(t, h.Node(t, "data-2"), "older", 10, recent.Add(-96*time.Hour), nil)
			addMNRecordsFromTime(t, h.Node(t, "data-3"), "mid", 10, recent.Add(-24*time.Hour), nil)

			got, err := searchGrid(t, h, "")
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if got.records != 40 {
				t.Fatalf("%d records, want all 40 — a window derived from the coordinator's own vault cuts off the remote vaults' older data", got.records)
			}
			if got.histTotal() != 40 {
				t.Errorf("histogram total %d, want 40", got.histTotal())
			}
			assertOneGrid(t, "unfiltered", got.hist)
			if len(got.hist) > 0 {
				oldest := recent.Add(-96 * time.Hour).UnixMilli()
				if first := got.hist[0].TimestampMs; first > oldest {
					t.Errorf("grid starts at %d ms, after the oldest record at %d ms", first, oldest)
				}
			}
		})
	}
}

// When a remote vault's leader cannot say where that vault's data lies, the
// search has no window to give its histogram: it returns every record, as an
// unbounded search, and no histogram rather than one built on a grid that may
// leave that vault's data out. Here that vault holds the oldest records, which
// a window taken from the other vaults would cut off.
func TestMultiNode_WindowlessSearchWithoutAVaultsBoundsHasNoHistogram(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes, WithoutVault("coord"))
	vaults := seedHistogramVaults(t, h, nodes)
	inWindow := vaults * (histErrPerVault + histInfoPerVault)
	const oldest = 5
	addMNRecordsFromTime(t, h.Node(t, "data-2"), "data-2-oldest", oldest, time.Date(2025, 6, 13, 10, 0, 0, 0, time.UTC), nil)
	want := inWindow + oldest

	whole, err := searchGrid(t, h, "")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if whole.records != want || whole.histTotal() != int64(want) {
		t.Fatalf("premise: %d records, histogram total %d, want %d of each with every leader answering", whole.records, whole.histTotal(), want)
	}

	h.remoteSearcher.boundsFails = map[string]bool{"data-2": true}
	got, err := searchGrid(t, h, "")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got.records != want {
		t.Errorf("%d records with data-2's bounds unknown, want all %d", got.records, want)
	}
	if len(got.hist) != 0 {
		t.Errorf("histogram of %d buckets totalling %d returned with data-2's bounds unknown; want none", len(got.hist), got.histTotal())
	}

	// A window the operator gives needs no vault's bounds.
	got, err = searchGrid(t, h, histWindow)
	if err != nil {
		t.Fatalf("windowed search: %v", err)
	}
	if got.records != inWindow || got.histTotal() != int64(inWindow) {
		t.Errorf("windowed search: %d records, histogram total %d, want %d of each", got.records, got.histTotal(), inWindow)
	}
	assertOneGrid(t, "windowed", got.hist)
}

// A paged window-less search runs every page in the window its first page
// resolved: every record arrives exactly once, the first page's histogram
// counts them all on one grid, and only the first page asks the vaults'
// leaders where their data lies.
func TestMultiNode_PagedWindowlessSearchKeepsTheFirstPagesWindow(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes, WithoutVault("coord"))
	vaults := seedHistogramVaults(t, h, nodes)
	want := vaults * (histErrPerVault + histInfoPerVault)

	seen := make(map[string]bool, want)
	var resume []byte
	var pages int
	var hist []*gastrologv1.HistogramBucket
	for {
		stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query:       &gastrologv1.Query{Expression: "limit=7 reverse=true"},
			ResumeToken: resume,
		}))
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		var next []byte
		for stream.Receive() {
			for _, rec := range stream.Msg().Records {
				raw := string(rec.Raw)
				if seen[raw] {
					t.Fatalf("page %d repeats record %q", pages, raw)
				}
				seen[raw] = true
			}
			if len(stream.Msg().Histogram) > 0 {
				if pages > 0 {
					t.Fatalf("page %d carries a histogram; only the first page does", pages)
				}
				hist = stream.Msg().Histogram
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
			if got := h.remoteSearcher.boundsRequests.Load(); got != int64(vaults) {
				t.Errorf("first page asked %d leaders for their vaults' chunks, want one per remote vault (%d)", got, vaults)
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
	if len(seen) != want {
		t.Errorf("%d distinct records across %d pages, want %d", len(seen), pages, want)
	}
	assertOneGrid(t, "first page", hist)
	if got := (gridSearch{hist: hist}).histTotal(); got != int64(want) {
		t.Errorf("first page's histogram total %d, want %d", got, want)
	}
	if got := h.remoteSearcher.boundsRequests.Load(); got != int64(vaults) {
		t.Errorf("%d leader chunk listings over %d pages, want %d — later pages run in the first page's window", got, pages, vaults)
	}
}

// A window-less search over vaults holding no records returns no records and
// no histogram, and does not fail.
func TestMultiNode_WindowlessSearchOverEmptyVaults(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithoutVault("coord"))
	got, err := searchGrid(t, h, "")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got.records != 0 || len(got.hist) != 0 {
		t.Errorf("%d records and a %d-bucket histogram over empty vaults, want none", got.records, len(got.hist))
	}
}
