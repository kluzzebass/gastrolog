package server_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// losslessPageSize is the page size a client sends with every search, the way
// the UI does. Every dataset below holds more records per vault than this, so
// a remote that scanned under the page size instead of the coordinator's
// pipeline would visibly change the answer.
const losslessPageSize = 100

// searchPage runs one Search with an explicit page size and returns the
// streamed records and the table, whichever the query produces.
func searchPage(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string, limit int64) ([]*gastrologv1.Record, *gastrologv1.TableResult) {
	t.Helper()
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr, Limit: limit},
	}))
	if err != nil {
		t.Fatalf("Search %q: %v", expr, err)
	}
	var records []*gastrologv1.Record
	var table *gastrologv1.TableResult
	for stream.Receive() {
		records = append(records, stream.Msg().Records...)
		if tr := stream.Msg().TableResult; tr != nil {
			table = tr
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Search %q: stream error: %v", expr, err)
	}
	return records, table
}

// singleCell returns the one value of a one-row table's named column.
func singleCell(t *testing.T, expr string, table *gastrologv1.TableResult, col string) string {
	t.Helper()
	if table == nil || len(table.Rows) != 1 {
		t.Fatalf("%s: expected one row, got %v", expr, table)
	}
	idx := slices.Index(table.Columns, col)
	if idx < 0 {
		t.Fatalf("%s: no column %q in %v", expr, col, table.Columns)
	}
	return table.Rows[0].Values[idx]
}

// seedSeqRecords appends n records to node, one per second from t0+offset
// with the given stride, each carrying a cluster-unique seq, host and event
// identity so every record is distinguishable from every other. One seq in
// ten (those ending in 7) is of kind "rare".
func seedSeqRecords(node multinodeTestNode, prefix string, n int, t0 time.Time, offset, stride int) {
	ingester := glid.New()
	for i := range n {
		seq := offset + i*stride
		ts := t0.Add(time.Duration(seq) * time.Second)
		kind := "common"
		if seq%10 == 7 {
			kind = "rare"
		}
		node.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			EventID: chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:     fmt.Appendf(nil, "%s-%d", prefix, i),
			Attrs: chunk.Attributes{
				"seq":  strconv.Itoa(seq),
				"host": fmt.Sprintf("%s-host-%d", prefix, i),
				"kind": kind,
			},
		})
	}
}

// A pipeline the coordinator runs over every node's records reads each remote
// vault's whole match set, not the client's page: the page size bounds what
// the coordinator returns, never what the pipeline consumes. The coordinator
// holds no vault, so every record it aggregates came back over the fan-out.
func TestMultiNode_CoordinatorPipelineReadsEveryRemoteRecord(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []mnOption
	}{
		{"in-process forwarding", nil},
		{"loopback gRPC forwarding", []mnOption{WithClusterSearch()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const perNode = 250
			dataNodes := []string{"data-1", "data-2", "data-3"}
			opts := append([]mnOption{WithoutVault("coord")}, tc.opts...)
			h := setupMultiNode(t, append([]string{"coord"}, dataNodes...), opts...)
			t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
			for i, id := range dataNodes {
				seedSeqRecords(h.Node(t, id), id, perNode, t0, i, len(dataNodes))
			}
			total := perNode * len(dataNodes)

			// Cluster order is seq order: seq k is record k/3 of node k%3.
			rawOfSeq := func(seq int) string {
				return fmt.Sprintf("%s-%d", dataNodes[seq%len(dataNodes)], seq/len(dataNodes))
			}

			// Premise: the page size really is below each vault's match set.
			if got, _ := searchPage(t, h.client, "", losslessPageSize); len(got) != losslessPageSize {
				t.Fatalf("plain search returned %d records, want one page of %d", len(got), losslessPageSize)
			}

			tableCases := []struct {
				expr, col, want string
			}{
				{"| stats dcount(host)", "dcount_host", strconv.Itoa(total)},
				{"| stats count", "count", strconv.Itoa(total)},
				{"| stats median(seq)", "median_seq", strconv.FormatFloat(float64(total-1)/2, 'f', -1, 64)},
				{"| stats max(seq)", "max_seq", strconv.Itoa(total - 1)},
				{"| head 350 | stats count", "count", "350"},
				{"| tail 7 | stats count", "count", "7"},
				{"| dedup | stats count", "count", strconv.Itoa(total)},
			}
			for _, c := range tableCases {
				_, table := searchPage(t, h.client, c.expr, losslessPageSize)
				if got := singleCell(t, c.expr, table, c.col); got != c.want {
					t.Errorf("%s = %s, want %s", c.expr, got, c.want)
				}
			}

			var lastFive, topThree, headFourHundred []string
			for seq := total - 5; seq < total; seq++ {
				lastFive = append(lastFive, rawOfSeq(seq))
			}
			for seq := total - 1; seq >= total-3; seq-- {
				topThree = append(topThree, rawOfSeq(seq))
			}
			for seq := range 400 {
				headFourHundred = append(headFourHundred, rawOfSeq(seq))
			}
			recordCases := []struct {
				expr string
				want []string
			}{
				{"| tail 5", lastFive},
				{"| sort -seq | head 3", topThree},
				{"| dedup | head 400", headFourHundred},
			}
			for _, c := range recordCases {
				records, _ := searchPage(t, h.client, c.expr, losslessPageSize)
				if got := rawsOf(records); !slices.Equal(got, c.want) {
					t.Errorf("%s returned %d records %v, want %v", c.expr, len(got), firstFew(got), firstFew(c.want))
				}
			}

			// An uncapped record pipeline still answers one page.
			if records, _ := searchPage(t, h.client, "| sort -seq", losslessPageSize); len(records) != losslessPageSize {
				t.Errorf("| sort -seq returned %d records, want one page of %d", len(records), losslessPageSize)
			} else if got := string(records[0].Raw); got != rawOfSeq(total-1) {
				t.Errorf("| sort -seq starts at %s, want the cluster's highest seq %s", got, rawOfSeq(total-1))
			}
		})
	}
}

// A pipeline whose filtering operators sit ahead of a head reads past the
// head's N: the head counts survivors, not scanned records.
func TestMultiNode_HeadBehindAFilterCountsSurvivors(t *testing.T) {
	t.Parallel()
	for _, layout := range []struct {
		name    string
		noVault string
		vaultOn string
	}{
		{"vault on coordinator", "data-1", "coord"},
		{"vault on data node", "coord", "data-1"},
	} {
		t.Run(layout.name, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithoutVault(layout.noVault, "data-2", "data-3"))
			t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
			seedSeqRecords(h.Node(t, layout.vaultOn), "r", 300, t0, 0, 1)

			// The rare records are seq 7, 17, …, 297: one in ten.
			expr := `| where kind = "rare" | dedup | head 20`
			var want []string
			for seq := 7; seq < 300 && len(want) < 20; seq += 10 {
				want = append(want, fmt.Sprintf("r-%d", seq))
			}
			records, _ := searchPage(t, h.client, expr, losslessPageSize)
			if got := rawsOf(records); !slices.Equal(got, want) {
				t.Errorf("%s returned %d records %v, want %v", expr, len(got), firstFew(got), firstFew(want))
			}
		})
	}
}

// Source-time bounds and position addressing travel with the forwarded
// query: a vault led on another node honours them exactly as a local one does.
func TestMultiNode_SourceBoundsAndPosReachRemoteVaults(t *testing.T) {
	t.Parallel()
	for _, layout := range []struct {
		name    string
		noVault string
		vaultOn string
	}{
		{"vault on coordinator", "data-1", "coord"},
		{"vault on data node", "coord", "data-1"},
	} {
		t.Run(layout.name, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"},
				WithFileVault(layout.vaultOn), WithoutVault(layout.noVault, "data-2", "data-3"))
			const n = 300
			sourceBase, chunkID := plantSourceBlob(t, h.Node(t, layout.vaultOn), n)

			// Source seconds [10, 40) hold records 10..39.
			srcStart := sourceBase.Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
			srcEnd := sourceBase.Add(40 * time.Second).UTC().Format(time.RFC3339Nano)
			var wantAsc []string
			for s := 10; s < 40; s++ {
				wantAsc = append(wantAsc, fmt.Sprintf("r-%d", s))
			}
			window := "source_start=" + srcStart + " source_end=" + srcEnd
			for _, c := range []struct {
				expr string
				want []string
			}{
				{"order=source_ts " + window, wantAsc},
				{"order=source_ts reverse=true " + window, reversed(wantAsc)},
				{window, wantAsc},
			} {
				records, _ := searchPage(t, h.client, c.expr, losslessPageSize)
				if got := rawsOf(records); !slices.Equal(got, c.want) {
					t.Errorf("%s returned %d records %v, want %d: %v", c.expr, len(got), firstFew(got), len(c.want), firstFew(c.want))
				}
			}

			_, table := searchPage(t, h.client, window+" | stats dcount(id)", losslessPageSize)
			if got := singleCell(t, "source window dcount", table, "dcount_id"); got != "30" {
				t.Errorf("source window | stats dcount(id) = %s, want 30", got)
			}

			for _, pos := range []int{0, 7, n - 1} {
				expr := fmt.Sprintf("chunk=%s pos=%d", chunkID, pos)
				records, _ := searchPage(t, h.client, expr, losslessPageSize)
				if got, want := rawsOf(records), []string{fmt.Sprintf("r-%d", pos)}; !slices.Equal(got, want) {
					t.Errorf("%s returned %d records %v, want %v", expr, len(got), firstFew(got), want)
				}
			}
			// A position past the chunk's end addresses nothing.
			expr := fmt.Sprintf("chunk=%s pos=%d", chunkID, n)
			if records, _ := searchPage(t, h.client, expr, losslessPageSize); len(records) != 0 {
				t.Errorf("%s returned %d records, want none", expr, len(records))
			}
		})
	}
}

// plantSourceBlob seals n records on node whose source timestamps run an
// hour behind ingest. Record i is raw "r-<i>" with attr id=<i> at position i,
// with source time sourceBase + i seconds.
func plantSourceBlob(t *testing.T, node multinodeTestNode, n int) (sourceBase time.Time, chunkID string) {
	t.Helper()
	registrar, ok := node.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file vault chunk manager must register external GLCBs")
	}
	ingester := glid.New()
	ingestBase := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	sourceBase = ingestBase.Add(-time.Hour)
	recs := make([]chunk.Record, 0, n)
	for i := range n {
		ingestTS := ingestBase.Add(time.Duration(i) * time.Second)
		recs = append(recs, chunk.Record{
			SourceTS: sourceBase.Add(time.Duration(i) * time.Second),
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Attrs:    chunk.Attributes{"id": strconv.Itoa(i)},
			Raw:      fmt.Appendf(nil, "r-%d", i),
		})
	}
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, node.vaultID, recs)
	if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	return sourceBase, id.String()
}

// A remote vault that fails mid-gather fails the coordinator's pipeline: an
// aggregate over the vaults that answered is a wrong number, not a partial one.
func TestMultiNode_CoordinatorPipelineFailsWhenARemoteVaultFails(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithoutVault("coord"))
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i, id := range []string{"data-1", "data-2", "data-3"} {
		seedSeqRecords(h.Node(t, id), id, 250, t0, i, 3)
	}
	delete(h.remoteSearcher.nodes, "data-2")

	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "| stats dcount(host)", Limit: losslessPageSize},
	}))
	if err != nil {
		return
	}
	for stream.Receive() {
		if tr := stream.Msg().TableResult; tr != nil {
			t.Fatalf("pipeline answered %v with data-2 unreachable; want the query to fail", tr.Rows)
		}
	}
	if stream.Err() == nil {
		t.Fatal("pipeline succeeded with data-2 unreachable; want the query to fail")
	}
}

func reversed(s []string) []string {
	out := slices.Clone(s)
	slices.Reverse(out)
	return out
}

func firstFew(s []string) []string {
	if len(s) > 6 {
		return append(slices.Clone(s[:6]), "…")
	}
	return s
}
