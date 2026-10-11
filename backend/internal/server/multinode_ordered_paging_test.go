package server_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// pagingWindow brackets every record the paging fixtures plant, on both
// timestamp axes.
const pagingWindow = "start=2025-06-15T09:00:00Z end=2025-06-15T11:00:00Z"

var (
	pagingIngestBase = time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	pagingSourceBase = time.Date(2025, 6, 15, 9, 30, 0, 0, time.UTC)
)

var pagingOrders = []string{"", "reverse=true", "order=source_ts", "order=source_ts reverse=true"}

// plantSealedChunk registers recs as one sealed chunk on node's file vault —
// the read path every sealed chunk takes, timestamp indexes included.
func plantSealedChunk(t *testing.T, node multinodeTestNode, recs []chunk.Record) {
	t.Helper()
	registrar, ok := node.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatalf("node %s: vault must be file-backed to plant sealed chunks", node.nodeID)
	}
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, node.vaultID, recs)
	if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("node %s: register chunk: %v", node.nodeID, err)
	}
}

// seedTiedAcrossVaults plants groups of records that share one IngestTS and
// one SourceTS across every vault in the harness: each vault holds one or two
// records of every group, the second in a separate sealed chunk, so ties sit
// between chunks of one vault and between vaults alike. Source order is a
// permutation of ingest order, so the two orderings page through different
// sequences. Every record carries its own event identity, as ingest mints for
// every record. Returns the raw payloads planted.
func seedTiedAcrossVaults(t *testing.T, h *multiNodeHarness, nodeIDs []string, groups int) []string {
	t.Helper()
	var want []string
	v := 0
	for _, id := range nodeIDs {
		n := h.Node(t, id)
		if n.vaultID.IsZero() {
			continue
		}
		ingester := glid.New()
		seq := uint32(0)
		chunks := make([][]chunk.Record, 2)
		for g := range groups {
			ingestTS := pagingIngestBase.Add(time.Duration(g) * time.Second)
			sourceTS := pagingSourceBase.Add(time.Duration((g*7)%groups) * time.Second)
			for i := range 1 + (g+v)%2 {
				seq++
				raw := fmt.Sprintf("%s-g%02d-%d", id, g, i)
				chunks[i] = append(chunks[i], chunk.Record{
					SourceTS: sourceTS,
					IngestTS: ingestTS,
					WriteTS:  ingestTS,
					EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: seq},
					Raw:      []byte(raw),
				})
				want = append(want, raw)
			}
		}
		for _, recs := range chunks {
			plantSealedChunk(t, n, recs)
		}
		v++
	}
	return want
}

// assertPagedExactlyOnce pages expr through the coordinator and requires every
// planted record exactly once, in non-decreasing (or, reversed, non-increasing)
// order on the ordering axis across every page boundary.
func assertPagedExactlyOnce(t *testing.T, h *multiNodeHarness, expr string, pageSize int64, want []string) {
	t.Helper()
	recs := searchPaged(t, h.client, expr, pageSize)
	seen := make(map[string]int, len(recs))
	for _, r := range recs {
		seen[string(r.Raw)]++
	}
	var lost, dup []string
	for _, raw := range want {
		if seen[raw] == 0 {
			lost = append(lost, raw)
		}
	}
	for raw, n := range seen {
		if n > 1 {
			dup = append(dup, fmt.Sprintf("%s×%d", raw, n))
		}
	}
	if len(lost) > 0 || len(dup) > 0 || len(recs) != len(want) {
		t.Fatalf("paged %d records, want %d: lost %d %v…, repeated %d %v…", len(recs), len(want), len(lost), head(lost, 5), len(dup), head(dup, 5))
	}
	reverse := strings.Contains(expr, "reverse=true")
	axis := func(r *gastrologv1.Record) time.Time { return r.GetIngestTs().AsTime() }
	if strings.Contains(expr, "order=source_ts") {
		axis = func(r *gastrologv1.Record) time.Time { return r.GetSourceTs().AsTime() }
	}
	for i := 1; i < len(recs); i++ {
		a, b := axis(recs[i-1]), axis(recs[i])
		if (!reverse && b.Before(a)) || (reverse && b.After(a)) {
			t.Fatalf("records %d and %d are out of order: %s at %v then %s at %v", i-1, i, recs[i-1].Raw, a, recs[i].Raw, b)
		}
	}
}

// pageEveryOrderAndSize runs assertPagedExactlyOnce for every ordering and for
// page sizes that put boundaries on every record, mid-group and across groups.
func pageEveryOrderAndSize(t *testing.T, h *multiNodeHarness, want []string) {
	t.Helper()
	for _, ord := range pagingOrders {
		for _, size := range []int64{1, 4, 7} {
			t.Run(fmt.Sprintf("%q/limit=%d", ord, size), func(t *testing.T) {
				assertPagedExactlyOnce(t, h, pagingWindow+" "+ord, size, want)
			})
		}
	}
}

// Paging through records whose timestamps tie within and across vaults hands
// back every record exactly once, in order, whatever the ordering, the page
// size, and whether the coordinator's own vault takes part.
func TestMultiNode_PagingTiedAcrossVaultsDeliversEveryRecordOnce(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	topologies := []struct {
		name string
		opts []mnOption
	}{
		{"remote vaults only", []mnOption{WithoutVault("coord"), WithFileVault(nodes[1:]...)}},
		{"remote vaults only, loopback gRPC forward", []mnOption{WithoutVault("coord"), WithFileVault(nodes[1:]...), WithClusterSearch()}},
		{"coordinator vault and remote vaults", []mnOption{WithFileVault(nodes...)}},
		{"coordinator vault and remote vaults, loopback gRPC forward", []mnOption{WithFileVault(nodes...), WithClusterSearch()}},
	}
	for _, topo := range topologies {
		t.Run(topo.name, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, nodes, topo.opts...)
			want := seedTiedAcrossVaults(t, h, nodes, 9)
			if len(want) < 21 {
				t.Fatalf("planted %d records; the test needs at least 21", len(want))
			}
			if got := len(searchAll(t, h.client, pagingWindow)); got != len(want) {
				t.Fatalf("unpaged search returned %d records, want %d", got, len(want))
			}
			pageEveryOrderAndSize(t, h, want)
		})
	}
}

// A record without an event identity still pages exactly once in both
// directions: the cursor that names it excludes it.
func TestMultiNode_PagingIdentityLessRecordsDeliversEveryRecordOnce(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	h := setupMultiNode(t, nodes, WithoutVault("coord"), WithFileVault(nodes[1:]...))
	var want []string
	for v, id := range nodes[1:] {
		var recs []chunk.Record
		for i := range 7 {
			k := v*7 + i
			ingestTS := pagingIngestBase.Add(time.Duration(k) * time.Second)
			raw := fmt.Sprintf("%s-%d", id, i)
			recs = append(recs, chunk.Record{
				SourceTS: pagingSourceBase.Add(time.Duration(k*5%21) * time.Second),
				IngestTS: ingestTS,
				WriteTS:  ingestTS,
				Raw:      []byte(raw),
			})
			want = append(want, raw)
		}
		plantSealedChunk(t, h.Node(t, id), recs)
	}
	if got := len(searchAll(t, h.client, pagingWindow)); got != len(want) {
		t.Fatalf("unpaged search returned %d records, want %d", got, len(want))
	}
	pageEveryOrderAndSize(t, h, want)
}
