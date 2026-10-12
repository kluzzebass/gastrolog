package orchestrator_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// submitMixedSourceRecords pushes count records through the pipeline routing
// path on nodeID, one second apart in ingest time. Every third record has no
// source timestamp; the others carry source times running backwards over the
// same seconds, so the two kinds interleave and tie on the source axis.
func (h *orchRelHarness) submitMixedSourceRecords(nodeID string, count int) []chunk.Record {
	h.t.Helper()
	n := h.nodes[nodeID]
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	ingester := glid.New()
	recs := make([]chunk.Record, 0, count)
	for i := range count {
		ingestTS := base.Add(time.Duration(i) * time.Second)
		rec := chunk.Record{
			IngestTS: ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "mixed-source-%03d", i),
		}
		if i%3 != 0 {
			rec.SourceTS = base.Add(time.Duration(count-1-i) * time.Second)
		}
		ack := make(chan error, 1)
		if err := n.orch.SubmitIngest(context.Background(), rec, ack); err != nil {
			h.t.Fatalf("SubmitIngest %d on %s: %v", i, n.label, err)
		}
		select {
		case err := <-ack:
			if err != nil {
				h.t.Fatalf("SubmitIngest %d on %s: ack: %v", i, n.label, err)
			}
		case <-time.After(orchHarnessEventTimeout):
			h.t.Fatalf("SubmitIngest %d on %s: no ack within %s", i, n.label, orchHarnessEventTimeout)
		}
		recs = append(recs, rec)
	}
	return recs
}

// sourceSearch runs a source-ordered search of v through eng, paging through
// the engine's own resume tokens when limit is positive.
func sourceSearch(t *testing.T, eng *query.Engine, v vaultSpec, reverse bool, limit int) []chunk.Record {
	t.Helper()
	q := vaultQuery(v)
	q.OrderBy = query.OrderBySourceTS
	q.IsReverse = reverse
	q.Limit = limit
	var out []chunk.Record
	var token *query.ResumeToken
	for page := 0; page < 1000; page++ {
		seq, next := eng.Search(context.Background(), q, token)
		for rec, err := range seq {
			if err != nil {
				t.Fatalf("source-ordered search of vault %s: %v", v.label, err)
			}
			out = append(out, rec.Copy())
		}
		if limit <= 0 {
			return out
		}
		if token = next(); token == nil {
			return out
		}
	}
	t.Fatal("paging never finished")
	return nil
}

// waitServedLocally waits until nodeID's local engine — leader or follower
// copy, sealed chunks and the open one — serves exactly want of v's records.
// Progress metric: the served count.
func (h *orchRelHarness) waitServedLocally(v vaultSpec, nodeID string, want int) {
	h.t.Helper()
	n := h.nodes[nodeID]
	h.waitProgress(fmt.Sprintf("vault %s: %d records served locally by %s", v.label, want, n.label), 50*time.Millisecond,
		func() (string, bool) {
			seq, _ := n.orch.LocalVaultQueryEngine().Search(context.Background(), vaultQuery(v), nil)
			got := 0
			for _, err := range seq {
				if err != nil {
					return fmt.Sprintf("served=%d err=%v", got, err), false
				}
				got++
			}
			return fmt.Sprintf("served=%d", got), got == want
		}, func() { h.dumpPipelineState(v) })
}

// Records ingested on a node that is not a home, some without a source
// timestamp, land in two sealed chunks and an open one. A source-ordered
// search returns every record exactly once in the same order from every
// home, the records without a source timestamp placed by their ingest
// timestamp and still served without one — whole and paged, both directions.
// Every home's local copy is searched, and the leader through the engine a
// forwarded search runs.
func TestOrchPipeline_SourceOrderIsIdenticalOnEveryHome(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	homeIdxs := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homeIdxs),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	const open = pipelineChunkMaxRecords / 2
	total := 2*pipelineChunkMaxRecords + open
	sent := h.submitMixedSourceRecords(h.nodeIDs[3], total)
	entries := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	h.waitOpenManifestRecords(v, h.nodeIDs[0], open)
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	stored := make(map[string]time.Time, len(sent))
	for _, r := range sent {
		stored[string(r.Raw)] = r.SourceTS
	}
	key := func(r chunk.Record) time.Time {
		if r.SourceTS.IsZero() {
			return r.IngestTS
		}
		return r.SourceTS
	}
	want := func(reverse bool) []string {
		sorted := slices.Clone(sent)
		slices.SortFunc(sorted, func(a, b chunk.Record) int {
			c := key(a).Compare(key(b))
			if c == 0 {
				c = a.EventID.Compare(b.EventID)
			}
			if reverse {
				return -c
			}
			return c
		})
		out := make([]string, len(sorted))
		for i, r := range sorted {
			out[i] = string(r.Raw)
		}
		return out
	}

	type server struct {
		name string
		eng  *query.Engine
	}
	var servers []server
	for _, idx := range homeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		h.waitServedLocally(v, h.nodeIDs[idx], total)
		servers = append(servers, server{n.label + " local copy", n.orch.LocalVaultQueryEngine()})
	}
	leader := h.nodes[h.nodeIDs[homeIdxs[0]]]
	servers = append(servers, server{leader.label + " as leader", leader.orch.LeaderVaultQueryEngine()})

	for _, srv := range servers {
		for _, reverse := range []bool{false, true} {
			for _, limit := range []int{0, 4} {
				got := sourceSearch(t, srv.eng, v, reverse, limit)
				raws := make([]string, len(got))
				for i, r := range got {
					raws[i] = string(r.Raw)
					if !r.SourceTS.Equal(stored[raws[i]]) {
						t.Errorf("%s served %s with SourceTS %v, ingested with %v", srv.name, r.Raw, r.SourceTS, stored[raws[i]])
					}
				}
				if w := want(reverse); !slices.Equal(raws, w) {
					t.Errorf("%s reverse=%v limit=%d:\n got  %v\n want %v", srv.name, reverse, limit, raws, w)
				}
			}
		}
	}
}
