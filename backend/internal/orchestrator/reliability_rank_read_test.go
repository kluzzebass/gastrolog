package orchestrator_test

import (
	"testing"
	"time"
)

// TestOrchRel_RankLookupsAcrossVoters covers the FSM-grounded IndexReader
// (gastrolog-nlepn) on a real cluster: vault B is homed on nodes {0,1,2};
// node 3 is the ingest origin and a vault-ctl voter without an instance.
// Every home — leader or follower — must resolve exact IngestTS ranks for
// the sealed chunks from its locally materialized bytes; the instance-less
// voter must report the lookup unresolvable rather than fabricate an answer
// (its residual is the FSM estimate, gastrolog-1952x).
func TestOrchRel_RankLookupsAcrossVoters(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}
	outsiderID := h.nodeIDs[3]

	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(outsiderID, total, "rank-read")
	sealed := h.waitSealedRecords(v, h.nodeIDs[0], total)
	if len(sealed) != 2 {
		t.Fatalf("expected 2 sealed chunks, got %d", len(sealed))
	}
	h.waitGLCBsOnHomes(v, homeIdxs, sealed)

	// Every home resolves exact ranks — the bytes are local (chunk manager
	// registration or the chunk-root GLCB tier; either way the answer comes
	// from the chunk's own ITSI section).
	for _, idx := range homeIdxs {
		node := h.nodes[h.nodeIDs[idx]]
		ir := node.orch.IndexReader()
		for _, e := range sealed {
			if e.IngestStart.IsZero() || e.IngestEnd.IsZero() {
				t.Fatalf("sealed entry %s missing ingest bounds", e.ID)
			}
			rank, ok := ir.FindIngestRank(e.ID, e.IngestStart)
			if !ok || rank != 0 {
				t.Errorf("%s: FindIngestRank(%s, IngestStart) = (%d, %v), want (0, true)", node.label, e.ID, rank, ok)
			}
			rank, ok = ir.FindIngestRank(e.ID, e.IngestEnd)
			if !ok {
				t.Errorf("%s: FindIngestRank(%s, IngestEnd) unresolvable on a home", node.label, e.ID)
			} else if int64(rank) >= e.RecordCount {
				t.Errorf("%s: FindIngestRank(%s, IngestEnd) = %d, want < %d", node.label, e.ID, rank, e.RecordCount)
			}
			pos, ok := ir.FindIngestPos(e.ID, e.IngestStart)
			if !ok || pos != 0 {
				t.Errorf("%s: FindIngestPos(%s, IngestStart) = (%d, %v), want (0, true)", node.label, e.ID, pos, ok)
			}
			// Past all entries: not found, same answer the ITSI gives.
			if got, ok := ir.FindIngestRank(e.ID, e.IngestEnd.Add(time.Minute)); ok {
				t.Errorf("%s: FindIngestRank(%s, past end) = (%d, true), want unresolved", node.label, e.ID, got)
			}
		}
	}

	// The instance-less voter resolves the manifest (see the 3w8qj test) but
	// holds no chunk bytes: interior rank lookups must be unresolvable,
	// never invented.
	outIR := h.nodes[outsiderID].orch.IndexReader()
	for _, e := range sealed {
		if got, ok := outIR.FindIngestRank(e.ID, e.IngestEnd); ok {
			t.Errorf("outsider: FindIngestRank(%s, IngestEnd) = (%d, true), want unresolvable without local bytes", e.ID, got)
		}
		if got, ok := outIR.FindIngestPos(e.ID, e.IngestStart); ok {
			t.Errorf("outsider: FindIngestPos(%s, IngestStart) = (%d, true), want unresolvable without local bytes", e.ID, got)
		}
	}

	// gastrolog-enfwd: the byte-free FSM-metadata boundary answer DOES
	// resolve on the instance-less voter — a timestamp strictly before a
	// sealed monotonic chunk's IngestStart is exactly rank 0, the same
	// answer the chunk's own ITSI section gives on the homes.
	for _, e := range sealed {
		if !e.IngestTSMonotonic {
			t.Fatalf("fixture chunk %s not monotonic; sequential ingest should be", e.ID)
		}
		before := e.IngestStart.Add(-time.Second)
		rank, ok := outIR.FindIngestRank(e.ID, before)
		if !ok || rank != 0 {
			t.Errorf("outsider: FindIngestRank(%s, before IngestStart) = (%d, %v), want (0, true) from FSM metadata", e.ID, rank, ok)
		}
		pos, ok := outIR.FindIngestPos(e.ID, before)
		if !ok || pos != 0 {
			t.Errorf("outsider: FindIngestPos(%s, before IngestStart) = (%d, %v), want (0, true) from FSM metadata", e.ID, pos, ok)
		}
		// The homes give the identical answer from real bytes — the
		// metadata tier never diverges from the ITSI truth.
		homeIR := h.nodes[h.nodeIDs[0]].orch.IndexReader()
		if hRank, hOK := homeIR.FindIngestRank(e.ID, before); !hOK || hRank != rank {
			t.Errorf("home vs voter rank divergence for %s: home=(%d,%v) voter=(%d,%v)", e.ID, hRank, hOK, rank, ok)
		}
	}
}
