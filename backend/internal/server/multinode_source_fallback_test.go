package server_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// seedMixedSourceTimes plants two sealed chunks on every vault node. Every
// third record has no source timestamp; the others carry source times that
// run backwards over the same seconds the ingest times run forwards, so the
// two kinds interleave and tie on the source axis within and across vaults.
func seedMixedSourceTimes(t *testing.T, h *multiNodeHarness, nodeIDs []string, perNode int) []chunk.Record {
	t.Helper()
	var all []chunk.Record
	for v, id := range nodeIDs {
		n := h.Node(t, id)
		ingester := glid.New()
		chunks := make([][]chunk.Record, 2)
		for i := range perNode {
			ingestTS := pagingIngestBase.Add(time.Duration(i*len(nodeIDs)+v) * time.Second)
			rec := chunk.Record{
				IngestTS: ingestTS,
				WriteTS:  ingestTS,
				EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
				Raw:      fmt.Appendf(nil, "%s-%02d", id, i),
			}
			if i%3 != 0 {
				rec.SourceTS = pagingIngestBase.Add(time.Duration((perNode-1-i)*len(nodeIDs)+v) * time.Second)
			}
			chunks[i%2] = append(chunks[i%2], rec)
			all = append(all, rec)
		}
		for _, recs := range chunks {
			plantSealedChunk(t, n, recs)
		}
	}
	return all
}

// mixedSourceOrder is recs' payloads in canonical source order, computed
// here: SourceTS, or IngestTS for a record without one, then EventID.
func mixedSourceOrder(recs []chunk.Record, reverse bool) []string {
	key := func(r chunk.Record) time.Time {
		if r.SourceTS.IsZero() {
			return r.IngestTS
		}
		return r.SourceTS
	}
	sorted := slices.Clone(recs)
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

// A source-ordered search over vaults on three nodes, whose records partly
// carry no source timestamp, returns every record exactly once in the same
// order whichever of the four nodes coordinates it — including one without a
// vault — whole and paged, both directions.
func TestMultiNode_SourceOrderPlacesRecordsWithoutSourceTSByIngestTSFromEveryNode(t *testing.T) {
	t.Parallel()
	nodes := []string{"n1", "n2", "n3", "n4"}
	vaultNodes := nodes[1:]
	for k := range nodes {
		order := slices.Concat(nodes[k:], nodes[:k])
		t.Run("coordinator "+order[0], func(t *testing.T) {
			h := setupMultiNode(t, order, WithoutVault("n1"), WithFileVault(vaultNodes...))
			recs := seedMixedSourceTimes(t, h, vaultNodes, 9)
			stored := make(map[string]time.Time, len(recs))
			for _, r := range recs {
				stored[string(r.Raw)] = r.SourceTS
			}
			for _, reverse := range []bool{false, true} {
				expr := pagingWindow + " order=source_ts"
				if reverse {
					expr += " reverse=true"
				}
				want := mixedSourceOrder(recs, reverse)
				var got []string
				for _, r := range searchAll(t, h.client, expr) {
					got = append(got, string(r.Raw))
					if stamp := stored[string(r.Raw)]; stamp.IsZero() != (r.GetSourceTs() == nil) || (!stamp.IsZero() && !r.GetSourceTs().AsTime().Equal(stamp)) {
						t.Errorf("%s served with source timestamp %v, stored %v", r.Raw, r.GetSourceTs(), stamp)
					}
				}
				if !slices.Equal(got, want) {
					t.Errorf("%q:\n got  %v\n want %v", expr, got, want)
				}
				for _, size := range []int64{4, 7} {
					var paged []string
					for _, r := range searchPaged(t, h.client, expr, size) {
						paged = append(paged, string(r.Raw))
					}
					if !slices.Equal(paged, want) {
						t.Errorf("%q paged by %d:\n got  %v\n want %v", expr, size, paged, want)
					}
				}
			}
		})
	}
}
