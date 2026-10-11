package server_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// plantShuffledBlob seals records on node whose ingest and source timestamps
// both run out of physical order: positions 0..4 carry base plus 10s, 0s, 5s,
// 3s and 1s on both axes.
func plantShuffledBlob(t *testing.T, node multinodeTestNode) time.Time {
	t.Helper()
	registrar, ok := node.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file vault chunk manager must register external GLCBs")
	}
	ingester := glid.New()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	offsets := []time.Duration{10 * time.Second, 0, 5 * time.Second, 3 * time.Second, time.Second}
	recs := make([]chunk.Record, 0, len(offsets))
	for i, off := range offsets {
		ts := base.Add(off)
		recs = append(recs, chunk.Record{
			SourceTS: ts,
			IngestTS: ts,
			WriteTS:  base.Add(time.Duration(i) * time.Second),
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "p%d", i),
		})
	}
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, node.vaultID, recs)
	if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	return base
}

// An ingest lower bound searched from a node without the vault returns every
// in-window record of a remote vault's chunk, wherever the record sits
// physically.
func TestMultiNode_LowerBoundKeepsRecordsOutOfPhysicalOrder(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"},
		WithFileVault("data-2"), WithoutVault("coord", "data-1", "data-3"))
	base := plantShuffledBlob(t, h.Node(t, "data-2"))
	bound := base.Add(3 * time.Second).UTC().Format(time.RFC3339Nano)
	want := []string{"p0", "p2", "p3"}
	for _, expr := range []string{
		"start=" + bound,
		"start=" + bound + " reverse=true",
		"start=" + bound + " order=source_ts",
		"start=" + bound + " order=source_ts reverse=true",
	} {
		var got []string
		for _, rec := range searchAll(t, h.client, expr) {
			got = append(got, string(rec.GetRaw()))
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: got %v, want %v", expr, got, want)
		}
	}
}
