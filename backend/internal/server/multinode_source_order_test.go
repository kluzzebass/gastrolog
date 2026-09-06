package server_test

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// plantLateSourceBlob seals n records on node whose source timestamps run an
// hour behind their ingest timestamps, and returns the ingest window that
// covers them all.
func plantLateSourceBlob(t *testing.T, node multinodeTestNode, n int) (time.Time, time.Time) {
	t.Helper()
	registrar, ok := node.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file vault chunk manager must register external GLCBs")
	}
	ingester := glid.New()
	ingestBase := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	recs := make([]chunk.Record, 0, n)
	for i := range n {
		ingestTS := ingestBase.Add(time.Duration(i) * time.Second)
		recs = append(recs, chunk.Record{
			SourceTS: ingestTS.Add(-time.Hour),
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "r%03d", i),
		})
	}
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, t.TempDir(), id, node.vaultID, recs)
	if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("register: %v", err)
	}
	return ingestBase, ingestBase.Add(time.Hour)
}

// A source-ordered search returns the same records as the default order for
// the same ingest window, whether the vault is on the coordinator (where the
// server fills an unbounded query's window from ingest bounds) or on a data
// node that receives the window in the forwarded query.
func TestMultiNode_SourceOrderHonoursTheIngestWindow(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, tc := range []struct {
		name     string
		vaultOn  string
		noVault  string
		explicit bool
	}{
		{"vault on coordinator, unbounded", "coord", "data-1", false},
		{"vault on coordinator, explicit window", "coord", "data-1", true},
		{"vault on data node, explicit window", "data-1", "coord", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupMultiNode(t, []string{"coord", "data-1"}, WithFileVault(tc.vaultOn), WithoutVault(tc.noVault))
			start, end := plantLateSourceBlob(t, h.Node(t, tc.vaultOn), n)
			window := ""
			if tc.explicit {
				window = fmt.Sprintf("start=%s end=%s ", start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
			}
			if got := len(searchAll(t, h.client, window)); got != n {
				t.Fatalf("default order returned %d records, want %d", got, n)
			}
			for _, ord := range []string{"order=source_ts", "order=source_ts reverse=true"} {
				if got := len(searchAll(t, h.client, window+ord)); got != n {
					t.Errorf("%s%s returned %d records, want %d", window, ord, got, n)
				}
			}
		})
	}
}
