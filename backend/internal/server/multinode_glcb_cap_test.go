package server_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// A coordinator fanning out to a data node whose vault holds more sealed
// chunks than that node keeps mapped at once. The remote search opens a
// cursor per chunk and holds them for the whole stream, so every mapping
// must survive the cap; on the pre-fix code the 65th open failed and the
// whole query with it.
func TestMultiNode_SearchSpansMoreSealedChunksThanTheMappedBlobCap(t *testing.T) {
	const perChunk = 3
	chunks := chunkfile.MappedBlobCap() + 6

	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"), WithFileVault("data-1"))
	data := h.Node(t, "data-1")
	registrar, ok := data.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file vault chunk manager must register external GLCBs")
	}

	root := t.TempDir()
	ingester := glid.New()
	base := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	for i := range chunks {
		id := chunk.NewChunkID()
		recs := glcbtest.Records(perChunk, base.Add(time.Duration(i)*time.Millisecond*10), ingester, "payload")
		path, info := glcbtest.WriteSealedBlob(t, root, id, data.vaultID, recs)
		if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", i, err)
		}
	}

	// Premise: the data node really holds more chunks than the cap.
	if chunks <= chunkfile.MappedBlobCap() {
		t.Fatalf("test needs more chunks (%d) than the mapped-blob cap (%d)", chunks, chunkfile.MappedBlobCap())
	}

	records := searchAll(t, h.client, "")
	if len(records) != chunks*perChunk {
		t.Fatalf("coordinator returned %d records from the remote vault, want %d", len(records), chunks*perChunk)
	}
	seen := make(map[string]int, chunks)
	for _, rec := range records {
		seen[string(rec.GetRef().GetChunkId())]++
	}
	if len(seen) != chunks {
		t.Fatalf("records came from %d chunks, want %d", len(seen), chunks)
	}
}
