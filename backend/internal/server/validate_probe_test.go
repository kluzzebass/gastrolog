package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	fileattr "gastrolog/internal/index/file/attr"
	filejson "gastrolog/internal/index/file/json"
	filekv "gastrolog/internal/index/file/kv"
	filetoken "gastrolog/internal/index/file/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	sysmem "gastrolog/internal/system/memory"
)

// fileVaultWithOneSealedChunk registers a file-backed vault on a fresh
// orchestrator holding one external sealed GLCB, and returns the blob path.
func fileVaultWithOneSealedChunk(t *testing.T, alerts alert.Sink) (*orchestrator.Orchestrator, glid.GLID, chunk.ChunkID, string) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "node-local", SystemLoader: sysmem.NewStore(), Alerts: alerts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orch.Stop() })
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	im := indexfile.NewManager(dir, []index.Indexer{
		filetoken.NewIndexer(dir, cm, nil),
		fileattr.NewIndexer(dir, cm, nil),
		filekv.NewIndexer(dir, cm, nil),
		filejson.NewIndexer(dir, cm, nil),
	}, nil, cm)
	vaultID, id := glid.New(), chunk.NewChunkID()

	blobDir := filepath.Join(t.TempDir(), id.String())
	if err := os.MkdirAll(blobDir, 0o750); err != nil {
		t.Fatal(err)
	}
	w, err := glcb.NewWriter(id, vaultID, blobDir)
	if err != nil {
		t.Fatal(err)
	}
	ing := glid.New()
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	const n = 6
	for i := range n {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		if err := w.Add(chunk.Record{SourceTS: ts, IngestTS: ts, WriteTS: ts, EventID: chunk.EventID{IngesterID: ing, IngestTS: ts, IngestSeq: uint32(i + 1)}, Raw: []byte("r")}); err != nil { //nolint:gosec // G115: tiny test counts
			t.Fatal(err)
		}
	}
	path := filepath.Join(blobDir, glcb.BlobFilename)
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	size, err := w.WriteTo(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	info := chunk.ExternalGLCBInfo{RecordCount: n, DiskBytes: size, IngestStart: base, IngestEnd: base.Add((n - 1) * time.Millisecond), WriteStart: base, WriteEnd: base.Add((n - 1) * time.Millisecond)}
	toc := w.TOC()
	if e, ok := toc.Find(glcb.SectionIngestTSIndex); ok {
		info.IngestIdxOffset, info.IngestIdxSize = e.Offset, e.Size
	}
	if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}
	if err := im.BuildIndexes(context.Background(), id); err != nil {
		t.Fatalf("build indexes: %v", err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, cm, im, query.New(cm, im, nil)))
	return orch, vaultID, id, path
}

// Validate reads each chunk on its own and then searches the whole vault the
// way a query does; a chunk the search cannot read is named in the report.
func TestValidateVaultLocalProbesTheVaultLikeASearch(t *testing.T) {
	orch, vaultID, id, path := fileVaultWithOneSealedChunk(t, nil)
	metas, err := orch.ListLocalChunkMetas(vaultID)
	if err != nil || len(metas) != 1 {
		t.Fatalf("metas=%d err=%v", len(metas), err)
	}

	healthy := ValidateVaultLocal(context.Background(), orch, vaultID, metas, "n")
	if !healthy.GetValid() || len(healthy.GetIssues()) != 0 {
		t.Fatalf("healthy vault reported valid=%v issues=%v chunks=%v", healthy.GetValid(), healthy.GetIssues(), healthy.GetChunks())
	}

	if err := os.Truncate(path, 8); err != nil {
		t.Fatal(err)
	}
	broken := ValidateVaultLocal(context.Background(), orch, vaultID, metas, "n")
	if broken.GetValid() {
		t.Fatal("vault with an unreadable chunk reported valid")
	}
	var chunkIssues []string
	for _, cv := range broken.GetChunks() {
		if chunk.ChunkID(glid.FromBytes(cv.GetChunkId())) == id {
			chunkIssues = cv.GetIssues()
		}
	}
	joined := strings.Join(chunkIssues, " | ")
	perChunkRead := strings.Contains(joined, "cannot open cursor") || strings.Contains(joined, "read error")
	if !perChunkRead || !strings.Contains(joined, "search cannot read this chunk") {
		t.Fatalf("chunk issues = %v, want both the per-chunk read failure and the search probe finding", chunkIssues)
	}
	if len(broken.GetIssues()) != 0 {
		t.Fatalf("probe failure was attributed to the chunk, so no vault-level issue is expected; got %v", broken.GetIssues())
	}
}
