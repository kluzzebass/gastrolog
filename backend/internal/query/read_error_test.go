package query_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// writeBlob builds <root>/<id>/data.glcb with n records and returns the path
// with the registration info a chunk manager needs.
func writeBlob(t *testing.T, root string, id chunk.ChunkID, vaultID glid.GLID, n int) (string, chunk.ExternalGLCBInfo) {
	t.Helper()
	dir := filepath.Join(root, id.String())
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	w, err := glcb.NewWriter(id, vaultID, dir)
	if err != nil {
		t.Fatal(err)
	}
	ing := glid.New()
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range n {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		if err := w.Add(chunk.Record{SourceTS: ts, IngestTS: ts, WriteTS: ts, EventID: chunk.EventID{IngesterID: ing, IngestTS: ts, IngestSeq: uint32(i + 1)}, Raw: []byte("r")}); err != nil { //nolint:gosec // G115: tiny test counts
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, glcb.BlobFilename)
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	size, err := w.WriteTo(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	info := chunk.ExternalGLCBInfo{RecordCount: int64(n), DiskBytes: size, IngestStart: base, IngestEnd: base.Add(time.Duration(n-1) * time.Millisecond), WriteStart: base, WriteEnd: base.Add(time.Duration(n-1) * time.Millisecond)}
	toc := w.TOC()
	if e, ok := toc.Find(glcb.SectionIngestTSIndex); ok {
		info.IngestIdxOffset, info.IngestIdxSize = e.Offset, e.Size
	}
	if e, ok := toc.Find(glcb.SectionSourceTSIndex); ok {
		info.SourceIdxOffset, info.SourceIdxSize = e.Offset, e.Size
	}
	return path, info
}

// A chunk whose bytes cannot be opened surfaces as a VaultReadError naming
// the vault and chunk, so callers can attribute the failure; cancellation is
// the caller's doing and is not dressed up as one.
func TestSearchReportsAnUnreadableChunkAsVaultReadError(t *testing.T) {
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	vaultID := glid.New()
	id := chunk.NewChunkID()
	path, info := writeBlob(t, t.TempDir(), id, vaultID, 5)
	if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatal(err)
	}
	eng := query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)

	if err := os.Truncate(path, 8); err != nil {
		t.Fatal(err)
	}
	it, _ := eng.Search(context.Background(), query.Query{}, nil)
	var got error
	for _, err := range it {
		if err != nil {
			got = err
			break
		}
	}
	var re *query.VaultReadError
	if !errors.As(got, &re) {
		t.Fatalf("search error = %v, want a VaultReadError", got)
	}
	if re.ChunkID != id {
		t.Fatalf("read error names chunk %s, want %s", re.ChunkID, id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it, _ = eng.Search(ctx, query.Query{}, nil)
	for _, err := range it {
		if err != nil {
			if errors.As(err, &re) {
				t.Fatalf("cancellation was reported as a vault read error: %v", err)
			}
			break
		}
	}
}
