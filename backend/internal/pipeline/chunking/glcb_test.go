package chunking_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/record"
)

func openGLCB(t *testing.T, path string) *chunkcloud.Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Close() })
	return rd
}

func TestBuildGLCBSingleSegmentRoundTrip(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "alpha"),
		makeRecord(1, base.Add(time.Second), "beta"),
	})

	chunkID := chunk.NewChunkID()
	refs := []chunking.SpanRef{{
		Path: path,
		Span: chunking.Span{SegmentID: segID, Start: 0, Count: 2},
	}}

	var buf bytes.Buffer
	result, err := chunking.BuildGLCB(&buf, chunking.BuildGLCBInput{
		ChunkID: chunkID,
		VaultID: vaultID,
		Refs:    refs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", result.RecordCount)
	}
	if result.BlobDigest == ([32]byte{}) {
		t.Fatal("empty blob digest")
	}

	glcbPath := filepath.Join(t.TempDir(), chunkcloud.BlobFilename)
	if err := os.WriteFile(glcbPath, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	rd := openGLCB(t, glcbPath)

	for i := range uint32(2) {
		got, err := rd.ReadRecord(i)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		wantRaw := "alpha"
		if i == 1 {
			wantRaw = "beta"
		}
		if string(got.Raw) != wantRaw {
			t.Fatalf("record %d raw = %q, want %q", i, got.Raw, wantRaw)
		}
		if got.Attrs["k"] != "v" {
			t.Fatalf("record %d attrs = %+v", i, got.Attrs)
		}
	}
}

func TestBuildGLCBKWayMergeRoundTrip(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segA := glid.New()
	segB := glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{
		makeRecord(0, base.Add(3*time.Second), "a3"),
		makeRecord(0, base, "a0"),
	})
	pathB := writeSegment(t, segB, vaultID, []record.Record{
		makeRecord(0, base.Add(2*time.Second), "b2"),
		makeRecord(0, base.Add(time.Second), "b1"),
	})

	refs := []chunking.SpanRef{
		{Path: pathA, Span: chunking.Span{SegmentID: segA, Start: 0, Count: 2}},
		{Path: pathB, Span: chunking.Span{SegmentID: segB, Start: 0, Count: 2}},
	}
	glcbPath := filepath.Join(t.TempDir(), chunkcloud.BlobFilename)
	result, err := chunking.BuildGLCBFile(glcbPath, chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: vaultID,
		Refs:    refs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 4 {
		t.Fatalf("RecordCount = %d, want 4", result.RecordCount)
	}

	rd := openGLCB(t, glcbPath)

	want := []string{"a0", "b1", "b2", "a3"}
	for i, wantRaw := range want {
		got, err := rd.ReadRecord(uint32(i))
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if string(got.Raw) != wantRaw {
			t.Fatalf("record %d = %q, want %q", i, got.Raw, wantRaw)
		}
	}
}

func TestBuildGLCBDeterministic(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segA, segB := glid.New(), glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{makeRecord(0, base, "a")})
	pathB := writeSegment(t, segB, vaultID, []record.Record{makeRecord(0, base.Add(time.Second), "b")})

	refsAB := []chunking.SpanRef{
		{Path: pathA, Span: chunking.Span{SegmentID: segA, Start: 0, Count: 1}},
		{Path: pathB, Span: chunking.Span{SegmentID: segB, Start: 0, Count: 1}},
	}
	refsBA := slices.Clone(refsAB)
	slices.Reverse(refsBA)

	in := chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: vaultID,
		Refs:    refsAB,
	}

	var bufAB, bufBA bytes.Buffer
	resAB, err := chunking.BuildGLCB(&bufAB, in)
	if err != nil {
		t.Fatal(err)
	}
	in.Refs = refsBA
	resBA, err := chunking.BuildGLCB(&bufBA, in)
	if err != nil {
		t.Fatal(err)
	}

	if resAB.BlobDigest != resBA.BlobDigest {
		t.Fatalf("digest AB=%x BA=%x", resAB.BlobDigest, resBA.BlobDigest)
	}
	if !bytes.Equal(bufAB.Bytes(), bufBA.Bytes()) {
		t.Fatal("GLCB bytes differ across span ref order")
	}

	// Rebuild from AB refs — same chunk ID and vault for byte-identical output.
	var bufAgain bytes.Buffer
	in.Refs = refsAB
	resAgain, err := chunking.BuildGLCB(&bufAgain, in)
	if err != nil {
		t.Fatal(err)
	}
	if resAgain.BlobDigest != resAB.BlobDigest {
		t.Fatalf("rebuild digest = %x, want %x", resAgain.BlobDigest, resAB.BlobDigest)
	}
}

func TestBuildGLCBRejectsEmptyMerge(t *testing.T) {
	t.Parallel()
	_, err := chunking.BuildGLCB(&bytes.Buffer{}, chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: glid.New(),
	})
	if err == nil {
		t.Fatal("expected error for empty merge")
	}
}
