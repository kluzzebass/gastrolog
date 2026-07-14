package chunking_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/record"
)

// openGLCB opens a built GLCB via the production open path
// (OpenMappedBlob + Reader) and returns its record reader.
func openGLCB(t *testing.T, path string) *glcb.Reader {
	t.Helper()
	blob, err := glcb.OpenMappedBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blob.Close() })
	rd, err := blob.Reader()
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

	glcbPath := filepath.Join(t.TempDir(), glcb.BlobFilename)
	result, err := chunking.BuildGLCBFile(glcbPath, chunking.BuildGLCBInput{
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
	glcbPath := filepath.Join(t.TempDir(), glcb.BlobFilename)
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

	workDir := t.TempDir()
	in := chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: vaultID,
		Refs:    refsAB,
	}

	pathAB := filepath.Join(workDir, "ab.glcb")
	resAB, err := chunking.BuildGLCBFile(pathAB, in)
	if err != nil {
		t.Fatal(err)
	}
	dataAB, err := os.ReadFile(pathAB)
	if err != nil {
		t.Fatal(err)
	}

	in.Refs = refsBA
	pathBA := filepath.Join(workDir, "ba.glcb")
	resBA, err := chunking.BuildGLCBFile(pathBA, in)
	if err != nil {
		t.Fatal(err)
	}
	dataBA, err := os.ReadFile(pathBA)
	if err != nil {
		t.Fatal(err)
	}

	if resAB.BlobDigest != resBA.BlobDigest {
		t.Fatalf("digest AB=%x BA=%x", resAB.BlobDigest, resBA.BlobDigest)
	}
	if !slices.Equal(dataAB, dataBA) {
		t.Fatal("GLCB bytes differ across span ref order")
	}

	in.Refs = refsAB
	pathAgain := filepath.Join(workDir, "again.glcb")
	resAgain, err := chunking.BuildGLCBFile(pathAgain, in)
	if err != nil {
		t.Fatal(err)
	}
	if resAgain.BlobDigest != resAB.BlobDigest {
		t.Fatalf("rebuild digest = %x, want %x", resAgain.BlobDigest, resAB.BlobDigest)
	}
}

func TestBuildGLCBRejectsEmptyMerge(t *testing.T) {
	t.Parallel()
	glcbPath := filepath.Join(t.TempDir(), glcb.BlobFilename)
	_, err := chunking.BuildGLCBFile(glcbPath, chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: glid.New(),
	})
	if err == nil {
		t.Fatal("expected error for empty merge")
	}
}
