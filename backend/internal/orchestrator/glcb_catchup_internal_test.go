package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// buildCatchupTestGLCB writes a real segment and builds a valid GLCB from
// it, returning the GLCB path and its record count.
func buildCatchupTestGLCB(t *testing.T, records int) (string, int64) {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()

	segPath := filepath.Join(t.TempDir(), segID.String())
	sf, err := segment.Create(segPath, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		rec := record.Record{
			EventID: record.EventID{
				IngesterID: glid.New(),
				NodeID:     glid.New(),
				IngestTS:   base.Add(time.Duration(i) * time.Second),
				IngestSeq:  uint32(i),
			},
			SourceTS: base.Add(time.Duration(i) * time.Second),
			IngestTS: base.Add(time.Duration(i) * time.Second),
			Attrs:    record.Attributes{"k": "v"},
			Raw:      []byte("catchup-record"),
		}
		if err := sf.Append(&rec, base); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	glcbPath := filepath.Join(t.TempDir(), "data.glcb")
	res, err := chunking.BuildGLCBFile(glcbPath, chunking.BuildGLCBInput{
		ChunkID: chunk.NewChunkID(),
		VaultID: vaultID,
		Refs: []chunking.SpanRef{{
			Path: segPath,
			Span: chunking.Span{SegmentID: segID, Start: 0, Count: uint32(records)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return glcbPath, int64(res.RecordCount)
}

// copyToTemp copies src into dir as a fresh temp file, standing in for the
// pulled-bytes staging file that verifyAndPromoteGLCB consumes.
func copyToTemp(t *testing.T, src, dir string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(dir, ".glcb.pull.*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func TestVerifyAndPromoteGLCBValid(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count}
	if err := verifyAndPromoteGLCB(tmp, dest, e); err != nil {
		t.Fatalf("verifyAndPromoteGLCB: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("promoted GLCB missing: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file survived promotion (err=%v)", err)
	}
}

func TestVerifyAndPromoteGLCBRecordCountMismatch(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	dir := t.TempDir()
	tmp := copyToTemp(t, src, dir)
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count + 1}
	if err := verifyAndPromoteGLCB(tmp, dest, e); err == nil {
		t.Fatal("expected record-count mismatch error, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("mismatched GLCB was promoted (err=%v)", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file not cleaned up after mismatch (err=%v)", err)
	}
}

func TestVerifyAndPromoteGLCBCorruptBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".glcb.pull.corrupt")
	if err := os.WriteFile(tmp, []byte("not a GLCB at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: 1}
	if err := verifyAndPromoteGLCB(tmp, dest, e); err == nil {
		t.Fatal("expected verification error for corrupt blob, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("corrupt GLCB was promoted (err=%v)", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file not cleaned up after corrupt blob (err=%v)", err)
	}
}

// TestVerifyAndPromoteGLCBTruncated covers the torn-transfer case: a valid
// GLCB cut short mid-file must fail verification, not promote.
func TestVerifyAndPromoteGLCBTruncated(t *testing.T) {
	t.Parallel()
	src, count := buildCatchupTestGLCB(t, 3)
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".glcb.pull.torn")
	if err := os.WriteFile(tmp, data[:len(data)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "data.glcb")

	e := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), RecordCount: count}
	if err := verifyAndPromoteGLCB(tmp, dest, e); err == nil {
		t.Fatal("expected verification error for truncated blob, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("truncated GLCB was promoted (err=%v)", err)
	}
}
