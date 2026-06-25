package cloud_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
)

func TestWriterDirectAndStagingProduceIdenticalBlob(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()
	workDir := t.TempDir()

	directPath := filepath.Join(workDir, "direct.glcb")
	directOut, err := os.Create(directPath)
	if err != nil {
		t.Fatal(err)
	}
	wDirect, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wDirect.BindOutput(directOut); err != nil {
		t.Fatal(err)
	}
	wDirect.ReserveRecords(uint32(len(records)))
	for _, rec := range records {
		if err := wDirect.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := wDirect.WriteTo(directOut); err != nil {
		t.Fatal(err)
	}
	if err := directOut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wDirect.Close(); err != nil {
		t.Fatal(err)
	}

	stagingPath := filepath.Join(workDir, "staging.glcb")
	stagingOut, err := os.Create(stagingPath)
	if err != nil {
		t.Fatal(err)
	}
	wStaging, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatal(err)
	}
	wStaging.ReserveRecords(uint32(len(records)))
	for _, rec := range records {
		if err := wStaging.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := wStaging.WriteTo(stagingOut); err != nil {
		t.Fatal(err)
	}
	if err := stagingOut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wStaging.Close(); err != nil {
		t.Fatal(err)
	}

	directBytes, err := os.ReadFile(directPath)
	if err != nil {
		t.Fatal(err)
	}
	stagingBytes, err := os.ReadFile(stagingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(directBytes, stagingBytes) {
		t.Fatal("direct and staging GLCB bytes differ")
	}
}

func TestWriterDirectBuildLeavesNoStagingTemp(t *testing.T) {
	tmpRoot := t.TempDir()
	workDir := t.TempDir()
	t.Setenv("TMPDIR", tmpRoot)

	chunkID, vaultID, records := testRecords()
	out, err := os.CreateTemp(workDir, "data.glcb.*")
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BindOutput(out); err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.WriteTo(out); err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	_ = w.Close()

	if matches, _ := filepath.Glob(filepath.Join(workDir, "glcb-records-*.tmp")); len(matches) != 0 {
		t.Fatalf("unexpected staging temp under workDir: %v", matches)
	}
}

func TestWriterIngestTSNonMonotonicDetected(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, _ := testRecords()
	workDir := t.TempDir()
	out, err := os.CreateTemp(workDir, "data.glcb.*")
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BindOutput(out); err != nil {
		t.Fatal(err)
	}
	ingesterID := glid.New()
	first := chunk.Record{
		IngestTS: time.Unix(0, 2).UTC(),
		WriteTS:  time.Unix(0, 2).UTC(),
		EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: time.Unix(0, 2).UTC(), IngestSeq: 1},
		Raw:      []byte("later-ingest-first-in-output"),
	}
	second := chunk.Record{
		IngestTS: time.Unix(0, 1).UTC(),
		WriteTS:  time.Unix(0, 1).UTC(),
		EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: time.Unix(0, 1).UTC(), IngestSeq: 2},
		Raw:      []byte("earlier-ingest-second"),
	}
	if err := w.Add(first); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(second); err != nil {
		t.Fatal(err)
	}
	if w.IngestTSMonotonic() {
		t.Fatal("expected non-monotonic ingest TS")
	}
	_ = out.Close()
}
