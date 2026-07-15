package glcb_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
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
	wDirect, err := glcb.NewWriter(chunkID, vaultID, workDir)
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
	wStaging, err := glcb.NewWriter(chunkID, vaultID, workDir)
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

// TestWriterAddViewMatchesAddByteForByte proves the two frame encoders
// (Add via appendRecordFrame, AddView via appendRecordFrameView) and the
// two emit paths (direct, staging) all produce byte-identical GLCBs for
// the same logical records. GLCB is durable on-disk format: divergence
// between any of these build paths is a format bug (gastrolog-3ieb26).
func TestWriterAddViewMatchesAddByteForByte(t *testing.T) {
	t.Parallel()
	chunkID, vaultID, records := testRecords()
	workDir := t.TempDir()

	views := make([]record.View, len(records))
	for i, rec := range records {
		attrsWire, err := record.Attributes(rec.Attrs).Encode()
		if err != nil {
			t.Fatal(err)
		}
		views[i] = record.View{
			EventID: record.EventID{
				IngesterID: rec.EventID.IngesterID,
				NodeID:     rec.EventID.NodeID,
				IngestTS:   rec.EventID.IngestTS,
				IngestSeq:  rec.EventID.IngestSeq,
			},
			SourceTS:  rec.SourceTS,
			IngestTS:  rec.IngestTS,
			WriteTS:   rec.WriteTS,
			AttrsWire: attrsWire,
			Raw:       rec.Raw,
		}
	}

	addRecords := func(w *glcb.Writer) error {
		for _, rec := range records {
			if err := w.Add(rec); err != nil {
				return err
			}
		}
		return nil
	}
	addViews := func(w *glcb.Writer) error {
		for _, v := range views {
			if err := w.AddView(v); err != nil {
				return err
			}
		}
		return nil
	}

	builds := []struct {
		name   string
		direct bool
		add    func(*glcb.Writer) error
	}{
		{"add-direct", true, addRecords},
		{"add-staging", false, addRecords},
		{"addview-direct", true, addViews},
		{"addview-staging", false, addViews},
	}

	var want []byte
	for _, b := range builds {
		got := buildBlobBytes(t, workDir, b.name+".glcb", b.direct, chunkID, vaultID, b.add)
		if want == nil {
			want = got
			continue
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("%s GLCB bytes differ from %s", b.name, builds[0].name)
		}
	}
}

// buildBlobBytes builds one GLCB via the direct (BindOutput) or staging
// build and returns the finished blob's bytes.
func buildBlobBytes(t *testing.T, workDir, name string, direct bool, chunkID chunk.ChunkID, vaultID glid.GLID, add func(*glcb.Writer) error) []byte {
	t.Helper()
	path := filepath.Join(workDir, name)
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := glcb.NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		t.Fatal(err)
	}
	if direct {
		if err := w.BindOutput(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := add(w); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(out); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
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
	w, err := glcb.NewWriter(chunkID, vaultID, workDir)
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
	w, err := glcb.NewWriter(chunkID, vaultID, workDir)
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
