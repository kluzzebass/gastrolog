package collection_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
)

func writeSegmentBytes(t *testing.T, vaultID, segID glid.GLID, raw string) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, segID.String())

	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC)
	rec := &record.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
			IngestSeq:  0,
		},
		Attrs: record.Attributes{"k": "v"},
		Raw:   []byte(raw),
	}
	if err := sf.Append(rec, ts); err != nil {
		t.Fatal(err)
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
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReceiveAndPromoteVerified(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "verified payload")

	prePath, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	headPath, err := collection.PromoteVerified(prePath, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head file: %v", err)
	}
	if _, err := os.Stat(prePath); !os.IsNotExist(err) {
		t.Fatal("pre-head copy should be gone after promote")
	}
	if _, err := os.Stat(paths.PreHeadSegment(root, segID)); !os.IsNotExist(err) {
		t.Fatal("pre-head should be empty after promote")
	}
	sf, err := segment.Open(headPath)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := sf.ReadAll()
	_ = sf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Raw) != "verified payload" {
		t.Fatalf("records = %+v", recs)
	}
}

func TestPromoteVerifiedRejectsCorruptTransfer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	if err := paths.EnsurePreHeadDir(root); err != nil {
		t.Fatal(err)
	}
	prePath := paths.PreHeadSegment(root, segID)
	if err := os.WriteFile(prePath, []byte("not a segment"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := collection.PromoteVerified(prePath, root)
	if !errors.Is(err, collection.ErrCorruptSegment) {
		t.Fatalf("PromoteVerified() = %v, want ErrCorruptSegment", err)
	}
	if _, err := os.Stat(prePath); !os.IsNotExist(err) {
		t.Fatal("corrupt pre-head file should be removed")
	}
	head, err := os.ReadDir(paths.HeadDir(root))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(head) != 0 {
		t.Fatal("head must stay empty when verification fails")
	}
}

func TestPreHeadDoesNotSatisfyHeadInvariant(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	root := t.TempDir()
	segID := glid.New()
	data := writeSegmentBytes(t, vaultID, segID, "still in pre-head")

	if _, err := collection.ReceiveToPreHead(root, segID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	headEntries, err := os.ReadDir(paths.HeadDir(root))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(headEntries) != 0 {
		t.Fatal("segment in pre-head must not appear in head")
	}
}
