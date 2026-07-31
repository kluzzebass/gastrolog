package distribution

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// writeCompletedFile builds one finalized single-record segment under
// working/ and renames it into completed/, mirroring the segmentation writer.
func writeCompletedFile(t *testing.T, root string, vaultID glid.GLID) glid.GLID {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	segID := glid.New()
	workingPath := paths.WorkingSegment(root, segID)
	sf, err := segment.Create(workingPath, segment.Meta{ID: segID, VaultID: vaultID})
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
		},
		Attrs: record.Attributes{"k": "v"},
		Raw:   []byte("stranded"),
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
	if err := os.Rename(workingPath, paths.CompletedSegment(root, segID)); err != nil {
		t.Fatal(err)
	}
	return segID
}

// TestStrandedReadsHeadersOnly proves the rescan cost is proportional to the
// header, not the file: one fixed-header read per unknown segment, zero
// full-verify Opens.
func TestStrandedReadsHeadersOnly(t *testing.T) {
	// Not parallel: asserts on the segment package's process-wide counters.
	root := t.TempDir()
	vaultID := glid.New()
	goodID := writeCompletedFile(t, root, vaultID)

	v, err := newVaultDist(root, VaultConfig{Publisher: &recordingBatchPublisher{}}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	opensBefore := segment.Opens()
	headerReadsBefore := segment.HeaderReads()

	segs := v.stranded(vaultID)
	if len(segs) != 1 || segs[0].SegmentID != goodID {
		t.Fatalf("stranded() = %+v, want the one completed segment", segs)
	}
	if segs[0].Header.IsUnpopulated() {
		t.Fatalf("stranded() header not populated: %+v", segs[0].Header)
	}
	if segs[0].Header.RecordCount != 1 || segs[0].Header.SegmentChecksum == 0 {
		t.Fatalf("stranded() header = %+v", segs[0].Header)
	}

	if d := segment.Opens() - opensBefore; d != 0 {
		t.Errorf("stranded() performed %d full-verify segment.Open calls, want 0", d)
	}
	if d := segment.HeaderReads() - headerReadsBefore; d != 1 {
		t.Errorf("stranded() performed %d header reads, want 1", d)
	}
}

// TestStrandedCorruptHeaderReadAndWarnedOnce proves a completed/ file whose
// fixed header fails to decode is read and warned about exactly once; later
// rescans skip it by segment ID without touching the file.
func TestStrandedCorruptHeaderReadAndWarnedOnce(t *testing.T) {
	// Not parallel: asserts on the segment package's process-wide counters.
	root := t.TempDir()
	vaultID := glid.New()
	goodID := writeCompletedFile(t, root, vaultID)

	corruptID := glid.New()
	garbage := bytes.Repeat([]byte{0xAB}, segment.HeaderSize)
	if err := os.WriteFile(paths.CompletedSegment(root, corruptID), garbage, 0o600); err != nil {
		t.Fatal(err)
	}

	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	v, err := newVaultDist(root, VaultConfig{Publisher: &recordingBatchPublisher{}}, log)
	if err != nil {
		t.Fatal(err)
	}

	warns := func() int {
		return strings.Count(logBuf.String(), "header unreadable")
	}

	// First pass: reads both headers, surfaces only the good segment, warns
	// once about the corrupt one.
	headerReadsBefore := segment.HeaderReads()
	segs := v.stranded(vaultID)
	if len(segs) != 1 || segs[0].SegmentID != goodID {
		t.Fatalf("stranded() pass 1 = %+v, want only the good segment", segs)
	}
	if d := segment.HeaderReads() - headerReadsBefore; d != 2 {
		t.Errorf("pass 1 performed %d header reads, want 2", d)
	}
	if n := warns(); n != 1 {
		t.Fatalf("pass 1 warned %d times, want 1\n%s", n, logBuf.String())
	}

	// Later passes: the corrupt file is remembered by segment ID — never
	// re-read, never re-warned. The good segment is still unknown (nothing
	// prepared it), so it is the only header read.
	headerReadsBefore = segment.HeaderReads()
	segs = v.stranded(vaultID)
	if len(segs) != 1 || segs[0].SegmentID != goodID {
		t.Fatalf("stranded() pass 2 = %+v, want only the good segment", segs)
	}
	if d := segment.HeaderReads() - headerReadsBefore; d != 1 {
		t.Errorf("pass 2 performed %d header reads, want 1 (corrupt file must not be re-read)", d)
	}
	if n := warns(); n != 1 {
		t.Fatalf("pass 2: warned %d times total, want 1\n%s", n, logBuf.String())
	}
}

// recordingBatchPublisher is a no-op publisher for vaultDist construction.
type recordingBatchPublisher struct{}

func (recordingBatchPublisher) Publish(context.Context, Metadata) error { return nil }
