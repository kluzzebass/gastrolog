package segment

// Internal test for the in-memory index capture's hard cap
// (gastrolog-oin19g): past memIndexEntryCap the capture is dropped and
// Finalize degrades to the disk-scan build — the capture must never grow
// RAM with the file, regardless of the caller's close policy.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/record"
)

// NOT t.Parallel(): overrides the package-level cap.
func TestMemCaptureCapDropsToDiskScan(t *testing.T) {
	orig := memIndexEntryCap
	memIndexEntryCap = 8
	defer func() { memIndexEntryCap = orig }()

	dir := t.TempDir()
	meta := Meta{ID: glid.New(), VaultID: glid.New()}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	frame := func(seq uint32) Frame {
		rec := &record.Record{
			SourceTS: ts.Add(time.Duration(seq) * time.Millisecond),
			IngestTS: ts,
			EventID: record.EventID{
				IngesterID: glid.New(),
				NodeID:     glid.New(),
				IngestTS:   ts,
				IngestSeq:  seq,
			},
			Raw: []byte("cap test"),
		}
		body, err := encodeFrame(rec, ts)
		if err != nil {
			t.Fatal(err)
		}
		return Frame{Rec: rec, Body: body}
	}
	frames := make([]Frame, 20) // > cap of 8
	for i := range frames {
		frames[i] = frame(uint32(i)) //nolint:gosec // G115: small test index
	}

	capped := filepath.Join(dir, "capped")
	sf, err := Create(capped, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.AppendFrames(frames); err != nil {
		t.Fatal(err)
	}
	if !sf.memCaptureOff || sf.memEntries != nil {
		t.Fatalf("capture not dropped past cap: off=%v entries=%d", sf.memCaptureOff, len(sf.memEntries))
	}
	if err := sf.Finalize(); err != nil {
		t.Fatalf("Finalize via disk-scan fallback: %v", err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	// Same frames with the capture intact must produce an identical file.
	memIndexEntryCap = orig
	uncapped := filepath.Join(dir, "uncapped")
	sf2, err := Create(uncapped, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := sf2.AppendFrames(frames); err != nil {
		t.Fatal(err)
	}
	if err := sf2.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf2.Close(); err != nil {
		t.Fatal(err)
	}

	a, err := os.ReadFile(capped) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(uncapped) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("capped (disk-scan) and uncapped (in-memory) finalize diverge: %d vs %d bytes", len(a), len(b))
	}
}
