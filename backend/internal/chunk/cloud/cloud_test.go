package cloud_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
)

func TestRoundTrip(t *testing.T) {
	chunkID := chunk.NewChunkID()
	vaultID := uuid.New()
	ingesterID := uuid.New()

	now := time.Now().Truncate(time.Nanosecond)
	records := []chunk.Record{
		{
			SourceTS: now.Add(-2 * time.Second),
			IngestTS: now.Add(-1 * time.Second),
			WriteTS:  now,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now.Add(-1 * time.Second), IngestSeq: 1},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info"},
			Raw:      []byte("first log message"),
		},
		{
			SourceTS: now.Add(-1 * time.Second),
			IngestTS: now,
			WriteTS:  now.Add(1 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now, IngestSeq: 2},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "error"},
			Raw:      []byte("second log message"),
		},
		{
			// No SourceTS.
			IngestTS: now.Add(1 * time.Second),
			WriteTS:  now.Add(2 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now.Add(1 * time.Second), IngestSeq: 3},
			Attrs:    chunk.Attributes{"host": "db-1"},
			Raw:      []byte("third message without source ts"),
		},
	}

	// Write.
	w := cloud.NewWriter(chunkID, vaultID)
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	t.Logf("blob size: %d bytes (3 records)", buf.Len())

	// Verify writer metadata.
	wm := w.Meta()
	if wm.RecordCount != 3 {
		t.Errorf("writer meta count = %d, want 3", wm.RecordCount)
	}
	if wm.ChunkID != chunkID {
		t.Errorf("writer meta chunkID mismatch")
	}
	if wm.VaultID != vaultID {
		t.Errorf("writer meta vaultID mismatch")
	}

	// Read.
	rd, err := cloud.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rd.Close()

	// Verify reader metadata.
	rm := rd.Meta()
	if rm.RecordCount != 3 {
		t.Errorf("reader meta count = %d, want 3", rm.RecordCount)
	}
	if rm.ChunkID != chunkID {
		t.Errorf("reader meta chunkID mismatch")
	}
	if rm.VaultID != vaultID {
		t.Errorf("reader meta vaultID mismatch")
	}
	if rm.SourceStart.IsZero() {
		t.Error("reader meta sourceStart is zero")
	}

	// Read all records and compare.
	for i, want := range records {
		got, err := rd.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if !got.WriteTS.Equal(want.WriteTS) {
			t.Errorf("[%d] WriteTS = %v, want %v", i, got.WriteTS, want.WriteTS)
		}
		if !got.IngestTS.Equal(want.IngestTS) {
			t.Errorf("[%d] IngestTS = %v, want %v", i, got.IngestTS, want.IngestTS)
		}
		if !got.SourceTS.Equal(want.SourceTS) {
			t.Errorf("[%d] SourceTS = %v, want %v", i, got.SourceTS, want.SourceTS)
		}
		if got.EventID.IngesterID != want.EventID.IngesterID {
			t.Errorf("[%d] IngesterID mismatch", i)
		}
		if got.EventID.IngestSeq != want.EventID.IngestSeq {
			t.Errorf("[%d] IngestSeq = %d, want %d", i, got.EventID.IngestSeq, want.EventID.IngestSeq)
		}
		if string(got.Raw) != string(want.Raw) {
			t.Errorf("[%d] Raw = %q, want %q", i, got.Raw, want.Raw)
		}
		for k, v := range want.Attrs {
			if got.Attrs[k] != v {
				t.Errorf("[%d] Attrs[%q] = %q, want %q", i, k, got.Attrs[k], v)
			}
		}
		if len(got.Attrs) != len(want.Attrs) {
			t.Errorf("[%d] Attrs count = %d, want %d", i, len(got.Attrs), len(want.Attrs))
		}
	}

	// Next should return ErrNoMoreRecords.
	_, err = rd.Next()
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}
}

func TestEmptyBlob(t *testing.T) {
	w := cloud.NewWriter(chunk.NewChunkID(), uuid.New())

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	rd, err := cloud.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rd.Close()

	if rd.Meta().RecordCount != 0 {
		t.Errorf("expected 0 records, got %d", rd.Meta().RecordCount)
	}

	_, err = rd.Next()
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}
}
