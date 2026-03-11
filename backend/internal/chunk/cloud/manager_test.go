package cloud_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
)

func skipIfNoEmulators(t *testing.T) {
	t.Helper()
	if os.Getenv("BLOBSTORE_INTEGRATION") == "" {
		t.Skip("set BLOBSTORE_INTEGRATION=1 to run (requires cloud-storage emulators)")
	}
}

func TestManagerImportAndQuery(t *testing.T) {
	skipIfNoEmulators(t)
	ctx := context.Background()

	store, err := blobstore.NewS3(ctx, blobstore.S3Config{
		Bucket:    "blobstore-test",
		Region:    "us-east-1",
		Endpoint:  "http://localhost:9000",
		AccessKey: "gastrolog",
		SecretKey: "gastrolog",
	})
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}

	vaultID := uuid.New()
	mgr := cloud.NewManager(store, vaultID, nil)

	// Build test records.
	ingesterID := uuid.New()
	now := time.Now().Truncate(time.Nanosecond)
	records := []chunk.Record{
		{
			SourceTS: now.Add(-2 * time.Second),
			IngestTS: now.Add(-1 * time.Second),
			WriteTS:  now,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now.Add(-1 * time.Second), IngestSeq: 1},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info"},
			Raw:      []byte("first message"),
		},
		{
			IngestTS: now,
			WriteTS:  now.Add(1 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now, IngestSeq: 2},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "error"},
			Raw:      []byte("second message"),
		},
	}

	// Import records.
	idx := 0
	iter := func() (chunk.Record, error) {
		if idx >= len(records) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		rec := records[idx]
		idx++
		return rec, nil
	}

	meta, err := mgr.ImportRecords(iter)
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}
	if meta.RecordCount != 2 {
		t.Errorf("ImportRecords count = %d, want 2", meta.RecordCount)
	}
	if !meta.Sealed {
		t.Error("ImportRecords should produce sealed chunk")
	}

	// List should find the chunk.
	chunks, err := mgr.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, c := range chunks {
		if c.ID == meta.ID {
			found = true
			if c.RecordCount != 2 {
				t.Errorf("List count = %d, want 2", c.RecordCount)
			}
		}
	}
	if !found {
		t.Error("List did not return imported chunk")
	}

	// Meta should return the same info.
	gotMeta, err := mgr.Meta(meta.ID)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if gotMeta.RecordCount != 2 {
		t.Errorf("Meta count = %d, want 2", gotMeta.RecordCount)
	}

	// OpenCursor and read records back.
	cursor, err := mgr.OpenCursor(meta.ID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	for i, want := range records {
		got, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if ref.Pos != uint64(i) {
			t.Errorf("[%d] Pos = %d", i, ref.Pos)
		}
		if string(got.Raw) != string(want.Raw) {
			t.Errorf("[%d] Raw = %q, want %q", i, got.Raw, want.Raw)
		}
		if got.Attrs["host"] != want.Attrs["host"] {
			t.Errorf("[%d] host = %q, want %q", i, got.Attrs["host"], want.Attrs["host"])
		}
	}

	_, _, err = cursor.Next()
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}

	// Delete.
	if err := mgr.Delete(meta.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// List should be empty for this vault.
	chunks, err = mgr.List()
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	for _, c := range chunks {
		if c.ID == meta.ID {
			t.Error("chunk still present after delete")
		}
	}
}
