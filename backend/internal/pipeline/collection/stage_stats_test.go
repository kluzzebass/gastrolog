package collection_test

// Coverage for gastrolog-10n6k8: home-side ingress counters (records/bytes
// arriving in head/) via both the remote-pull and local-promotion paths.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
)

func TestCollectStatsCountsPulledSegments(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	pull := newMemoryPull()
	pull.Put(segID, writeSegmentBytes(t, vaultID, segID, "stage counter"))
	log := &staticLog{}
	log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: log, Pull: pull, Receipts: &recordingReceipts{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CollectOnce(context.Background(), vaultID); err != nil {
		t.Fatal(err)
	}

	stats := mgr.CollectStats()
	if len(stats) != 1 || stats[0].VaultID != vaultID {
		t.Fatalf("CollectStats = %+v, want one entry for %s", stats, vaultID)
	}
	if stats[0].CollectedRecords == 0 || stats[0].CollectedBytes == 0 {
		t.Fatalf("collected = %d records / %d bytes, want > 0 after pull",
			stats[0].CollectedRecords, stats[0].CollectedBytes)
	}
}

func TestNoteLocalHeadArrivalCounts(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	root := t.TempDir()

	mgr := collection.New(collection.Config{})
	if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
		Log: &staticLog{}, Pull: newMemoryPull(), Receipts: &recordingReceipts{},
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate distribution's local promotion: segment lands in head/ directly.
	data := writeSegmentBytes(t, vaultID, segID, "local promote")
	if err := os.MkdirAll(filepath.Dir(paths.HeadSegment(root, segID)), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.HeadSegment(root, segID), data, 0o600); err != nil {
		t.Fatal(err)
	}

	mgr.NoteLocalHeadArrival(vaultID, segID)
	stats := mgr.CollectStats()
	if stats[0].CollectedRecords == 0 || stats[0].CollectedBytes == 0 {
		t.Fatalf("collected = %d records / %d bytes, want > 0 after local promotion",
			stats[0].CollectedRecords, stats[0].CollectedBytes)
	}

	// Unknown vault and unreadable segment are silent no-ops.
	mgr.NoteLocalHeadArrival(glid.New(), segID)
	mgr.NoteLocalHeadArrival(vaultID, glid.New())
}
