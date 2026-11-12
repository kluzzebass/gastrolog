package segmentation_test

import (
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
)

// HoldsRecords tells an origin whether letting go of a vault would leave acked
// records behind: the open working segment always exists, so only its record
// count, and records still queued for it, count.
func TestManagerHoldsRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	var syncs atomic.Uint32
	var in chan<- segmentation.Input
	mgr, _ := startManager(t, segmentation.Config{
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
		OnSync:          func() { syncs.Add(1) },
	}, func(t *testing.T, mgr *segmentation.Manager) {
		t.Helper()
		var err error
		in, err = mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{})
		if err != nil {
			t.Fatal(err)
		}
	})

	if holds, registered := mgr.HoldsRecords(vaultID); holds || !registered {
		t.Fatalf("empty open segment: holds=%v registered=%v, want false true", holds, registered)
	}

	in <- segmentation.Input{Record: sampleRecord(0, time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC))}
	waitSync(t, mgr, &syncs, 1)
	if holds, registered := mgr.HoldsRecords(vaultID); !holds || !registered {
		t.Fatalf("durable record in the open segment: holds=%v registered=%v, want true true", holds, registered)
	}

	mgr.UnregisterVault(vaultID)
	if holds, registered := mgr.HoldsRecords(vaultID); holds || registered {
		t.Fatalf("after unregister: holds=%v registered=%v, want false false", holds, registered)
	}
	completed, err := paths.ListSegmentIDs(paths.CompletedDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(completed) != 1 {
		t.Fatalf("completed segments after unregister = %d, want the record's segment", len(completed))
	}
}

func TestManagerHoldsRecordsUnknownVault(t *testing.T) {
	t.Parallel()
	mgr, _ := segmentation.New(segmentation.Config{})
	if holds, registered := mgr.HoldsRecords(glid.New()); holds || registered {
		t.Fatalf("unknown vault: holds=%v registered=%v, want false false", holds, registered)
	}
}
