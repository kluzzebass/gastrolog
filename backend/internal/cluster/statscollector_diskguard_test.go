package cluster

// Coverage for the disk-guard cross-node admission transport (gastrolog-5jobl5
// / gastrolog-20ywka): the StatsCollector must copy a node's locally
// protected / size-capped vault IDs into the broadcast NodeStats, which is
// how every other node's admission gate learns to refuse those vaults. The
// multi-node server harness mocks the stat providers, so it cannot exercise
// this path — this drives the real collector.

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func TestStatsCollector_BroadcastsDiskGuardVaultIDs(t *testing.T) {
	t.Parallel()
	protectedA, protectedB := glid.New(), glid.New()
	capped := glid.New()
	provider := &stubStatsProvider{
		storageProtected: []glid.GLID{protectedA, protectedB},
		sizeCapped:       []glid.GLID{capped},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	stats := collector.CollectLocalTick(time.Now())

	got := map[string]bool{}
	for _, id := range stats.StorageProtectedVaultIds {
		got[string(id)] = true
	}
	if !got[string(protectedA.ToProto())] || !got[string(protectedB.ToProto())] {
		t.Fatalf("disk_protected_vault_ids missing entries: got %d ids", len(stats.StorageProtectedVaultIds))
	}
	if len(stats.SizeCappedVaultIds) != 1 || string(stats.SizeCappedVaultIds[0]) != string(capped.ToProto()) {
		t.Fatalf("size_capped_vault_ids = %d ids, want the one capped vault", len(stats.SizeCappedVaultIds))
	}

	// A node with nothing protected broadcasts empty lists (not stale entries).
	provider.storageProtected = nil
	provider.sizeCapped = nil
	stats = collector.CollectLocalTick(time.Now())
	if len(stats.StorageProtectedVaultIds) != 0 || len(stats.SizeCappedVaultIds) != 0 {
		t.Fatalf("cleared guard state must broadcast empty lists, got %d/%d",
			len(stats.StorageProtectedVaultIds), len(stats.SizeCappedVaultIds))
	}
}
