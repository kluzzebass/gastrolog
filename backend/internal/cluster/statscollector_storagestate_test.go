package cluster

// Coverage for gastrolog-3cobq4: the StatsCollector must copy a node's
// LOCALLY-hosted storage disk-guard snapshots into the broadcast NodeStats
// as StorageState entries — the wire shape the storage inspector's
// entity-list and per-node views both read. Mirrors
// statscollector_diskguard_test.go's shape for the sibling vault-ID
// broadcast.

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func TestStatsCollector_BroadcastsStorageState(t *testing.T) {
	t.Parallel()
	storageID := glid.New()
	vaultA, vaultB := glid.New(), glid.New()
	sampledAt := time.Now().Add(-2 * time.Second)

	provider := &stubStatsProvider{
		storages: []StatsStorageSnapshot{{
			ID:             storageID.String(),
			Name:           "fast-ssd",
			Node:           "node-a",
			Path:           "/data/fast",
			StorageClass:   2,
			WarnExpr:       "",     // inherits the node default
			FloorExpr:      "5GiB", // explicit override
			WarnBytes:      40 << 30,
			FloorBytes:     5 << 30,
			FreeBytes:      3 << 30,
			TotalBytes:     400 << 30,
			SampledAt:      sampledAt,
			WarnVerdict:    false,
			ProtectVerdict: true,
			PlacedVaultIDs: []glid.GLID{vaultA, vaultB},
		}},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	stats := collector.CollectLocalTick(time.Now())

	if len(stats.Storages) != 1 {
		t.Fatalf("want 1 storage on the wire, got %d", len(stats.Storages))
	}
	got := stats.Storages[0]

	if string(got.Id) != string(storageID.ToProto()) {
		t.Fatalf("Id must be raw GLID bytes (matching FileStorage.Id/VaultInfo.Id convention), got %v", got.Id)
	}
	if got.Name != "fast-ssd" || got.NodeName != "node-a" || got.Path != "/data/fast" || got.StorageClass != 2 {
		t.Fatalf("identity fields wrong: %+v", got)
	}
	if !got.WarnInherited {
		t.Fatal("empty WarnExpr must report WarnInherited=true — never left for the caller to infer")
	}
	if got.FloorInherited {
		t.Fatal("explicit FloorExpr must report FloorInherited=false")
	}
	if got.FloorExpr != "5GiB" {
		t.Fatalf("FloorExpr must round-trip verbatim, got %q", got.FloorExpr)
	}
	if got.WarnBytes != 40<<30 || got.FloorBytes != 5<<30 || got.FreeBytes != 3<<30 || got.TotalBytes != 400<<30 {
		t.Fatalf("resolved byte fields wrong: %+v", got)
	}
	if got.WarnVerdict || !got.ProtectVerdict {
		t.Fatalf("verdicts must pass through verbatim (server-computed, never re-derived here): got warn=%v protect=%v", got.WarnVerdict, got.ProtectVerdict)
	}
	if got.SampledAt == nil || !got.SampledAt.AsTime().Equal(sampledAt) {
		t.Fatalf("SampledAt must round-trip, got %v want %v", got.SampledAt, sampledAt)
	}
	if len(got.PlacedVaultIds) != 2 {
		t.Fatalf("placed vault ids = %d, want 2", len(got.PlacedVaultIds))
	}
	gotVaults := map[string]bool{string(got.PlacedVaultIds[0]): true, string(got.PlacedVaultIds[1]): true}
	if !gotVaults[string(vaultA.ToProto())] || !gotVaults[string(vaultB.ToProto())] {
		t.Fatalf("placed vault ids don't match the source vaults: %v", got.PlacedVaultIds)
	}

	// A node with no local storages broadcasts an empty list, not stale entries.
	provider.storages = nil
	stats = collector.CollectLocalTick(time.Now())
	if len(stats.Storages) != 0 {
		t.Fatalf("cleared storage state must broadcast empty, got %d", len(stats.Storages))
	}
}

// TestStatsCollector_SkipsUnparsableStorageID pins the defensive fallback:
// a StorageSnapshot.ID that doesn't parse as a GLID (only reachable from a
// synthetic test fixture — refreshVaultDiskGuards always derives it from
// fs.ID.String()) is skipped rather than published with an empty/zero
// identity that FindStorageState could never match.
func TestStatsCollector_SkipsUnparsableStorageID(t *testing.T) {
	t.Parallel()
	provider := &stubStatsProvider{
		storages: []StatsStorageSnapshot{{ID: "not-a-glid", Name: "bogus"}},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	stats := collector.CollectLocalTick(time.Now())
	if len(stats.Storages) != 0 {
		t.Fatalf("an unparsable storage ID must be skipped, got %d entries", len(stats.Storages))
	}
}
