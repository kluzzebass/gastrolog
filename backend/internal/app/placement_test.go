package app

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"

	"github.com/google/uuid"
)

func newTestPlacement(t *testing.T, localNodeID string, livePeers []string) (*placementManager, *cfgmem.Store, *alert.Collector) {
	t.Helper()
	store := cfgmem.NewStore()
	ps := cluster.NewPeerState(60 * time.Second)
	now := time.Now()
	for _, p := range livePeers {
		ps.Update(p, nil, now)
	}
	alerts := alert.New()
	pm := &placementManager{
		cfgStore:    store,
		peerState:   ps,
		alerts:      alerts,
		localNodeID: localNodeID,
		logger:      slog.Default(),
		triggerCh:   make(chan struct{}, 1),
	}
	return pm, store, alerts
}

func tierNode(t *testing.T, store *cfgmem.Store, tierID uuid.UUID) string {
	t.Helper()
	tier, err := store.GetTier(context.Background(), tierID)
	if err != nil {
		t.Fatalf("GetTier(%s): %v", tierID, err)
	}
	if tier == nil {
		t.Fatalf("tier %s not found", tierID)
	}
	return tier.NodeID
}

func hasAlert(alerts *alert.Collector, prefix string) bool {
	for _, a := range alerts.Active() {
		if len(a.ID) >= len(prefix) && a.ID[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// ---------- Basic assignment ----------

func TestPlacementSingleNodeMemoryTier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-1" {
		t.Fatalf("expected node-1, got %q", got)
	}
}

func TestPlacementLocalTierRequiresStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 1})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// Only node-2 has storage class 1.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-2",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "fast", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-2" {
		t.Fatalf("expected node-2, got %q", got)
	}
}

func TestPlacementCloudTierMatchesActiveChunkClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	csID := uuid.Must(uuid.NewV7())
	_ = store.PutCloudService(ctx, config.CloudService{ID: csID, Name: "s3", Provider: "s3", Bucket: "b"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{
		ID: tierID, Name: "cloud", Type: config.TierTypeCloud,
		CloudServiceID: &csID, ActiveChunkClass: 2,
	})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// Only node-2 has storage class 2.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-2",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 2, Name: "ssd", Path: "/cache"}},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-2" {
		t.Fatalf("expected node-2, got %q", got)
	}
}

func TestPlacementMemoryTierAnyNodeEligible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// 3 nodes, no storage configs — memory tier should still be assigned.
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2", "node-3"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	got := tierNode(t, store, tierID)
	if got == "" {
		t.Fatal("expected tier to be assigned, got empty")
	}
}

// ---------- Stability ----------

func TestPlacementStableAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, NodeID: "node-2"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-2" {
		t.Fatalf("expected stable on node-2, got %q", got)
	}
}

func TestPlacementIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)
	first := tierNode(t, store, tierID)

	// Run again — should not change.
	pm.reconcile(ctx)
	second := tierNode(t, store, tierID)

	if first != second {
		t.Fatalf("reconcile is not idempotent: first=%q, second=%q", first, second)
	}
}

func TestPlacementMultipleReconcilesStable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2", "node-3"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)
	assigned := tierNode(t, store, tierID)

	// 10 more reconciles — should stay on the same node.
	for i := 0; i < 10; i++ {
		pm.reconcile(ctx)
		if got := tierNode(t, store, tierID); got != assigned {
			t.Fatalf("reconcile %d changed assignment from %q to %q", i, assigned, got)
		}
	}
}

// ---------- Failure & recovery ----------

func TestPlacementReassignOnNodeDeath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// node-2 is NOT in livePeers → dead.
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, NodeID: "node-2"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-1" {
		t.Fatalf("expected reassignment to node-1, got %q", got)
	}
}

func TestPlacementReassignLocalTierOnNodeDeath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// node-2 dies. node-1 and node-3 alive, but only node-3 has matching storage.
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-3"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 1, NodeID: "node-2"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-3",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-3" {
		t.Fatalf("expected reassignment to node-3, got %q", got)
	}
}

func TestPlacementNodeLosesStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 1, NodeID: "node-1"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// node-1 has no storage areas. node-2 has the right class.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-2",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	// node-1 is alive but ineligible — should reassign to node-2.
	if got := tierNode(t, store, tierID); got != "node-2" {
		t.Fatalf("expected reassignment to eligible node-2, got %q", got)
	}
}

func TestPlacementNoEligibleNodeClearsAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 5, NodeID: "node-1"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected cleared, got %q", got)
	}

	// Alert should be set.
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected tier-unplaced alert")
	}
}

func TestPlacementNoEligibleNodeAlreadyUnassigned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	// Already unassigned, no eligible node.
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 5})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected still unassigned, got %q", got)
	}
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert for unplaceable tier")
	}
}

// ---------- Load balancing ----------

func TestPlacementLoadBalances(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tier1 := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tier1, Name: "t1", Type: config.TierTypeMemory, NodeID: "node-1"})

	tier2 := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tier2, Name: "t2", Type: config.TierTypeMemory})

	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tier1, tier2}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tier2); got != "node-2" {
		t.Fatalf("expected tier2 on node-2 (fewer tiers), got %q", got)
	}
}

func TestPlacementLoadBalancesAcrossThreeNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-a", []string{"node-b", "node-c"})

	var tierIDs []uuid.UUID
	for i := 0; i < 6; i++ {
		tid := uuid.Must(uuid.NewV7())
		_ = store.PutTier(ctx, config.TierConfig{ID: tid, Name: "t", Type: config.TierTypeMemory})
		tierIDs = append(tierIDs, tid)
	}
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: tierIDs})

	pm.reconcile(ctx)

	// Count tiers per node.
	counts := make(map[string]int)
	for _, tid := range tierIDs {
		counts[tierNode(t, store, tid)]++
	}

	// With load balancing, no node should have more than 3 tiers (6 / 3 = 2, +1 for randomness).
	for node, count := range counts {
		if count > 3 {
			t.Errorf("node %s has %d tiers, expected at most 3", node, count)
		}
	}
	// All 3 nodes should have at least 1 tier.
	if len(counts) != 3 {
		t.Errorf("expected tiers on 3 nodes, got %d", len(counts))
	}
}

func TestPlacementRandomTiebreak(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-a", []string{"node-b"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	got := tierNode(t, store, tierID)
	if got != "node-a" && got != "node-b" {
		t.Fatalf("expected tier assigned to node-a or node-b, got %q", got)
	}
}

// ---------- Orphaned / edge cases ----------

func TestPlacementOrphanedTierIgnored(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "orphan", Type: config.TierTypeMemory})
	// No vault references this tier.

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected orphaned tier unassigned, got %q", got)
	}
}

func TestPlacementEmptyConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, _, _ := newTestPlacement(t, "node-1", nil)

	// No tiers, no vaults — should not panic.
	pm.reconcile(ctx)
}

func TestPlacementVaultWithNoTiers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "empty", TierIDs: nil})

	pm.reconcile(ctx)
	// No panic, no error.
}

func TestPlacementUnknownTierType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "weird", Type: "quantum"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	// Unknown type → nodeEligible returns false → no eligible node.
	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected unknown type tier unassigned, got %q", got)
	}
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert for unplaceable unknown-type tier")
	}
}

func TestPlacementLocalTierStorageClassZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	// StorageClass 0 is invalid — nodeHasStorageClass returns false for class 0.
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 0})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-1",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 0, Name: "zero", Path: "/z"}},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected StorageClass 0 tier unassigned, got %q", got)
	}
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert")
	}
}

func TestPlacementCloudTierActiveChunkClassZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	csID := uuid.Must(uuid.NewV7())
	_ = store.PutCloudService(ctx, config.CloudService{ID: csID, Name: "s3", Provider: "s3", Bucket: "b"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{
		ID: tierID, Name: "cloud", Type: config.TierTypeCloud,
		CloudServiceID: &csID, ActiveChunkClass: 0,
	})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "" {
		t.Fatalf("expected ActiveChunkClass 0 tier unassigned, got %q", got)
	}
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert")
	}
}

// ---------- Alert lifecycle ----------

func TestPlacementAlertClearedWhenPlaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 1})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// First reconcile: no eligible node → alert set.
	pm.reconcile(ctx)
	if !hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert after first reconcile")
	}

	// Add matching storage area → now eligible.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-1",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-1" {
		t.Fatalf("expected placed on node-1, got %q", got)
	}
	if hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected alert to be cleared after placement")
	}
}

func TestPlacementAlertClearedOnStableAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, NodeID: "node-1"})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// Pre-set an alert manually.
	alerts.Set("tier-unplaced:"+tierID.String(), alert.Warning, "test", "stale alert")

	pm.reconcile(ctx)

	// Tier is correctly assigned → alert should be cleared.
	if hasAlert(alerts, "tier-unplaced:") {
		t.Fatal("expected stale alert to be cleared")
	}
}

// ---------- Multi-vault / shared tiers ----------

func TestPlacementTierSharedByMultipleVaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "shared", Type: config.TierTypeMemory})

	// Two vaults reference the same tier.
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v1", TierIDs: []uuid.UUID{tierID}})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v2", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-1" {
		t.Fatalf("expected assigned, got %q", got)
	}
}

func TestPlacementMultipleTiersDifferentTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	memTier := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: memTier, Name: "mem", Type: config.TierTypeMemory})

	localTier := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: localTier, Name: "local", Type: config.TierTypeFile, StorageClass: 1})

	_ = store.PutVault(ctx, config.VaultConfig{
		ID: uuid.Must(uuid.NewV7()), Name: "v",
		TierIDs: []uuid.UUID{memTier, localTier},
	})

	// Only node-2 has the storage class.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-2",
		Areas:  []config.StorageArea{{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	// Memory tier: either node. Local tier: must be node-2.
	memNode := tierNode(t, store, memTier)
	localNode := tierNode(t, store, localTier)

	if memNode == "" {
		t.Fatal("memory tier should be assigned")
	}
	if localNode != "node-2" {
		t.Fatalf("local tier should be on node-2, got %q", localNode)
	}
}

// ---------- Multiple storage areas on one node ----------

func TestPlacementNodeWithMultipleStorageClasses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	tier1 := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tier1, Name: "fast", Type: config.TierTypeFile, StorageClass: 1})

	tier2 := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tier2, Name: "slow", Type: config.TierTypeFile, StorageClass: 3})

	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tier1, tier2}})

	// node-1 has both classes.
	_ = store.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: "node-1",
		Areas: []config.StorageArea{
			{ID: uuid.Must(uuid.NewV7()), StorageClass: 1, Name: "nvme", Path: "/fast"},
			{ID: uuid.Must(uuid.NewV7()), StorageClass: 3, Name: "hdd", Path: "/slow"},
		},
	})

	pm.reconcile(ctx)

	if got := tierNode(t, store, tier1); got != "node-1" {
		t.Fatalf("fast tier: expected node-1, got %q", got)
	}
	if got := tierNode(t, store, tier2); got != "node-1" {
		t.Fatalf("slow tier: expected node-1, got %q", got)
	}
}

// ---------- Nil alerts (no panic) ----------

func TestPlacementNilAlerts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)
	pm.alerts = nil // no alert collector

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	// Should not panic.
	pm.reconcile(ctx)

	if got := tierNode(t, store, tierID); got != "node-1" {
		t.Fatalf("expected node-1, got %q", got)
	}
}

// ---------- nodeHasStorageClass unit tests ----------

func TestNodeHasStorageClass(t *testing.T) {
	t.Parallel()
	nscs := []config.NodeStorageConfig{
		{NodeID: "n1", Areas: []config.StorageArea{
			{StorageClass: 1}, {StorageClass: 3},
		}},
		{NodeID: "n2", Areas: []config.StorageArea{
			{StorageClass: 2},
		}},
	}

	tests := []struct {
		nodeID string
		class  uint32
		want   bool
	}{
		{"n1", 1, true},
		{"n1", 3, true},
		{"n1", 2, false},
		{"n2", 2, true},
		{"n2", 1, false},
		{"n3", 1, false},  // unknown node
		{"n1", 0, false},  // class 0 always false
		{"", 1, false},    // empty node ID
	}

	for _, tt := range tests {
		got := nodeHasStorageClass(nscs, tt.nodeID, tt.class)
		if got != tt.want {
			t.Errorf("nodeHasStorageClass(%q, %d) = %v, want %v", tt.nodeID, tt.class, got, tt.want)
		}
	}
}

// ---------- Replication / secondary placement ----------

func TestPlacementRF2AssignsSecondary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, ReplicationFactor: 2})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	tier, _ := store.GetTier(ctx, tierID)
	if tier.NodeID == "" {
		t.Fatal("expected primary assigned")
	}
	if len(tier.SecondaryNodeIDs) != 1 {
		t.Fatalf("expected 1 secondary, got %d", len(tier.SecondaryNodeIDs))
	}
	if tier.SecondaryNodeIDs[0] == tier.NodeID {
		t.Error("secondary should not be the same as primary")
	}
}

func TestPlacementRF1NoSecondaries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, ReplicationFactor: 1})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	tier, _ := store.GetTier(ctx, tierID)
	if len(tier.SecondaryNodeIDs) != 0 {
		t.Errorf("expected 0 secondaries for RF=1, got %d", len(tier.SecondaryNodeIDs))
	}
}

func TestPlacementRF3InsufficientNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", []string{"node-2"})

	tierID := uuid.Must(uuid.NewV7())
	_ = store.PutTier(ctx, config.TierConfig{ID: tierID, Name: "mem", Type: config.TierTypeMemory, ReplicationFactor: 3})
	_ = store.PutVault(ctx, config.VaultConfig{ID: uuid.Must(uuid.NewV7()), Name: "v", TierIDs: []uuid.UUID{tierID}})

	pm.reconcile(ctx)

	tier, _ := store.GetTier(ctx, tierID)
	// RF=3 needs 2 secondaries, but only 1 other node available.
	if len(tier.SecondaryNodeIDs) != 1 {
		t.Errorf("expected 1 secondary (max available), got %d", len(tier.SecondaryNodeIDs))
	}
	if !hasAlert(alerts, "tier-underreplicated:") {
		t.Error("expected underreplicated alert")
	}
}
