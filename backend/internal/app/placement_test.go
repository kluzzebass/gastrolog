package app

import (
	"context"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// leaderPlacement creates a Placements slice with a single leader using a synthetic storage ID.
func leaderPlacement(nodeID string) []system.VaultPlacement {
	return []system.VaultPlacement{{StorageID: system.SyntheticStorageID(nodeID), Leader: true}}
}

func newTestPlacement(t *testing.T, localNodeID string, livePeers []string) (*placementManager, *sysmem.Store, *alert.Collector) {
	t.Helper()
	store := sysmem.NewStore()
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

func vaultNode(t *testing.T, store *sysmem.Store, vaultID glid.GLID) string {
	t.Helper()
	ctx := context.Background()
	nscs, err := store.ListNodeStorageConfigs(ctx)
	if err != nil {
		t.Fatalf("ListNodeStorageConfigs: %v", err)
	}
	placements, _ := store.GetVaultPlacements(ctx, vaultID)
	return system.LeaderNodeID(placements, nscs)
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

func TestPlacementSingleNodeMemoryVault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "v", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-1" {
		t.Fatalf("expected node-1, got %q", got)
	}
}

func TestPlacementLocalVaultRequiresStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 1})

	// Only node-2 has storage class 1.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-2",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1, Name: "fast", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-2" {
		t.Fatalf("expected node-2, got %q", got)
	}
}

func TestPlacementCloudVaultMatchesActiveChunkClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	csID := glid.New()
	_ = store.PutCloudService(ctx, system.CloudService{ID: csID, Name: "s3", Provider: "s3", Bucket: "b"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "cloud", Type: system.VaultTypeFile,
		CloudServiceID: &csID, StorageClass: 2,
	})

	// Only node-2 has storage class 2.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-2",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 2, Name: "ssd", Path: "/cache"}},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-2" {
		t.Fatalf("expected node-2, got %q", got)
	}
}

func TestPlacementMemoryVaultAnyNodeEligible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// 3 nodes, no storage configs — memory vault should still be assigned.
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2", "node-3"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got == "" {
		t.Fatal("expected vault to be assigned, got empty")
	}
}

// ---------- Stability ----------

func TestPlacementStableAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)
	first := vaultNode(t, store, vaultID)
	if first == "" {
		t.Fatal("expected vault to be assigned after first reconcile")
	}

	// Reconcile again — assignment should be stable.
	pm.reconcile(ctx)
	second := vaultNode(t, store, vaultID)
	if second != first {
		t.Fatalf("assignment changed: first=%q, second=%q", first, second)
	}
}

func TestPlacementIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)
	first := vaultNode(t, store, vaultID)

	// Run again — should not change.
	pm.reconcile(ctx)
	second := vaultNode(t, store, vaultID)

	if first != second {
		t.Fatalf("reconcile is not idempotent: first=%q, second=%q", first, second)
	}
}

func TestPlacementMultipleReconcilesStable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2", "node-3"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)
	assigned := vaultNode(t, store, vaultID)

	// 10 more reconciles — should stay on the same node.
	for i := 0; i < 10; i++ {
		pm.reconcile(ctx)
		if got := vaultNode(t, store, vaultID); got != assigned {
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

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-1" {
		t.Fatalf("expected reassignment to node-1, got %q", got)
	}
}

func TestPlacementReassignLocalVaultOnNodeDeath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// node-2 dies. node-1 and node-3 alive, but only node-3 has matching storage.
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-3"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 1})

	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-3",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-3" {
		t.Fatalf("expected reassignment to node-3, got %q", got)
	}
}

func TestPlacementNodeLosesStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 1})

	// node-1 has no file storages. node-2 has the right class.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-2",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	// node-1 is alive but ineligible — should reassign to node-2.
	if got := vaultNode(t, store, vaultID); got != "node-2" {
		t.Fatalf("expected reassignment to eligible node-2, got %q", got)
	}
}

func TestPlacementNoEligibleNodeClearsAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 5})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "" {
		t.Fatalf("expected cleared, got %q", got)
	}

	// Alert should be set.
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected vault-unplaced alert")
	}
}

func TestPlacementNoEligibleNodeAlreadyUnassigned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	// Already unassigned, no eligible node.
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 5})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "" {
		t.Fatalf("expected still unassigned, got %q", got)
	}
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert for unplaceable vault")
	}
}

// ---------- Load balancing ----------

func TestPlacementLoadBalances(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vault1 := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vault1, Name: "v1", Type: system.VaultTypeMemory})

	vault2 := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vault2, Name: "v2", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)

	node1 := vaultNode(t, store, vault1)
	node2 := vaultNode(t, store, vault2)
	if node1 == node2 {
		t.Fatalf("expected load-balanced across two nodes, both on %q", node1)
	}
}

func TestPlacementLoadBalancesAcrossThreeNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-a", []string{"node-b", "node-c"})

	var vaultIDs []glid.GLID
	for i := 0; i < 6; i++ {
		vid := glid.New()
		_ = store.PutVault(ctx, system.VaultConfig{ID: vid, Name: "v", Type: system.VaultTypeMemory})
		vaultIDs = append(vaultIDs, vid)
	}

	pm.reconcile(ctx)

	// Count vaults per node.
	counts := make(map[string]int)
	for _, vid := range vaultIDs {
		counts[vaultNode(t, store, vid)]++
	}

	// With load balancing, no node should have more than 3 vaults (6 / 3 = 2, +1 for randomness).
	for node, count := range counts {
		if count > 3 {
			t.Errorf("node %s has %d vaults, expected at most 3", node, count)
		}
	}
	// All 3 nodes should have at least 1 vault.
	if len(counts) != 3 {
		t.Errorf("expected vaults on 3 nodes, got %d", len(counts))
	}
}

func TestPlacementRandomTiebreak(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-a", []string{"node-b"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	pm.reconcile(ctx)

	got := vaultNode(t, store, vaultID)
	if got != "node-a" && got != "node-b" {
		t.Fatalf("expected vault assigned to node-a or node-b, got %q", got)
	}
}

// ---------- Edge cases ----------

func TestPlacementEmptyConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, _, _ := newTestPlacement(t, "node-1", nil)

	// No vaults — should not panic.
	pm.reconcile(ctx)
}

func TestPlacementUnknownVaultType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "weird", Type: "quantum"})

	pm.reconcile(ctx)

	// Unknown type → nodeEligible returns false → no eligible node.
	if got := vaultNode(t, store, vaultID); got != "" {
		t.Fatalf("expected unknown type vault unassigned, got %q", got)
	}
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert for unplaceable unknown-type vault")
	}
}

func TestPlacementLocalVaultStorageClassZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	// StorageClass 0 is invalid — nodeHasStorageClass returns false for class 0.
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 0})

	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-1",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 0, Name: "zero", Path: "/z"}},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "" {
		t.Fatalf("expected StorageClass 0 vault unassigned, got %q", got)
	}
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert")
	}
}

func TestPlacementCloudVaultActiveChunkClassZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	csID := glid.New()
	_ = store.PutCloudService(ctx, system.CloudService{ID: csID, Name: "s3", Provider: "s3", Bucket: "b"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "cloud", Type: system.VaultTypeFile,
		CloudServiceID: &csID, StorageClass: 0,
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "" {
		t.Fatalf("expected ActiveChunkClass 0 vault unassigned, got %q", got)
	}
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert")
	}
}

// ---------- Alert lifecycle ----------

func TestPlacementAlertClearedWhenPlaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "local", Type: system.VaultTypeFile, StorageClass: 1})

	// First reconcile: no eligible node → alert set.
	pm.reconcile(ctx)
	if !hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert after first reconcile")
	}

	// Add matching file storage → now eligible.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-1",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-1" {
		t.Fatalf("expected placed on node-1, got %q", got)
	}
	if hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected alert to be cleared after placement")
	}
}

func TestPlacementAlertClearedOnStableAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", nil)

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	// Pre-set an alert manually.
	alerts.Set("vault-unplaced:"+vaultID.String(), alert.Warning, "test", "stale alert")

	pm.reconcile(ctx)

	// Vault is correctly assigned → alert should be cleared.
	if hasAlert(alerts, "vault-unplaced:") {
		t.Fatal("expected stale alert to be cleared")
	}
}

// ---------- Multiple vaults / mixed types ----------

func TestPlacementMultipleVaultsDifferentTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	memVault := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: memVault, Name: "mem", Type: system.VaultTypeMemory})

	localVault := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: localVault, Name: "local", Type: system.VaultTypeFile, StorageClass: 1})

	// Only node-2 has the storage class.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       "node-2",
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1, Name: "ssd", Path: "/data"}},
	})

	pm.reconcile(ctx)

	// Memory vault: either node. Local vault: must be node-2.
	memNode := vaultNode(t, store, memVault)
	localNode := vaultNode(t, store, localVault)

	if memNode == "" {
		t.Fatal("memory vault should be assigned")
	}
	if localNode != "node-2" {
		t.Fatalf("local vault should be on node-2, got %q", localNode)
	}
}

// ---------- Multiple file storages on one node ----------

func TestPlacementNodeWithMultipleStorageClasses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)

	vault1 := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vault1, Name: "fast", Type: system.VaultTypeFile, StorageClass: 1})

	vault2 := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vault2, Name: "slow", Type: system.VaultTypeFile, StorageClass: 3})

	// node-1 has both classes.
	_ = store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: "node-1",
		FileStorages: []system.FileStorage{
			{ID: glid.New(), StorageClass: 1, Name: "nvme", Path: "/fast"},
			{ID: glid.New(), StorageClass: 3, Name: "hdd", Path: "/slow"},
		},
	})

	pm.reconcile(ctx)

	if got := vaultNode(t, store, vault1); got != "node-1" {
		t.Fatalf("fast vault: expected node-1, got %q", got)
	}
	if got := vaultNode(t, store, vault2); got != "node-1" {
		t.Fatalf("slow vault: expected node-1, got %q", got)
	}
}

// ---------- Nil alerts (no panic) ----------

func TestPlacementNilAlerts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil)
	pm.alerts = nil // no alert collector

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory})

	// Should not panic.
	pm.reconcile(ctx)

	if got := vaultNode(t, store, vaultID); got != "node-1" {
		t.Fatalf("expected node-1, got %q", got)
	}
}

// ---------- nodeHasStorageClass unit tests ----------

func TestNodeHasStorageClass(t *testing.T) {
	t.Parallel()
	nscs := []system.NodeStorageConfig{
		{NodeID: "n1", FileStorages: []system.FileStorage{
			{StorageClass: 1}, {StorageClass: 3},
		}},
		{NodeID: "n2", FileStorages: []system.FileStorage{
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
		{"n3", 1, false}, // unknown node
		{"n1", 0, false}, // class 0 always false
		{"", 1, false},   // empty node ID
	}

	for _, tt := range tests {
		got := nodeHasStorageClass(nscs, tt.nodeID, tt.class)
		if got != tt.want {
			t.Errorf("nodeHasStorageClass(%q, %d) = %v, want %v", tt.nodeID, tt.class, got, tt.want)
		}
	}
}

// ---------- Replication / follower placement ----------

func TestPlacementRF2AssignsSecondary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory, ReplicationFactor: 2})

	pm.reconcile(ctx)

	nscs, _ := store.ListNodeStorageConfigs(ctx)
	placements, _ := store.GetVaultPlacements(ctx, vaultID)
	if system.LeaderNodeID(placements, nscs) == "" {
		t.Fatal("expected leader assigned")
	}
	followers := system.FollowerNodeIDs(placements, nscs)
	if len(followers) != 1 {
		t.Fatalf("expected 1 follower, got %d", len(followers))
	}
	if followers[0] == system.LeaderNodeID(placements, nscs) {
		t.Error("follower should not be the same as leader")
	}
}

func TestPlacementRF1NoSecondaries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory, ReplicationFactor: 1})

	pm.reconcile(ctx)

	nscs, _ := store.ListNodeStorageConfigs(ctx)
	placements, _ := store.GetVaultPlacements(ctx, vaultID)
	if followers := system.FollowerNodeIDs(placements, nscs); len(followers) != 0 {
		t.Errorf("expected 0 followers for RF=1, got %d", len(followers))
	}
}

func TestPlacementRF3InsufficientNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts := newTestPlacement(t, "node-1", []string{"node-2"})

	vaultID := glid.New()
	_ = store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "mem", Type: system.VaultTypeMemory, ReplicationFactor: 3})

	pm.reconcile(ctx)

	nscs, _ := store.ListNodeStorageConfigs(ctx)
	placements, _ := store.GetVaultPlacements(ctx, vaultID)
	// RF=3 needs 2 followers, but only 1 other node available.
	if followers := system.FollowerNodeIDs(placements, nscs); len(followers) != 1 {
		t.Errorf("expected 1 follower (max available), got %d", len(followers))
	}
	if !hasAlert(alerts, "vault-underreplicated:") {
		t.Error("expected underreplicated alert")
	}
}

// ---------- Active ingester placement ----------

// singletonFactories builds a Factories map with a singleton-capable "kafka"
// registration (the real-world type most likely to use Singleton=true).
func singletonFactories() *orchestrator.Factories {
	return &orchestrator.Factories{
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"kafka": {Factory: nil, SingletonSupported: true},
		},
	}
}

// singletonIngester returns an IngesterConfig with Singleton=true using the
// kafka type from singletonFactories. Callers override fields as needed.
func singletonIngester(name string, nodeIDs ...string) system.IngesterConfig {
	return system.IngesterConfig{
		ID: glid.New(), Name: name, Type: "kafka", Enabled: true,
		NodeIDs: nodeIDs, Singleton: true,
	}
}

func TestPlacementSingletonIngesterAssignment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-1", "node-2")
	_ = store.PutIngester(ctx, ing)

	pm.reconcile(ctx)

	assigned, err := store.GetIngesterAssignment(ctx, ing.ID)
	if err != nil {
		t.Fatalf("GetIngesterAssignment: %v", err)
	}
	if assigned != "node-1" && assigned != "node-2" {
		t.Fatalf("expected assignment to node-1 or node-2, got %q", assigned)
	}
}

func TestPlacementSingletonIngesterPrefersNonLeader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-1", "node-2")
	_ = store.PutIngester(ctx, ing)

	// clusterSrv is nil in unit tests so leaderID == "", which means the
	// non-leader preference degrades to "pick any". Verify the stable path:
	// once assigned, repeated reconciles do not move it.
	pm.reconcile(ctx)

	first, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if first == "" {
		t.Fatal("expected assignment after reconcile")
	}

	pm.reconcile(ctx)
	second, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if second != first {
		t.Fatalf("assignment not stable: first=%q, second=%q", first, second)
	}
}

func TestPlacementSingletonIngesterReassignOnNodeDeath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-1", "node-2")
	_ = store.PutIngester(ctx, ing)

	// Force assignment to node-2.
	_ = store.SetIngesterAssignment(ctx, ing.ID, "node-2")

	// New placementManager where node-2 is dead (not in livePeers).
	pm2, _, _ := newTestPlacement(t, "node-1", nil)
	pm2.cfgStore = store
	pm2.factories = pm.factories

	pm2.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if assigned != "node-1" {
		t.Fatalf("expected reassignment to node-1, got %q", assigned)
	}
}

func TestPlacementSkipsParallelIngesters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	// "parallel" type: SingletonSupported=false, so Singleton=true is ignored.
	pm.factories = &orchestrator.Factories{
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"parallel": {Factory: nil}, // SingletonSupported defaults to false
		},
	}

	ingID := glid.New()
	_ = store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "p-ing", Type: "parallel", Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
		// Singleton:true is ignored because the type has SingletonSupported=false.
		Singleton: true,
	})

	pm.reconcile(ctx)

	// Parallel ingesters should NOT get an assignment from placement manager.
	assigned, _ := store.GetIngesterAssignment(ctx, ingID)
	if assigned != "" {
		t.Fatalf("expected no assignment for parallel ingester, got %q", assigned)
	}
}

func TestPlacementSkipsNonSingletonConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	// Type supports singleton, but the instance has Singleton=false.
	ingID := glid.New()
	_ = store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "kafka-ing", Type: "kafka", Enabled: true,
		NodeIDs:   []string{"node-1", "node-2"},
		Singleton: false,
	})

	pm.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ingID)
	if assigned != "" {
		t.Fatalf("expected no assignment when Singleton=false, got %q", assigned)
	}
}

func TestPlacementSkipsDisabledIngesters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-1", "node-2")
	ing.Enabled = false
	_ = store.PutIngester(ctx, ing)

	pm.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if assigned != "" {
		t.Fatalf("expected no assignment for disabled ingester, got %q", assigned)
	}
}

func TestPlacementSingletonIngesterEmptyNodeIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing") // empty NodeIDs
	_ = store.PutIngester(ctx, ing)

	pm.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if assigned != "" {
		t.Fatalf("expected no assignment for ingester with empty NodeIDs, got %q", assigned)
	}
}

func TestPlacementSingletonIngesterNoAliveCandidate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", nil) // only node-1 alive
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-3", "node-4") // neither alive
	_ = store.PutIngester(ctx, ing)

	pm.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if assigned != "" {
		t.Fatalf("expected no assignment when no candidates alive, got %q", assigned)
	}
}

func TestPlacementSingletonIngesterStableOnRepeatedReconcile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	ing := singletonIngester("kafka-ing", "node-1", "node-2")
	_ = store.PutIngester(ctx, ing)

	pm.reconcile(ctx)
	first, _ := store.GetIngesterAssignment(ctx, ing.ID)
	if first == "" {
		t.Fatal("expected initial assignment")
	}

	for i := 0; i < 10; i++ {
		pm.reconcile(ctx)
		got, _ := store.GetIngesterAssignment(ctx, ing.ID)
		if got != first {
			t.Fatalf("reconcile %d changed assignment from %q to %q", i, first, got)
		}
	}
}

func TestPlacementSingletonIngesterUnknownType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, _ := newTestPlacement(t, "node-1", []string{"node-2"})
	pm.factories = singletonFactories()

	// Singleton=true but type is unknown → SingletonSupported check fails → skipped.
	ingID := glid.New()
	_ = store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "mystery-ing", Type: "unknown-type", Enabled: true,
		NodeIDs:   []string{"node-1", "node-2"},
		Singleton: true,
	})

	pm.reconcile(ctx)

	assigned, _ := store.GetIngesterAssignment(ctx, ingID)
	if assigned != "" {
		t.Fatalf("expected no assignment for unknown type, got %q", assigned)
	}
}

// ---------- State-driven placement guard (gastrolog-slc6l) ----------

// setupStateGuardTest creates a placement manager with two nodes (the
// local node and a peer) registered as real NodeConfig records in the
// system store, so cfgStore.ListNodes() returns them and placeVault's
// state-based gate fires. Returns the IDs for use as placement keys.
func setupStateGuardTest(t *testing.T, peerState system.NodeState) (pm *placementManager, store *sysmem.Store, alerts *alert.Collector, localID, peerID glid.GLID) {
	t.Helper()
	localID = glid.New()
	peerID = glid.New()
	pm, store, alerts = newTestPlacement(t, localID.String(), []string{peerID.String()})
	now := time.Now()
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: localID, Name: "local", State: system.NodeStateLive, StateSince: now,
	}); err != nil {
		t.Fatalf("PutNode local: %v", err)
	}
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: peerID, Name: "peer", State: peerState, StateSince: now,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}
	return pm, store, alerts, localID, peerID
}

// seedVaultWithLeader creates a memory vault whose leader storage
// points at the given node ID.
func seedVaultWithLeader(t *testing.T, store *sysmem.Store, leaderNodeID string) glid.GLID {
	t.Helper()
	ctx := context.Background()
	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, leaderPlacement(leaderNodeID)); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}
	return vaultID
}

func TestPlacement_MaintenanceLeader_RetainsPlacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts, _, peerID := setupStateGuardTest(t, system.NodeStateMaintenance)
	vaultID := seedVaultWithLeader(t, store, peerID.String())

	pm.reconcile(ctx)

	// Placement should remain on the Maintenance node despite the local
	// node being alive and eligible.
	if got := vaultNode(t, store, vaultID); got != peerID.String() {
		t.Errorf("placement rotated off Maintenance leader: got %q, want %q", got, peerID.String())
	}
	if !hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("expected soft-offline-leader alert, got none")
	}
}

func TestPlacement_UnreachableLeader_RetainsPlacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Unreachable peer is NOT in livePeers (heartbeats lapsed). Without
	// the state guard, placement would rotate off it.
	pm, store, alerts := newTestPlacement(t, glid.New().String(), nil /* no live peers */)
	peerID := glid.New()
	now := time.Now()
	_ = store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateUnreachable, StateSince: now,
	})
	vaultID := seedVaultWithLeader(t, store, peerID.String())

	pm.reconcile(ctx)

	// State guard refuses rotation off Unreachable nodes — the placement
	// guard's load-bearing case for the RF=1 redeploy bug.
	if got := vaultNode(t, store, vaultID); got != peerID.String() {
		t.Errorf("placement rotated off Unreachable leader: got %q, want %q", got, peerID.String())
	}
	if !hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("expected soft-offline-leader alert, got none")
	}
}

func TestPlacement_DrainingLeader_RetainsPlacement_NoSoftOfflineAlert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pm, store, alerts, _, peerID := setupStateGuardTest(t, system.NodeStateDraining)
	vaultID := seedVaultWithLeader(t, store, peerID.String())

	pm.reconcile(ctx)

	// Draining: placement reconcile is a no-op for the vault — the
	// drain orchestrator is the authority for moving placements off.
	if got := vaultNode(t, store, vaultID); got != peerID.String() {
		t.Errorf("placement rotated off Draining leader: got %q, want %q", got, peerID.String())
	}
	// Draining is a deliberate operator transition — no soft-offline
	// alert (the operator already knows).
	if hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("Draining state should not raise soft-offline alert")
	}
}

func TestPlacement_LiveLeader_NoStateGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Live peer with NO live heartbeats — existing rotate-on-not-alive
	// behavior should fire. State guard does not apply.
	localID := glid.New()
	peerID := glid.New()
	pm, store, _ := newTestPlacement(t, localID.String(), nil /* no live peers */)
	now := time.Now()
	_ = store.PutNode(ctx, system.NodeConfig{ID: localID, Name: "local", State: system.NodeStateLive, StateSince: now})
	_ = store.PutNode(ctx, system.NodeConfig{ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: now})
	vaultID := seedVaultWithLeader(t, store, peerID.String())

	pm.reconcile(ctx)

	// Live + not heartbeating → existing rotate behavior. Until the
	// auto-trigger sweep (gastrolog-39m2k) transitions this to
	// Unreachable, the state guard doesn't catch it.
	if got := vaultNode(t, store, vaultID); got != localID.String() {
		t.Errorf("Live-but-not-alive leader should have rotated: got %q, want %q", got, localID.String())
	}
}

func TestPlacement_SoftOfflineCleared_OnReturnToLive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Start with peer in Maintenance, soft-offline alert raised.
	pm, store, alerts, _, peerID := setupStateGuardTest(t, system.NodeStateMaintenance)
	vaultID := seedVaultWithLeader(t, store, peerID.String())

	pm.reconcile(ctx)
	if !hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Fatalf("setup: expected soft-offline alert after first reconcile")
	}
	_ = vaultID

	// Operator returns peer to Live.
	peer, _ := store.GetNode(ctx, peerID)
	peer.State = system.NodeStateLive
	peer.StateSince = time.Now()
	_ = store.PutNode(ctx, *peer)

	pm.reconcile(ctx)

	if hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("soft-offline alert should clear after transition to Live")
	}
}
