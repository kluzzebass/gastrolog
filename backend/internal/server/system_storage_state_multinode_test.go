package server_test

// Multi-node coverage for the storage inspector's entity-list surface:
// ListStorages composes NodeStorageConfigs (config,
// replicated to every node) with live disk-guard state from the owning
// node — locally via the orchestrator, remotely via PeerStorageStats (the
// harness's mnPeerStorageStats stands in for the real NodeStats broadcast +
// PeerState.FindStorageState, the same shortcut mnPeerVaultStats takes for
// vault stats elsewhere in this package).
//
// Two tests, matching this codebase's convention for a scheduler-driven
// surface with no test-only trigger (see
// retention_unenforceable_multinode_test.go's package comment for the
// precedent this file follows): a fast config-only test that needs no
// guard tick, and a full-gate test that waits for the real disk-guard cron
// (diskGuardSchedule, 15s cadence — registered at orchestrator.New) to
// observe live state, cross-node.

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// storageWaitDeadline is comfortably more than one disk-guard tick
// (diskGuardSchedule, 15s) so a test that starts right after a tick still
// catches the next one, plus margin for scheduler jitter and slow CI —
// same shape as retention_unenforceable_multinode_test.go's
// mnAlarmWaitDeadline.
const storageWaitDeadline = 30 * time.Second

// findStorage returns the entry with the given ID from a ListStorages
// response, or nil.
func findStorage(storages []*gastrologv1.StorageState, id glid.GLID) *gastrologv1.StorageState {
	for _, s := range storages {
		if glid.FromBytes(s.Id) == id {
			return s
		}
	}
	return nil
}

// waitForMNStorageState polls the coordinator's ListStorages until want
// returns true for the given storage ID, or storageWaitDeadline expires.
// Polls for the observable effect (the real disk-guard cron tick), never a
// fixed sleep-as-sync — same shape as waitForMNAlarmPresence.
func waitForMNStorageState(t *testing.T, h *multiNodeHarness, id glid.GLID, want func(*gastrologv1.StorageState) bool) *gastrologv1.StorageState {
	t.Helper()
	deadline := time.Now().Add(storageWaitDeadline)
	for {
		resp, err := h.configClient.ListStorages(context.Background(), connect.NewRequest(&gastrologv1.ListStoragesRequest{}))
		if err != nil {
			t.Fatalf("ListStorages: %v", err)
		}
		s := findStorage(resp.Msg.Storages, id)
		if s != nil && want(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("storage %s did not reach the wanted state within %s; last seen: %+v", id, storageWaitDeadline, s)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// seedStorageWithPlacement writes a FileStorage on nodeID (config store
// directly, replicated to every harness node) plus a file vault placed on
// it: storage config and placements-on-storage are both config-derived.
func seedStorageWithPlacement(t *testing.T, h *multiNodeHarness, nodeID, path, floorExpr string) (storageID, vaultID glid.GLID) {
	t.Helper()
	ctx := context.Background()
	storageID = glid.New()

	if err := h.cfgStore.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: nodeID,
		FileStorages: []system.FileStorage{{
			ID:            storageID,
			Name:          "guarded-storage",
			Path:          path,
			StorageClass:  4,
			DiskFreeFloor: floorExpr,
		}},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig: %v", err)
	}

	vaultID = glid.New()
	placements := []system.VaultPlacement{{StorageID: storageID.String(), Leader: true}}
	if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
		ID:      vaultID,
		Name:    "on-guarded-storage",
		Enabled: true,
		Type:    system.VaultTypeFile,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := h.cfgStore.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}
	return storageID, vaultID
}

// TestMultiNodeListStorages_ConfigOnlyBeforeFirstSample pins the honest
// fallback contract: immediately after a storage is configured, before any
// disk-guard tick has sampled it, ListStorages reports its identity and
// config-derived placements correctly while every live field (free/total/
// verdicts) stays honestly zero — never fabricated (facts before
// speculation). Fast: no scheduler wait, runs under -short.
func TestMultiNodeListStorages_ConfigOnlyBeforeFirstSample(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"})

	storageID, vaultID := seedStorageWithPlacement(t, h, "data-1", t.TempDir(), "")

	resp, err := h.configClient.ListStorages(context.Background(), connect.NewRequest(&gastrologv1.ListStoragesRequest{}))
	if err != nil {
		t.Fatalf("ListStorages: %v", err)
	}
	s := findStorage(resp.Msg.Storages, storageID)
	if s == nil {
		t.Fatalf("storage %s missing from ListStorages; got %d entries", storageID, len(resp.Msg.Storages))
	}
	if s.Name != "guarded-storage" || s.StorageClass != 4 {
		t.Fatalf("identity fields wrong: %+v", s)
	}
	if !s.FloorIsDefault || !s.WarnIsDefault {
		t.Fatalf("unset expressions must report default, got warn_is_default=%v floor_is_default=%v", s.WarnIsDefault, s.FloorIsDefault)
	}
	if s.WarnExpr != orchestrator.DefaultDiskFreeWarn || s.FloorExpr != orchestrator.DefaultDiskFreeFloor {
		t.Fatalf("defaulted thresholds must publish the EFFECTIVE (built-in default) expression, not empty: got warn=%q floor=%q", s.WarnExpr, s.FloorExpr)
	}
	if s.FreeBytes != 0 || s.TotalBytes != 0 || s.ProtectVerdict || s.WarnVerdict {
		t.Fatalf("no sample has run yet: live fields must be honestly zero, got %+v", s)
	}
	if len(s.PlacedVaultIds) != 1 || glid.FromBytes(s.PlacedVaultIds[0]) != vaultID {
		t.Fatalf("placements must be config-derived and present even before any guard tick, got %v want [%s]", s.PlacedVaultIds, vaultID)
	}
}

// TestMultiNodeListStorages_RealTickReportsProtectPlacementsAndRemoval is
// the full-gate pin for the live surface: a storage on data-1 with an
// unreachable 99% free-space floor (guaranteed breach on any real
// filesystem short of being nearly empty — deterministic without knowing
// exact free bytes, avoiding "test timing with timing") reaches
// ProtectVerdict=true on the real disk-guard cron tick, visible from the
// COORDINATOR's ListStorages via PeerStorageStats — "every node can serve
// every storage's state including remote ones." The storage is then
// removed from config and must disappear immediately (config-driven
// identity, so a removed storage must not strand) with no further wait.
func TestMultiNodeListStorages_RealTickReportsProtectPlacementsAndRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a real disk-guard cron tick (diskGuardSchedule, 15s cadence)")
	}
	t.Parallel()

	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithDiskGuard("data-1"))
	storageID, vaultID := seedStorageWithPlacement(t, h, "data-1", t.TempDir(), "99%")

	s := waitForMNStorageState(t, h, storageID, func(s *gastrologv1.StorageState) bool {
		return s.ProtectVerdict
	})
	if s.NodeName == "" {
		t.Error("live state must name the owning node")
	}
	if s.TotalBytes == 0 {
		t.Error("a real statfs sample must report a nonzero volume total")
	}
	if s.FloorExpr != "99%" || s.FloorIsDefault {
		t.Fatalf("explicit floor override must round-trip, got expr=%q is_default=%v", s.FloorExpr, s.FloorIsDefault)
	}
	if len(s.PlacedVaultIds) != 1 || glid.FromBytes(s.PlacedVaultIds[0]) != vaultID {
		t.Fatalf("placements must still be present alongside live state, got %v", s.PlacedVaultIds)
	}

	// Remove the storage from config entirely (and drop the vault's now-
	// dangling placement, mirroring how an operator would actually do it).
	if err := h.cfgStore.SetNodeStorageConfig(context.Background(), system.NodeStorageConfig{NodeID: "data-1"}); err != nil {
		t.Fatalf("SetNodeStorageConfig (removal): %v", err)
	}
	if err := h.cfgStore.SetVaultPlacements(context.Background(), vaultID, nil); err != nil {
		t.Fatalf("SetVaultPlacements (clear): %v", err)
	}

	resp, err := h.configClient.ListStorages(context.Background(), connect.NewRequest(&gastrologv1.ListStoragesRequest{}))
	if err != nil {
		t.Fatalf("ListStorages after removal: %v", err)
	}
	if got := findStorage(resp.Msg.Storages, storageID); got != nil {
		t.Fatalf("removed storage must be gone from ListStorages immediately (config-driven, no guard-tick wait needed), got %+v", got)
	}
}
