package orchestrator

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func TestSequencedFanOutTargetsOnPlacementFollower(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-follower"})
	vaultID := glid.New()
	registerSequencedTestVault(t, orch, vaultID, nil)
	v := orch.vaults[vaultID]
	inst := v.Instance
	inst.IsFollower = true
	inst.LeaderNodeID = "node-leader"
	v.seqFanOutTargets = nil

	targets := orch.sequencedFanOutTargets(v, inst)
	if len(targets) != 1 || targets[0].NodeID != "node-leader" {
		t.Fatalf("targets = %+v, want peer to placement leader", targets)
	}
}

func TestRefreshSeqFanOutTargetsFromPlacements(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-follower"})
	vaultID := glid.New()
	registerSequencedTestVault(t, orch, vaultID, nil)
	v := orch.vaults[vaultID]
	v.seqFanOutTargets = nil

	leaderStorageID := glid.New()
	followerStorageID := glid.New()
	placements := []system.VaultPlacement{
		{StorageID: leaderStorageID.String(), Leader: true},
		{StorageID: followerStorageID.String()},
	}
	nscs := []system.NodeStorageConfig{
		{NodeID: "node-leader", FileStorages: []system.FileStorage{{ID: leaderStorageID}}},
		{NodeID: "node-follower", FileStorages: []system.FileStorage{{ID: followerStorageID}}},
	}
	orch.RefreshSeqFanOutTargets(vaultID, placements, nscs)

	if len(v.seqFanOutTargets) != 1 || v.seqFanOutTargets[0].NodeID != "node-leader" {
		t.Fatalf("seqFanOutTargets = %+v, want leader peer from placements", v.seqFanOutTargets)
	}
}

func TestSequencedRoutesCompileLocalOnPlacementFollower(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-follower"})
	registerSequencedTestVault(t, orch, vaultID, nil)
	orch.vaults[vaultID].Instance.IsFollower = true
	orch.vaults[vaultID].Instance.LeaderNodeID = "node-leader"

	leaderStorageID := glid.New()
	followerStorageID := glid.New()

	orch.mu.Lock()
	err := orch.reloadRoutesFromConfig(&system.System{
		Config: system.Config{
			Routes: []system.RouteConfig{{
				ID:           glid.New(),
				Name:         "all",
				Enabled:      true,
				Destinations: []glid.GLID{vaultID},
				Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
			}},
			Vaults: []system.VaultConfig{{
				ID:         vaultID,
				Name:       "seq",
				WriteModel: string(system.VaultWriteModelSequenced),
				Placements: []system.VaultPlacement{
					{StorageID: leaderStorageID.String(), Leader: true},
					{StorageID: followerStorageID.String()},
				},
			}},
		},
		Runtime: system.Runtime{
			NodeStorageConfigs: []system.NodeStorageConfig{
				{NodeID: "node-leader", FileStorages: []system.FileStorage{{ID: leaderStorageID}}},
				{NodeID: "node-follower", FileStorages: []system.FileStorage{{ID: followerStorageID}}},
			},
		},
	})
	orch.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	matches := RouteFanOutMatches(orch.routeSet, testRecord("x").Attrs, SourceContext{Kind: SourceIngest})
	if len(matches) != 1 {
		t.Fatalf("matches = %v, want 1", matches)
	}
	if matches[0].NodeID != "" {
		t.Fatalf("sequenced route NodeID = %q, want empty (local follower ingest)", matches[0].NodeID)
	}
}
