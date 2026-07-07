package orchestrator

import (
	"log/slog"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// TestReconcileInstanceRole: the placement sweep must heal a stale
// instance role — a raced dispatch once left every node follower and a
// vault leaderless (no retention, no backfill) for seven hours. It must
// NOT flip roles when placements resolve to no leader (mid-flap state),
// and must not touch instances outside the placement.
func TestReconcileInstanceRole(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	leaderStorage := glid.New()
	followerStorage := glid.New()

	mkSys := func(withLeader bool) *system.System {
		placements := []system.VaultPlacement{
			{StorageID: followerStorage.String()},
		}
		if withLeader {
			placements = append(placements, system.VaultPlacement{StorageID: leaderStorage.String(), Leader: true})
		}
		return &system.System{
			Runtime: system.Runtime{
				NodeStorageConfigs: []system.NodeStorageConfig{
					{NodeID: "node-L", FileStorages: []system.FileStorage{{ID: leaderStorage}}},
					{NodeID: "node-F", FileStorages: []system.FileStorage{{ID: followerStorage}}},
				},
			},
			Config: system.Config{
				Vaults: []system.VaultConfig{{ID: vaultID, Placements: placements}},
			},
		}
	}

	// Stale-follower leader heals.
	o := &Orchestrator{localNodeID: "node-L", rotationLogger: slog.Default()}
	inst := &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	sys := mkSys(true)
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if inst.IsFollower || inst.LeaderNodeID != "" {
		t.Fatalf("leader not healed: %+v", inst)
	}

	// Follower keeps role, leader pointer refreshes.
	o = &Orchestrator{localNodeID: "node-F", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if !inst.IsFollower || inst.LeaderNodeID != "node-L" {
		t.Fatalf("follower pointer not refreshed: %+v", inst)
	}

	// No resolvable leader: do not touch anything.
	o = &Orchestrator{localNodeID: "node-L", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	sysNoLeader := mkSys(false)
	o.reconcileInstanceRole(sysNoLeader, sysNoLeader.Config.Vaults[0], inst)
	if !inst.IsFollower || inst.LeaderNodeID != "node-X" {
		t.Fatalf("roles flipped on unresolvable placement: %+v", inst)
	}

	// Node outside the placement: untouched.
	o = &Orchestrator{localNodeID: "node-Z", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: false}
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if inst.IsFollower {
		t.Fatalf("out-of-placement instance touched: %+v", inst)
	}
}
