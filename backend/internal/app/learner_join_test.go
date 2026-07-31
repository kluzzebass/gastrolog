package app

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Tests for the fresh-vs-restart-of-voter detection. The predicate is what
// decides whether requestClusterMembership asks the leader for voter or
// nonvoter admission — fresh joiners enter as learners and get promoted by
// the cluster-ctl learner promoter once caught up; restart-of-voter requests
// use AddVoter for idempotent address refresh.

func TestIsRestartOfVoter_NonRaftConfig(t *testing.T) {
	t.Parallel()
	// Memory mode has no persistent FSM to consult — always treat as
	// fresh so the test/single-node paths don't accidentally claim
	// voter restart.
	if isRestartOfVoter(context.Background(), RunConfig{ConfigType: "memory"}, sysmem.NewStore()) {
		t.Fatal("expected non-raft config to never report restart-of-voter")
	}
}

func TestIsRestartOfVoter_NilStore(t *testing.T) {
	t.Parallel()
	if isRestartOfVoter(context.Background(), RunConfig{ConfigType: "raft"}, nil) {
		t.Fatal("expected nil store to never report restart-of-voter")
	}
}

func TestIsRestartOfVoter_EmptyFSM(t *testing.T) {
	t.Parallel()
	// Fresh joiner: empty FSM. No vaults, no JWT secret.
	store := sysmem.NewStore()
	if isRestartOfVoter(context.Background(), RunConfig{ConfigType: "raft"}, store) {
		t.Fatal("expected empty FSM to report fresh join, not restart")
	}
}

func TestIsRestartOfVoter_HasVaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	// Add one vault — the snapshot-restored state of a returning
	// voter.
	if err := store.PutVault(ctx, system.VaultConfig{ID: glid.New(), Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if !isRestartOfVoter(ctx, RunConfig{ConfigType: "raft"}, store) {
		t.Fatal("expected vault present to indicate restart-of-voter")
	}
}

func TestIsRestartOfVoter_HasJWTSecretOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	// JWT secret alone suffices — a bootstrap node that wrote auth
	// settings but no vaults still counts as a returning voter (the
	// snapshot replay populated server settings).
	if err := store.SaveServerSettings(ctx, system.ServerSettings{
		Auth: system.AuthConfig{JWTSecret: "test-secret"},
	}); err != nil {
		t.Fatalf("SaveServerSettings: %v", err)
	}
	if !isRestartOfVoter(ctx, RunConfig{ConfigType: "raft"}, store) {
		t.Fatal("expected JWT secret to indicate restart-of-voter")
	}
}
