package app

import (
	"context"
	"sync"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Live AddVoter on a real vault-ctl group is exercised by the multi-node
// integration test (learner_promoter_multinode_test.go); the unit tests
// here cover the peer-stats lookup helper and the group-enumeration
// provider, which are the parts easiest to get subtly wrong.

// fakePeerStats implements peerStatsReader with configurable returns.
type fakePeerStats struct {
	mu     sync.Mutex
	byNode map[string]*gastrologv1.NodeStats
}

func (f *fakePeerStats) Get(senderID string) *gastrologv1.NodeStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byNode[senderID]
}

// fakeGroupMgr implements vaultCtlRaftGroupAccess. Returns nil for every
// group — sufficient for the provider-enumeration test, which exercises
// the cfgStore listing and nil-group skipping without a real Raft group.
type fakeGroupMgr struct{}

func (fakeGroupMgr) GetGroup(_ string) *raftgroup.Group { return nil }

func TestPeerVaultAppliedIndex_MatchByGLIDBytes(t *testing.T) {
	t.Parallel()
	target := glid.New()
	other := glid.New()

	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			vaultStatsByID(other, 50),
			vaultStatsByID(target, 100),
			vaultStatsByID(glid.New(), 200),
		}},
	}}

	got, ok := peerVaultAppliedIndex(ps, "peer", target)
	if !ok {
		t.Fatalf("expected target found, got ok=false")
	}
	if got != 100 {
		t.Fatalf("expected applied=100, got %d", got)
	}
}

func TestPeerVaultAppliedIndex_NoPeerEntry(t *testing.T) {
	t.Parallel()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}}
	if _, ok := peerVaultAppliedIndex(ps, "ghost", glid.New()); ok {
		t.Fatal("expected ok=false for missing peer")
	}
}

func TestPeerVaultAppliedIndex_PeerHasNoVaultEntry(t *testing.T) {
	t.Parallel()
	target := glid.New()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			vaultStatsByID(glid.New(), 100),
			vaultStatsByID(glid.New(), 200),
		}},
	}}
	if _, ok := peerVaultAppliedIndex(ps, "peer", target); ok {
		t.Fatal("expected ok=false when target vault not present in peer's snapshot")
	}
}

// TestPeerVaultAppliedIndex_NilVaultEntryTolerated guards against stats
// slices containing nil entries (broadcast race during slice append).
func TestPeerVaultAppliedIndex_NilVaultEntryTolerated(t *testing.T) {
	t.Parallel()
	target := glid.New()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			nil,
			vaultStatsByID(target, 100),
			nil,
		}},
	}}
	got, ok := peerVaultAppliedIndex(ps, "peer", target)
	if !ok || got != 100 {
		t.Fatalf("expected (100, true), got (%d, %v)", got, ok)
	}
}

// TestVaultCtlPromoter_ProviderSkipsAbsentGroups verifies the group
// provider enumerates configured vaults but skips those whose Raft group
// is not present locally (GetGroup returns nil) — so evaluate() drains
// quietly on a node that hosts none of the vaults' control-plane groups.
func TestVaultCtlPromoter_ProviderSkipsAbsentGroups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	for _, name := range []string{"v1", "v2"} {
		if err := store.PutVault(ctx, system.VaultConfig{ID: glid.New(), Name: name, Type: system.VaultTypeMemory}); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
	}
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}}

	p := newVaultCtlLearnerPromoter(ctx, store, fakeGroupMgr{}, ps, quietLogger())
	// Should not panic and should promote nothing (no groups resolve).
	p.evaluate()
}

// TestVaultCtlPromoter_ListVaultsErrorNoOp verifies a cfgStore error is
// handled: the provider returns no groups and evaluate() is a no-op.
func TestVaultCtlPromoter_ListVaultsErrorNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}}
	p := newVaultCtlLearnerPromoter(ctx, errStore{}, fakeGroupMgr{}, ps, quietLogger())
	p.evaluate() // must not panic
}

// errStore is a system.Store whose ListVaults fails. Only ListVaults is
// exercised by the promoter; the embedded memory store satisfies the rest
// of the interface.
type errStore struct{ *sysmem.Store }

func (errStore) ListVaults(context.Context) ([]system.VaultConfig, error) {
	return nil, errFakeMember
}
