package orchestrator

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/vaultraft"

	hraft "github.com/hashicorp/raft"
)

// deferThenCreateUnelectableGroup registers the vault with its vault-ctl group
// deferred, then creates the group with the unreachable peer as the second
// voter: the group exists but can never commit, so its FSM applies nothing
// until the test delivers an entry.
func deferThenCreateUnelectableGroup(t *testing.T, f *deferredGroupFixture) *vaultraft.FSM {
	t.Helper()
	f.setNodes(f.peerID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	if f.group() != nil {
		t.Fatal("precondition: group creation should have deferred")
	}
	nodes := f.setNodes(f.peerID, f.localID)
	f.orch.RefreshVaultCtlMembers(nodes, f.factories)
	g := f.group()
	if g == nil {
		t.Fatal("refresh with every node resolvable did not create the group")
	}
	if err := f.vault(t).ReadinessErr(); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("group that has applied nothing: ReadinessErr = %v, want ErrVaultNotReady", err)
	}
	return g.FSM.(*vaultraft.FSM)
}

// The first entry a vault-ctl group's FSM applies — here the bootstrap
// configuration, as hraft's FSM goroutine delivers it — is the vault's
// readiness edge.
func TestVaultCtlGroupFirstApplyMakesVaultReady(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	vfsm := deferThenCreateUnelectableGroup(t, f)

	vfsm.StoreConfiguration(1, hraft.Configuration{})

	if err := f.vault(t).ReadinessErr(); err != nil {
		t.Fatalf("ReadinessErr after the group's first apply = %v, want ready", err)
	}
}
