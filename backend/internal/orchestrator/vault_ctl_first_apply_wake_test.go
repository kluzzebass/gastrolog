package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/vaultraft"
	"gastrolog/internal/waittest"

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
// readiness edge, and it signals the vault-ready broadcast.
func TestVaultCtlGroupFirstApplySignalsVaultReady(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	vfsm := deferThenCreateUnelectableGroup(t, f)

	wake := f.orch.vaultReadySignal.C()
	vfsm.StoreConfiguration(1, hraft.Configuration{})

	select {
	case <-wake:
	default:
		t.Fatal("the group's first apply did not signal vault readiness: a WaitVaultReady caller sleeps until its deadline")
	}
	if err := f.vault(t).ReadinessErr(); err != nil {
		t.Fatalf("ReadinessErr after the group's first apply = %v, want ready", err)
	}
}

// A WaitVaultReady caller blocked on a vault whose vault-ctl group was created
// after its instance returns once the group's first apply lands. Its context
// has no deadline: only the readiness signal can release it.
func TestWaitVaultReadyWakesOnDeferredGroupFirstApply(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	f.setNodes(f.peerID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- f.orch.WaitVaultReady(t.Context(), f.vaultCfg.ID) }()

	nodes := f.setNodes(f.peerID, f.localID)
	f.orch.RefreshVaultCtlMembers(nodes, f.factories)
	g := f.group()
	if g == nil {
		t.Fatal("refresh with every node resolvable did not create the group")
	}
	select {
	case err := <-done:
		t.Fatalf("WaitVaultReady returned %v before the group applied anything", err)
	default:
	}

	g.FSM.(*vaultraft.FSM).StoreConfiguration(1, hraft.Configuration{})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitVaultReady = %v, want nil", err)
		}
	case <-time.After(waittest.Failsafe):
		t.Fatal("WaitVaultReady still blocked after the group's first apply")
	}
}

// End to end on a real single-member group: the waiter blocks while the group
// is deferred, the refresh creates it, and the group's own election and
// bootstrap configuration apply release the waiter.
func TestWaitVaultReadyWakesWhenCreatedGroupElectsAndApplies(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	nodes := f.setNodes(f.localID)
	if err := f.orch.AddVault(context.Background(), f.vaultCfg, f.resolverHiding(f.localID)); err != nil {
		t.Fatalf("AddVault: %v", err)
	}
	if f.group() != nil {
		t.Fatal("precondition: group creation should have deferred")
	}

	done := make(chan error, 1)
	go func() { done <- f.orch.WaitVaultReady(t.Context(), f.vaultCfg.ID) }()

	f.orch.RefreshVaultCtlMembers(nodes, f.factories)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitVaultReady = %v, want nil", err)
		}
	case <-time.After(waittest.Failsafe):
		t.Fatal("WaitVaultReady still blocked after the created group elected and applied")
	}
	if err := f.vault(t).ReadinessErr(); err != nil {
		t.Fatalf("ReadinessErr after WaitVaultReady returned = %v, want ready", err)
	}
}

// A group that exists but has applied nothing does not release a waiter: it
// stays blocked until its own context ends.
func TestWaitVaultReadyOnGroupThatNeverApplies(t *testing.T) {
	t.Parallel()
	f := newDeferredGroupFixture(t)
	deferThenCreateUnelectableGroup(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- f.orch.WaitVaultReady(ctx, f.vaultCfg.ID) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitVaultReady = %v, want context.Canceled", err)
	}
}
