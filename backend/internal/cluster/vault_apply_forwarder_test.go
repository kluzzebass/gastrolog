package cluster

import (
	"testing"
	"time"

	"gastrolog/internal/vaultraft"
)

func TestVaultApplyForwarder_LocalLeader(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createTierRaft(t, "v-leader", fsm, true, nil)
	waitTierLeader(t, r, 5*time.Second)

	forwarder := NewVaultApplyForwarder(r, "vault/test/ctl", nil, ReplicationTimeout)
	if err := forwarder.Apply([]byte{1}); err != nil {
		t.Fatalf("Apply on leader: %v", err)
	}
}

func TestVaultApplyForwarder_NoLeader(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createTierRaft(t, "v-lonely", fsm, false, nil)
	forwarder := NewVaultApplyForwarder(r, "vault/x/ctl", nil, 2*time.Second)
	if err := forwarder.Apply([]byte{1}); err == nil {
		t.Fatal("expected error when no leader and can't forward")
	}
}

func TestVaultApplyForwarder_LeaderShutdown(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createTierRaft(t, "v-doomed", fsm, true, nil)
	waitTierLeader(t, r, 5*time.Second)
	forwarder := NewVaultApplyForwarder(r, "vault/y/ctl", nil, ReplicationTimeout)
	r.Shutdown()
	if err := forwarder.Apply([]byte{1}); err == nil {
		t.Fatal("expected error after leader shutdown")
	}
}
