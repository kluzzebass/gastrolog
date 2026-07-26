package cluster

import (
	"strings"
	"testing"
	"time"

	"gastrolog/internal/applywait"
	"gastrolog/internal/vaultraft"
)

func TestVaultApplyForwarder_LocalLeader(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createVaultRaft(t, "v-leader", fsm, true, nil)
	waitVaultLeader(t, r, 5*time.Second)

	forwarder := NewVaultApplyForwarder(r, "vault/test/ctl", fsm.ApplyWait(), nil, ReplicationTimeout)
	if err := forwarder.Apply([]byte{1}); err != nil {
		t.Fatalf("Apply on leader: %v", err)
	}
}

func TestVaultApplyForwarder_NoLeader(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createVaultRaft(t, "v-lonely", fsm, false, nil)
	forwarder := NewVaultApplyForwarder(r, "vault/x/ctl", fsm.ApplyWait(), nil, 2*time.Second)
	if err := forwarder.Apply([]byte{1}); err == nil {
		t.Fatal("expected error when no leader and can't forward")
	}
}

func TestVaultApplyForwarder_LeaderShutdown(t *testing.T) {
	t.Parallel()

	fsm := vaultraft.NewFSM()
	r := createVaultRaft(t, "v-doomed", fsm, true, nil)
	waitVaultLeader(t, r, 5*time.Second)
	forwarder := NewVaultApplyForwarder(r, "vault/y/ctl", fsm.ApplyWait(), nil, ReplicationTimeout)
	r.Shutdown()
	if err := forwarder.Apply([]byte{1}); err == nil {
		t.Fatal("expected error after leader shutdown")
	}
}

// --- waitForGroupApply (gastrolog-4l24u read-after-write barrier) ---

func TestWaitForGroupApply_ZeroTargetAndNilTracker(t *testing.T) {
	t.Parallel()
	// Zero target: leader reported nothing meaningful — no wait.
	if err := waitForGroupApply(applywait.New(), "vault/z/ctl", 0, time.Millisecond); err != nil {
		t.Fatalf("zero target: %v", err)
	}
	// Nil tracker: group FSM without a tracker — barrier skipped, no panic.
	if err := waitForGroupApply(nil, "vault/z/ctl", 5, time.Millisecond); err != nil {
		t.Fatalf("nil tracker: %v", err)
	}
}

func TestWaitForGroupApply_AlreadyApplied(t *testing.T) {
	t.Parallel()
	tr := applywait.New()
	tr.Advance(8)
	// Already applied (replication beat the forward response): returns
	// without engaging the wait — a hang here would trip the test binary
	// timeout, not a flaky bound.
	if err := waitForGroupApply(tr, "vault/z/ctl", 8, time.Minute); err != nil {
		t.Fatalf("already applied: %v", err)
	}
}

func TestWaitForGroupApply_WokenByAdvance(t *testing.T) {
	t.Parallel()
	tr := applywait.New()
	done := make(chan error, 1)
	go func() { done <- waitForGroupApply(tr, "vault/z/ctl", 3, time.Minute) }()
	tr.Advance(3)
	if err := <-done; err != nil {
		t.Fatalf("waitForGroupApply after Advance(3): %v", err)
	}
}

func TestWaitForGroupApply_Timeout(t *testing.T) {
	t.Parallel()
	tr := applywait.New()
	tr.Advance(1)
	err := waitForGroupApply(tr, "vault/stuck/ctl", 99, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error when the group FSM never catches up")
	}
	if !strings.Contains(err.Error(), "vault/stuck/ctl") || !strings.Contains(err.Error(), "index 99") {
		t.Fatalf("timeout error missing group/target context: %v", err)
	}
	if !strings.Contains(err.Error(), "last applied 1") {
		t.Fatalf("timeout error missing last-applied context: %v", err)
	}
}
