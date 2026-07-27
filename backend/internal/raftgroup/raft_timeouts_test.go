package raftgroup

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func TestIsVaultControlPlaneGroupID(t *testing.T) {
	t.Parallel()
	v := glid.New()
	if !IsVaultControlPlaneGroupID(VaultControlPlaneGroupID(v)) {
		t.Fatal("vault ctl group ID not recognized")
	}
	for _, id := range []string{"cluster-ctl", "vault/foo", "vault/foo/data", "group-a"} {
		if IsVaultControlPlaneGroupID(id) {
			t.Fatalf("%q should not be vault ctl", id)
		}
	}
}

func TestRaftTimeouts(t *testing.T) {
	t.Parallel()
	v := glid.New()
	hb, el, ll := raftTimeouts(GroupConfig{GroupID: ClusterControlPlaneGroupID})
	if hb != DefaultHeartbeatTimeout || el != DefaultHeartbeatTimeout || ll != DefaultLeaderLeaseTimeout {
		t.Fatalf("cluster-ctl: got %v %v %v", hb, el, ll)
	}
	hb, el, ll = raftTimeouts(GroupConfig{GroupID: VaultControlPlaneGroupID(v)})
	wantVaultCtl := DefaultHeartbeatTimeout + vaultCtlTimeoutSlack
	if hb != wantVaultCtl || el != wantVaultCtl || ll != DefaultLeaderLeaseTimeout {
		t.Fatalf("vault ctl default: got %v %v %v", hb, el, ll)
	}
	custom := 7 * time.Second
	hb, el, ll = raftTimeouts(GroupConfig{
		GroupID:            VaultControlPlaneGroupID(v),
		HeartbeatTimeout:   custom,
		ElectionTimeout:    custom,
		LeaderLeaseTimeout: custom,
	})
	if hb != custom || el != custom || ll != custom {
		t.Fatalf("overrides: got %v %v %v", hb, el, ll)
	}
}

// TestConfigureTimeouts: the boot-time override moves the base for every
// group profile (vault-ctl keeps its slack on top) and rejects inverted or
// non-positive configurations. Not parallel: mutates the package base.
func TestConfigureTimeouts(t *testing.T) {
	t.Cleanup(func() {
		baseHeartbeatTimeout = DefaultHeartbeatTimeout
		baseLeaderLease = DefaultLeaderLeaseTimeout
	})

	if err := ConfigureTimeouts(5*time.Second, 3*time.Second); err != nil {
		t.Fatalf("ConfigureTimeouts: %v", err)
	}
	hb, el, ll := raftTimeouts(GroupConfig{GroupID: ClusterControlPlaneGroupID})
	if hb != 5*time.Second || el != 5*time.Second || ll != 3*time.Second {
		t.Fatalf("configured cluster-ctl: got %v %v %v", hb, el, ll)
	}
	hb, el, ll = raftTimeouts(GroupConfig{GroupID: VaultControlPlaneGroupID(glid.New())})
	if hb != 5*time.Second+vaultCtlTimeoutSlack || el != hb || ll != 3*time.Second {
		t.Fatalf("configured vault ctl: got %v %v %v", hb, el, ll)
	}

	// A base BELOW the shipped default scales the vault-ctl slack down with
	// it (capped at half the base) instead of letting a fixed second
	// dominate the window. Without the cap a 200ms base would still give
	// vault-ctl a 1.2s detector — six times the base — so compressing the
	// node-wide detector would not compress vault-ctl elections at all.
	if err := ConfigureTimeouts(200*time.Millisecond, 150*time.Millisecond); err != nil {
		t.Fatalf("ConfigureTimeouts (compressed): %v", err)
	}
	hb, el, ll = raftTimeouts(GroupConfig{GroupID: VaultControlPlaneGroupID(glid.New())})
	if hb != 300*time.Millisecond || el != hb || ll != 150*time.Millisecond {
		t.Fatalf("compressed vault ctl: got %v %v %v, want 300ms 300ms 150ms", hb, el, ll)
	}
	// The cap is inert at and above the shipped default: exactly base + 1s.
	if err := ConfigureTimeouts(DefaultHeartbeatTimeout, DefaultLeaderLeaseTimeout); err != nil {
		t.Fatalf("ConfigureTimeouts (default): %v", err)
	}
	hb, _, _ = raftTimeouts(GroupConfig{GroupID: VaultControlPlaneGroupID(glid.New())})
	if hb != DefaultHeartbeatTimeout+vaultCtlTimeoutSlack {
		t.Fatalf("default vault ctl after reconfigure: got %v, want %v", hb, DefaultHeartbeatTimeout+vaultCtlTimeoutSlack)
	}

	// Lease longer than the detector window must be rejected, not clamped:
	// hashicorp/raft panics on it, and silent clamping hides operator error.
	if err := ConfigureTimeouts(2*time.Second, 3*time.Second); err == nil {
		t.Fatal("lease > heartbeat accepted, want error")
	}
	if err := ConfigureTimeouts(0, time.Second); err == nil {
		t.Fatal("zero heartbeat accepted, want error")
	}
	if err := ConfigureTimeouts(time.Second, -time.Second); err == nil {
		t.Fatal("negative lease accepted, want error")
	}
}
