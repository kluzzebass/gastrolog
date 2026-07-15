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
