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
	if hb != defaultHeartbeatTimeout || el != defaultElectionTimeout || ll != defaultLeaderLeaseTimeout {
		t.Fatalf("cluster-ctl: got %v %v %v", hb, el, ll)
	}
	hb, el, ll = raftTimeouts(GroupConfig{GroupID: VaultControlPlaneGroupID(v)})
	if hb != vaultCtlHeartbeatTimeout || el != vaultCtlElectionTimeout || ll != vaultCtlLeaderLeaseTimeout {
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
