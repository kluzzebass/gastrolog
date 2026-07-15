package multiraft

import "testing"

func TestLaneSNIRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []string{
		ClusterConfigGroupID,
		"vault/06fg6qs7p1pkn1r26o95kmpm5c/ctl",
	}
	for _, groupID := range cases {
		sni := LaneSNI(groupID)
		if !IsRaftLaneSNI(sni) {
			t.Fatalf("IsRaftLaneSNI(%q) = false", sni)
		}
		got, ok := GroupIDFromLaneSNI(sni)
		if !ok || got != groupID {
			t.Fatalf("GroupIDFromLaneSNI(%q) = %q, %v; want %q", sni, got, ok, groupID)
		}
	}
}

func TestLegacyRaftLaneSNIMapsToConfig(t *testing.T) {
	t.Parallel()
	got, ok := GroupIDFromLaneSNI(LegacyRaftLaneSNI)
	if !ok || got != ClusterConfigGroupID {
		t.Fatalf("legacy SNI: got %q, %v; want config, true", got, ok)
	}
}

func TestNonRaftLaneSNI(t *testing.T) {
	t.Parallel()
	if IsRaftLaneSNI("gastrolog-cluster") {
		t.Fatal("service SNI must not be a raft lane")
	}
	if _, ok := GroupIDFromLaneSNI("example.com"); ok {
		t.Fatal("unexpected raft lane match")
	}
}
