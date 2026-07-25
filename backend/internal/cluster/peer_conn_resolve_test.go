package cluster

import (
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// gastrolog-1rw6df: a freshly-joined node knows the leader's identity and
// address from the first heartbeat before its raft log has backfilled the
// configuration entries. Address resolution must serve the leader from that
// observation instead of failing until backfill completes.
func TestResolveAddrFromRaft(t *testing.T) {
	servers := []hraft.Server{
		{ID: "node-a", Address: "10.0.0.1:4566"},
		{ID: "node-b", Address: "10.0.0.2:4576"},
	}

	t.Run("found in config", func(t *testing.T) {
		addr, err := resolveAddrFromRaft(servers, "10.0.0.1:4566", "node-a", "node-b")
		if err != nil || addr != "10.0.0.2:4576" {
			t.Fatalf("addr=%q err=%v, want config address", addr, err)
		}
	})

	t.Run("config wins over leader observation", func(t *testing.T) {
		// If the config has an entry for the leader, it is authoritative —
		// the observation is only the pre-backfill fallback.
		addr, err := resolveAddrFromRaft(servers, "10.9.9.9:9999", "node-a", "node-a")
		if err != nil || addr != "10.0.0.1:4566" {
			t.Fatalf("addr=%q err=%v, want config address over observed", addr, err)
		}
	})

	t.Run("empty config, target is observed leader", func(t *testing.T) {
		// The regression shape: empty local config (log not backfilled),
		// leader known from the heartbeat.
		addr, err := resolveAddrFromRaft(nil, "10.0.0.1:4566", "node-a", "node-a")
		if err != nil || addr != "10.0.0.1:4566" {
			t.Fatalf("addr=%q err=%v, want observed leader address", addr, err)
		}
	})

	t.Run("empty config, target is not the leader", func(t *testing.T) {
		_, err := resolveAddrFromRaft(nil, "10.0.0.1:4566", "node-a", "node-b")
		if err == nil || !strings.Contains(err.Error(), "not found in raft config") {
			t.Fatalf("err=%v, want not-found", err)
		}
	})

	t.Run("empty config, no leader observed", func(t *testing.T) {
		_, err := resolveAddrFromRaft(nil, "", "", "node-a")
		if err == nil {
			t.Fatalf("want not-found when nothing is known")
		}
	})

	t.Run("leader observed with empty address is not dialable", func(t *testing.T) {
		_, err := resolveAddrFromRaft(nil, "", "node-a", "node-a")
		if err == nil {
			t.Fatalf("want not-found when the observation carries no address")
		}
	})
}
