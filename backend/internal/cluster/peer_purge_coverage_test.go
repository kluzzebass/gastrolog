package cluster

import (
	"reflect"
	"testing"
	"time"
)

// Every per-peer cache is reconciled against Raft membership by a periodic
// job (app/peer_cache_reconcile.go), because a follower gets no hraft
// observation for a membership change — PeerObservation is emitted only from
// leader-only code. That job is therefore the only thing keeping removed
// peers out of these caches, and it can only purge state its type's
// ReconcilePeers knows about.
//
// The hazard is adding a per-peer map and forgetting to purge it: nothing
// fails, the entry simply outlives the node, and the leak surfaces far from
// the field that caused it. These tests make the omission fail loudly.

// perPeerMapFields lists, per reconciled type, the map fields keyed (or
// prefixed) by peer ID and how each is kept clean. A field absent from this
// table fails TestPerPeerMapsAreAccountedFor, which is the point: adding a
// map forces a decision instead of a silent leak.
var perPeerMapFields = map[string]map[string]string{
	"PeerState": {
		"entries": "purged by ReconcilePeers/Delete/MarkUnreachable",
		"raft":    "purged by ReconcilePeers/Delete/MarkUnreachable",
	},
	"PeerJobState": {
		"entries": "purged by ReconcilePeers",
	},
	"Broadcaster": {
		"failed": "purged by ReconcilePeers",
	},
	"PeerByteMetrics": {
		"sent": "purged by ReconcilePeers",
		"recv": "purged by ReconcilePeers",
	},
	"StatsCollector": {
		"rates": "purged by ReconcilePeers (keys are role-namespaced; rateSeriesPeerID extracts the peer)",
		// EXEMPT, and fragile. Keys come from peerConnStatKey, which begins
		// with the peer ID, so this IS per-peer state — but
		// storePublishedPurposeWindows builds a fresh map and REPLACES the
		// field on every tick, so a removed peer's entry cannot outlive one
		// interval. If that ever becomes a merge instead of a replace, this
		// field starts leaking and ReconcilePeers will not catch it.
		"lastPublishedPurposeWindows": "exempt: wholesale-replaced each tick, never merged",
	},
}

func TestPerPeerMapsAreAccountedFor(t *testing.T) {
	t.Parallel()
	types := []any{
		PeerState{}, PeerJobState{}, Broadcaster{}, PeerByteMetrics{}, StatsCollector{},
	}
	for _, v := range types {
		rt := reflect.TypeOf(v)
		known := perPeerMapFields[rt.Name()]
		if known == nil {
			t.Errorf("%s is reconciled but has no entry in perPeerMapFields", rt.Name())
			continue
		}
		for i := range rt.NumField() {
			f := rt.Field(i)
			if f.Type.Kind() != reflect.Map || f.Type.Key().Kind() != reflect.String {
				continue
			}
			if _, ok := known[f.Name]; !ok {
				t.Errorf("%s.%s is a new string-keyed map and is not accounted for.\n"+
					"If it is keyed (or prefixed) by peer ID, purge it in ReconcilePeers.\n"+
					"If it is not per-peer, add it to perPeerMapFields with that reason.\n"+
					"Silently leaving it out means removed peers outlive their node until restart.",
					rt.Name(), f.Name)
			}
		}
		// The reverse direction: a table entry for a field that no longer
		// exists is stale guidance, and stale guidance is how the next reader
		// concludes coverage is complete when it is not.
		for name := range known {
			if _, ok := rt.FieldByName(name); !ok {
				t.Errorf("perPeerMapFields lists %s.%s, which no longer exists", rt.Name(), name)
			}
		}
	}
}

// The behavioural half: each type actually drops a peer that is no longer a
// member. Populated through the real recording paths where they exist, so the
// test breaks if a path starts writing somewhere ReconcilePeers cannot see.
func TestReconcilePeersDropsRemovedPeerEverywhere(t *testing.T) {
	t.Parallel()
	keep := map[string]struct{}{"stay": {}}

	t.Run("PeerState", func(t *testing.T) {
		t.Parallel()
		ps := NewPeerState(time.Hour, time.Hour)
		ps.RecordRaftContact("gone", "g1", time.Now())
		ps.RecordRaftContact("stay", "g1", time.Now())
		ps.ReconcilePeers(keep)
		if !ps.LastSeen("gone").IsZero() {
			t.Error("PeerState kept the removed peer's Raft contact")
		}
		if ps.LastSeen("stay").IsZero() {
			t.Error("PeerState dropped a peer that is still a member")
		}
	})

	t.Run("PeerByteMetrics", func(t *testing.T) {
		t.Parallel()
		m := NewPeerByteMetrics()
		// "gone" appears in recv ONLY: a peer we received from but never sent
		// to. That is the case a purge covering just the sent map would miss.
		m.TrackReceived("gone", 1)
		m.TrackSent("stay", 1)
		m.ReconcilePeers(keep)

		seen := map[string]bool{}
		for _, c := range m.Snapshot() {
			seen[c.Peer] = true
		}
		if seen["gone"] {
			t.Error("PeerByteMetrics kept a removed peer present only in recv — " +
				"the second map is the one a partial purge forgets")
		}
		if !seen["stay"] {
			t.Error("PeerByteMetrics dropped a peer that is still a member")
		}
	})
}
