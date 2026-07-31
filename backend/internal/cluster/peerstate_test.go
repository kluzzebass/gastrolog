package cluster

import (
	"context"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

func TestPeerState_Delete(t *testing.T) {
	ps := NewPeerState(time.Minute, 0)
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	ps.Update("node-b", &gastrologv1.NodeStats{}, time.Now())

	if got := ps.Get("node-a"); got == nil {
		t.Fatal("precondition: node-a should be present")
	}

	ps.Delete("node-a")

	if got := ps.Get("node-a"); got != nil {
		t.Errorf("Get(node-a) after Delete: want nil, got %v", got)
	}
	if got := ps.Get("node-b"); got == nil {
		t.Error("Get(node-b): unrelated entry was removed")
	}
}

// TestPeerState_Delete_Missing verifies Delete on an unknown node is a no-op,
// not a panic.
func TestPeerState_Delete_Missing(t *testing.T) {
	ps := NewPeerState(time.Minute, 0)
	ps.Delete("never-existed") // must not panic
}

// TestPeerState_Delete_SurvivesLaterUpdate verifies that after Delete, a
// subsequent Update for the same node restores it (contrast with
// MarkUnreachable which is also restored by later updates — these are both
// idempotent from a data-freshness perspective).
func TestPeerState_Delete_SurvivesLaterUpdate(t *testing.T) {
	ps := NewPeerState(time.Minute, 0)
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	ps.Delete("node-a")
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	if got := ps.Get("node-a"); got == nil {
		t.Error("Update after Delete should restore the entry")
	}
}

// The central invariant of fast paused-node detection, carried over to its
// replacement: the fast liveness signal refreshes reachability WITHOUT
// touching the cached NodeStats, so a peer's last known payload stays
// queryable between the heavy 5s broadcasts. That signal used to be an empty
// Heartbeat broadcast; it is now Raft contact, and the invariant has to
// survive the swap.
func TestPeerState_RaftContactDoesNotClobberCachedStats(t *testing.T) {
	ps := NewPeerState(4*time.Second, 4*time.Second)
	stats := &gastrologv1.NodeStats{NodeName: "alpha", Version: "v1"}

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte("node-a"),
		Timestamp: nil,
		Payload:   &gastrologv1.BroadcastMessage_NodeStats{NodeStats: stats},
	})
	if got := ps.Get("node-a"); got == nil || got.NodeName != "alpha" {
		t.Fatalf("after NodeStats: want stats with NodeName=alpha, got %v", got)
	}

	ps.RecordRaftContact("node-a", "cluster-ctl", time.Now())

	got := ps.Get("node-a")
	if got == nil {
		t.Fatal("after Raft contact: stats should still be returned (within TTL)")
	}
	if got.NodeName != "alpha" || got.Version != "v1" {
		t.Errorf("Raft contact clobbered stats: want NodeName=alpha Version=v1, got %+v", got)
	}
	if !livePeerContains(ps, "node-a") {
		t.Errorf("LivePeers should include node-a after Raft contact, got %v", ps.LivePeers())
	}
}

// Only NodeStats moves peer state. A NodeJobs broadcast rides the same
// envelope and lands on the same subscriber list, and it must NOT be mistaken
// for liveness — the peer-jobs cache is a separate consumer with its own TTL.
// Before liveness moved to Raft contact, an empty Heartbeat payload was the
// other case handled here; nothing replaced it, and nothing should.
func TestPeerState_HandleBroadcast_IgnoresNonStatsPayloads(t *testing.T) {
	ps := NewPeerState(4*time.Second, 4*time.Second)

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId: []byte("node-jobs-only"),
		Payload:  &gastrologv1.BroadcastMessage_NodeJobs{NodeJobs: &gastrologv1.NodeJobs{}},
	})

	if live := ps.LivePeers(); len(live) != 0 {
		t.Errorf("LivePeers = %v, want empty — a jobs broadcast is not a liveness signal", live)
	}
	if !ps.LastSeen("node-jobs-only").IsZero() {
		t.Error("a jobs broadcast must not create liveness evidence")
	}
}

// The paused-node detection timing acceptance, re-pointed at the signal that
// replaced the heartbeat broadcast: a peer whose Raft contact stops while we
// keep probing must drop out of LivePeers within its TTL plus polling slack.
//
// Constants are scaled down from production for test speed but keep the
// production shape — contact arriving well inside the TTL, then stopping
// abruptly. Production is a 4s Raft window against a ~200ms probe cadence,
// where the old broadcast shape was an 8s window against a 1s cadence: the
// same test, a faster verdict.
func TestPeerState_PausedPeerDetectedWithinTTL(t *testing.T) {
	const (
		contactTick  = 100 * time.Millisecond
		ttl          = 400 * time.Millisecond
		detectBudget = ttl + 2*contactTick // TTL + 2 ticks of polling slack
	)
	ps := NewPeerState(time.Hour, ttl)

	// node-B answers our Raft probes until ctx fires. The stats TTL is an
	// hour, so nothing but the Raft signal can decide this test.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ps.Update("node-B", &gastrologv1.NodeStats{}, time.Now())
	go func() {
		ticker := time.NewTicker(contactTick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				ps.RecordRaftContact("node-B", "cluster-ctl", now)
			}
		}
	}()

	time.Sleep(3 * contactTick)
	if !livePeerContains(ps, "node-B") {
		t.Fatalf("node-B should be alive after 3 contacts, got %v", ps.LivePeers())
	}

	// node-B pauses: contact stops, but we keep probing it, which is what
	// makes the silence count.
	pauseAt := time.Now()
	cancel()

	for time.Now().Before(pauseAt.Add(detectBudget)) {
		ps.RecordRaftProbe("node-B", "cluster-ctl", time.Now())
		if !livePeerContains(ps, "node-B") {
			t.Logf("node-B detected offline %v after pause (budget %v)", time.Since(pauseAt), detectBudget)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node-B still alive %v after pause; want offline within %v", time.Since(pauseAt), detectBudget)
}

func livePeerContains(ps *PeerState, id string) bool {
	for _, p := range ps.LivePeers() {
		if p == id {
			return true
		}
	}
	return false
}

// Absent any further evidence, a peer known only from its NodeStats broadcast
// expires from LivePeers after the stats TTL — the fallback path for peers
// this node shares no Raft edge with.
func TestPeerState_BroadcastOnlyPeerExpiresWithTTL(t *testing.T) {
	ps := NewPeerState(50*time.Millisecond, 4*time.Second)

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId: []byte("node-a"),
		Payload:  &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{}},
	})
	if len(ps.LivePeers()) != 1 {
		t.Fatal("precondition: 1 live peer after the broadcast")
	}
	time.Sleep(80 * time.Millisecond)
	if live := ps.LivePeers(); len(live) != 0 {
		t.Errorf("after TTL: want 0 live peers, got %v", live)
	}
}

func TestPeerJobState_Delete(t *testing.T) {
	pjs := NewPeerJobState(time.Minute)
	pjs.Update("node-a", []*gastrologv1.Job{{Id: []byte("j1")}}, time.Now())
	pjs.Update("node-b", []*gastrologv1.Job{{Id: []byte("j2")}}, time.Now())

	all := pjs.GetAll()
	if _, ok := all["node-a"]; !ok {
		t.Fatal("precondition: node-a should be present")
	}

	pjs.Delete("node-a")

	all = pjs.GetAll()
	if _, ok := all["node-a"]; ok {
		t.Error("GetAll after Delete(node-a): entry still present")
	}
	if _, ok := all["node-b"]; !ok {
		t.Error("GetAll after Delete(node-a): unrelated entry was removed")
	}
}

// TestPeerJobState_Delete_Missing verifies Delete on an unknown node is a
// no-op.
func TestPeerJobState_Delete_Missing(t *testing.T) {
	pjs := NewPeerJobState(time.Minute)
	pjs.Delete("never-existed") // must not panic
}

// TestPeerState_VaultStorageProtected pins the broadcast half of per-vault disk
// protect: a vault listed in any LIVE peer's StorageProtectedVaultIds reads as
// protected here; an expired peer's verdict does not linger.
func TestPeerState_VaultStorageProtected(t *testing.T) {
	starved := glid.New()
	healthy := glid.New()

	ps := NewPeerState(time.Minute, 0)
	ps.Update("node-a", &gastrologv1.NodeStats{
		StorageProtectedVaultIds: [][]byte{starved.ToProto()},
	}, time.Now())
	ps.Update("node-b", &gastrologv1.NodeStats{}, time.Now())

	if !ps.VaultStorageProtected(starved) {
		t.Fatal("vault protected on a live peer must read as protected")
	}
	if ps.VaultStorageProtected(healthy) {
		t.Fatal("unlisted vault must not read as protected")
	}

	// The reporting peer's entry expires: its verdict expires with it —
	// a dead node must not suspend a vault's admission forever.
	ps.Update("node-a", &gastrologv1.NodeStats{
		StorageProtectedVaultIds: [][]byte{starved.ToProto()},
	}, time.Now().Add(-2*time.Minute))
	if ps.VaultStorageProtected(starved) {
		t.Fatal("expired peer's protect verdict must not linger")
	}
}

// TestPeerState_FindStorageState pins the cross-node lookup contract behind
// ListStorages, the entity-list analogue of ListVaults: a storage is only
// ever reported by the node that owns it, so FindStorageState scans every
// live peer's broadcast and returns that one entry, keyed by the GLID's
// canonical String() form (parsed and compared against the wire's raw bytes
// — never a raw-bytes-vs-string mismatch, see the function's own doc
// comment). An unknown ID, an unparsable ID, and an expired reporting peer
// all resolve to nil — this is the surface "every node can serve every
// storage's state including remote ones" rests on.
func TestPeerState_FindStorageState(t *testing.T) {
	hosted := glid.New()
	unknown := glid.New()

	ps := NewPeerState(time.Minute, 0)
	ps.Update("node-a", &gastrologv1.NodeStats{
		Storages: []*gastrologv1.StorageState{{
			Id:        hosted.ToProto(),
			Name:      "fast-ssd",
			NodeName:  "node-a",
			FreeBytes: 10 << 30,
		}},
	}, time.Now())
	ps.Update("node-b", &gastrologv1.NodeStats{}, time.Now())

	got := ps.FindStorageState(hosted.String())
	if got == nil || got.Name != "fast-ssd" || got.NodeName != "node-a" {
		t.Fatalf("FindStorageState(hosted) = %+v, want the node-a entry", got)
	}

	if ps.FindStorageState(unknown.String()) != nil {
		t.Fatal("an ID no live peer reports must resolve to nil")
	}
	if ps.FindStorageState("not-a-glid") != nil {
		t.Fatal("an unparsable ID must resolve to nil, not panic or match spuriously")
	}

	// The owning node's entry expires: the storage's state expires with
	// it — a dead node must not leave stale free/total numbers standing.
	ps.Update("node-a", &gastrologv1.NodeStats{
		Storages: []*gastrologv1.StorageState{{Id: hosted.ToProto(), Name: "fast-ssd"}},
	}, time.Now().Add(-2*time.Minute))
	if ps.FindStorageState(hosted.String()) != nil {
		t.Fatal("expired peer's storage state must not linger")
	}
}

// TestPeerState_VaultStorageProtectedNodeNames pins a review finding: the
// "reported by <node>" admission detail must name nodes, not raw IDs, and
// the joined list must be stable between reads (sorted) rather than
// following Go's randomized map iteration order.
// Names come from each peer's own broadcast NodeStats.NodeName — a peer
// that hasn't reported one yet (empty string) falls back to its node ID,
// same contract as placementManager.nameOrID.
func TestPeerState_VaultStorageProtectedNodeNames(t *testing.T) {
	starved := glid.New()

	ps := NewPeerState(time.Minute, 0)
	// Three reporting peers, updated out of sorted order, one with no
	// NodeName yet (falls back to its raw node ID).
	ps.Update("node-c", &gastrologv1.NodeStats{
		NodeName:                 "charlie",
		StorageProtectedVaultIds: [][]byte{starved.ToProto()},
	}, time.Now())
	ps.Update("node-a", &gastrologv1.NodeStats{
		NodeName:                 "alpha",
		StorageProtectedVaultIds: [][]byte{starved.ToProto()},
	}, time.Now())
	ps.Update("node-b", &gastrologv1.NodeStats{
		// No NodeName reported yet: falls back to the raw node ID.
		StorageProtectedVaultIds: [][]byte{starved.ToProto()},
	}, time.Now())

	got := ps.VaultStorageProtectedNodeNames(starved)
	want := []string{"alpha", "charlie", "node-b"}
	if len(got) != len(want) {
		t.Fatalf("VaultStorageProtectedNodeNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("VaultStorageProtectedNodeNames = %v, want %v (sorted)", got, want)
		}
	}

	// VaultStorageProtectedNodes (the placement manager's set-membership
	// method) must stay ID-keyed — a repurposing to names would silently
	// break placement's degraded-home matching against real node IDs.
	ids := ps.VaultStorageProtectedNodes(starved)
	for _, id := range ids {
		if id != "node-a" && id != "node-b" && id != "node-c" {
			t.Fatalf("VaultStorageProtectedNodes leaked a name instead of a node ID: %v", ids)
		}
	}
}

// TestPeerState_VaultSizeCapped mirrors the disk-protect lookup for the
// max-size-capped list.
func TestPeerState_VaultSizeCapped(t *testing.T) {
	capped := glid.New()
	roomy := glid.New()
	ps := NewPeerState(time.Minute, 0)
	ps.Update("node-a", &gastrologv1.NodeStats{
		SizeCappedVaultIds: [][]byte{capped.ToProto()},
	}, time.Now())
	if !ps.VaultSizeCapped(capped) {
		t.Fatal("vault capped on a live peer must read as capped")
	}
	if ps.VaultSizeCapped(roomy) {
		t.Fatal("unlisted vault must not read as capped")
	}
}
