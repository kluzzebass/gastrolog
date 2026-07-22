package cluster

import (
	"context"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

func TestPeerState_Delete(t *testing.T) {
	ps := NewPeerState(time.Minute)
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
	ps := NewPeerState(time.Minute)
	ps.Delete("never-existed") // must not panic
}

// TestPeerState_Delete_SurvivesLaterUpdate verifies that after Delete, a
// subsequent Update for the same node restores it (contrast with
// MarkUnreachable which is also restored by later updates — these are both
// idempotent from a data-freshness perspective).
func TestPeerState_Delete_SurvivesLaterUpdate(t *testing.T) {
	ps := NewPeerState(time.Minute)
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	ps.Delete("node-a")
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	if got := ps.Get("node-a"); got == nil {
		t.Error("Update after Delete should restore the entry")
	}
}

// TestPeerState_HandleBroadcast_HeartbeatTouchesLastSeen verifies that a
// Heartbeat broadcast refreshes lastSeen without overwriting the cached
// NodeStats from the last NodeStats broadcast. This is the central
// invariant of gastrolog-2kio8: stats stay queryable between heavy
// broadcasts; liveness updates ride on the lightweight heartbeat.
func TestPeerState_HandleBroadcast_HeartbeatTouchesLastSeen(t *testing.T) {
	ps := NewPeerState(4 * time.Second)
	stats := &gastrologv1.NodeStats{NodeName: "alpha", Version: "v1"}

	// First, full NodeStats broadcast lands.
	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte("node-a"),
		Timestamp: nil,
		Payload:   &gastrologv1.BroadcastMessage_NodeStats{NodeStats: stats},
	})
	if got := ps.Get("node-a"); got == nil || got.NodeName != "alpha" {
		t.Fatalf("after NodeStats: want stats with NodeName=alpha, got %v", got)
	}

	// Snapshot current entry's received timestamp via reflection-free path:
	// Touch with an older time, then send a heartbeat — heartbeat must
	// override to a newer time AND keep stats intact.
	ps.Touch("node-a", time.Now().Add(-3*time.Second))

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId: []byte("node-a"),
		Payload:  &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
	})

	got := ps.Get("node-a")
	if got == nil {
		t.Fatal("after Heartbeat: stats should still be returned (within TTL)")
	}
	if got.NodeName != "alpha" || got.Version != "v1" {
		t.Errorf("Heartbeat clobbered stats: want NodeName=alpha Version=v1, got %+v", got)
	}
	live := ps.LivePeers()
	found := false
	for _, id := range live {
		if id == "node-a" {
			found = true
		}
	}
	if !found {
		t.Errorf("LivePeers should include node-a after heartbeat refreshed lastSeen, got %v", live)
	}
}

// TestPeerState_HandleBroadcast_HeartbeatOnlyForUnknownPeer verifies that
// a heartbeat from a peer we've never seen NodeStats from still tracks
// liveness. Otherwise a node booting up would appear dead until its
// first 5s NodeStats broadcast lands.
func TestPeerState_HandleBroadcast_HeartbeatOnlyForUnknownPeer(t *testing.T) {
	ps := NewPeerState(4 * time.Second)

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId: []byte("node-fresh"),
		Payload:  &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
	})

	live := ps.LivePeers()
	found := false
	for _, id := range live {
		if id == "node-fresh" {
			found = true
		}
	}
	if !found {
		t.Errorf("LivePeers should include node-fresh after heartbeat-only, got %v", live)
	}
	// No NodeStats received yet — Get returns nil because the entry's
	// stats field is nil, even though lastSeen is fresh.
	if got := ps.Get("node-fresh"); got != nil {
		t.Errorf("Get on heartbeat-only peer: want nil stats, got %v", got)
	}
}

// TestPeerState_PausedPeerDetectedWithinTTL is the timing acceptance
// for gastrolog-2kio8: a peer broadcasting heartbeats every 100ms with
// a 400ms TTL (4× safety factor — same shape as the production 1s/4s
// defaults, scaled down for test speed) must be marked offline within
// ~500ms of its heartbeats stopping.
//
// Before this issue, the equivalent timing was 5s broadcast / 20s TTL,
// so detection took 15-20s. The math here proves the new defaults give
// 0.4-0.5s detection; production (1s/4s) gives 4-5s. Same shape, faster
// constants.
func TestPeerState_PausedPeerDetectedWithinTTL(t *testing.T) {
	const (
		heartbeatTick = 100 * time.Millisecond
		ttl           = 400 * time.Millisecond // 4× heartbeat
		detectBudget  = ttl + 2*heartbeatTick  // TTL + 2 ticks of polling slack
	)
	ps := NewPeerState(ttl)

	// Goroutine simulating node-B broadcasting heartbeats. Stops when ctx fires.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(heartbeatTick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
					SenderId: []byte("node-B"),
					Payload:  &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
				})
			}
		}
	}()

	// Let a few heartbeats land. node-B must be alive.
	time.Sleep(3 * heartbeatTick)
	if !livePeerContains(ps, "node-B") {
		t.Fatalf("node-B should be alive after 3 heartbeats, got %v", ps.LivePeers())
	}

	// Pause node-B by stopping its heartbeat goroutine.
	pauseAt := time.Now()
	cancel()

	// Within detectBudget of the pause, node-B must drop from LivePeers.
	deadline := pauseAt.Add(detectBudget)
	for time.Now().Before(deadline) {
		if !livePeerContains(ps, "node-B") {
			elapsed := time.Since(pauseAt)
			t.Logf("node-B detected offline %v after pause (budget %v)", elapsed, detectBudget)
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

// TestPeerState_HeartbeatExpiresWithTTL verifies that absent any further
// broadcast, a peer expires from LivePeers after the TTL.
func TestPeerState_HeartbeatExpiresWithTTL(t *testing.T) {
	ps := NewPeerState(50 * time.Millisecond)

	ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId: []byte("node-a"),
		Payload:  &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
	})
	if len(ps.LivePeers()) != 1 {
		t.Fatal("precondition: 1 live peer after heartbeat")
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

	ps := NewPeerState(time.Minute)
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

// TestPeerState_FindStorageState pins the cross-node lookup contract for
// gastrolog-3cobq4: a storage is only ever reported by the node that owns
// it, so FindStorageState scans every live peer's broadcast and returns
// that one entry, keyed by the GLID's canonical String() form (parsed and
// compared against the wire's raw bytes — never a raw-bytes-vs-string
// mismatch, see the function's own doc comment). An unknown ID, an
// unparsable ID, and an expired reporting peer all resolve to nil — this
// is the surface "every node can serve every storage's state including
// remote ones" rests on.
func TestPeerState_FindStorageState(t *testing.T) {
	hosted := glid.New()
	unknown := glid.New()

	ps := NewPeerState(time.Minute)
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

// TestPeerState_VaultStorageProtectedNodeNames pins a review finding on
// gastrolog-9akebz: the "reported by <node>" admission detail must name
// nodes, not raw IDs, and the joined list must be stable between reads
// (sorted) rather than following Go's randomized map iteration order.
// Names come from each peer's own broadcast NodeStats.NodeName — a peer
// that hasn't reported one yet (empty string) falls back to its node ID,
// same contract as placementManager.nameOrID.
func TestPeerState_VaultStorageProtectedNodeNames(t *testing.T) {
	starved := glid.New()

	ps := NewPeerState(time.Minute)
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
	ps := NewPeerState(time.Minute)
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
