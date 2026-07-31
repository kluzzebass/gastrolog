package server_test

// Multi-node coverage for the standing-alarm surface: alarms are raised
// per-node in an alert.Collector, converted to NodeStats.alerts by the
// real cluster.StatsCollector, and served to any
// node's clients through GetClusterStatus — the RPC both the CLI
// (`gastrolog alerts`, `cluster status`) and the inspector's System Alerts
// panel read. These tests drive that path through the harness coordinator,
// so "alarm raised on one node, read from another" is exercised end to end
// minus only the wire broadcast (stood in by mnPeerNodeStats).

import (
	"context"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"

	"connectrpc.com/connect"
)

// alertsByNode fetches cluster status from the coordinator and returns each
// node's standing alerts keyed by raft node ID.
func alertsByNode(t *testing.T, h *multiNodeHarness) map[string][]*gastrologv1.SystemAlert {
	t.Helper()
	resp, err := h.lifecycleClient.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !resp.Msg.ClusterEnabled {
		t.Fatal("cluster not enabled; WithClusterStats wiring missing")
	}
	out := make(map[string][]*gastrologv1.SystemAlert, len(resp.Msg.Nodes))
	for _, n := range resp.Msg.Nodes {
		if n.Stats == nil {
			t.Fatalf("node %s has no stats", n.Id)
		}
		out[string(n.Id)] = n.Stats.Alerts
	}
	return out
}

// TestMultiNodeAlerts_PeerAlarmVisibleFromCoordinator raises an alarm on a
// non-coordinator node and reads it from the coordinator's RPC surface —
// the incident shape being that the node with the standing alarm is not
// the node the operator's shell is pointed at.
func TestMultiNodeAlerts_PeerAlarmVisibleFromCoordinator(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-2"].Raise("disk-space-exhausted", "storage1", "disk protect engaged")

	byNode := alertsByNode(t, h)
	if len(byNode) != 4 {
		t.Fatalf("cluster status returned %d nodes, want 4", len(byNode))
	}
	got := byNode["data-2"]
	if len(got) != 1 {
		t.Fatalf("data-2 alerts = %d, want 1", len(got))
	}
	a := got[0]
	if string(a.Id) != "disk-space-exhausted:storage1" || a.Source != "storage" ||
		a.Detail != "disk protect engaged" ||
		a.Priority != gastrologv1.AlarmPriority_ALARM_PRIORITY_HIGH {
		t.Fatalf("alert fields wrong: %+v", a)
	}
	if a.Response == "" || a.Cause == "" {
		t.Fatalf("catalog text not stamped on the wire: %+v", a)
	}
	if a.FirstSeen == nil || a.LastSeen == nil {
		t.Fatalf("alert timestamps missing: %+v", a)
	}
	for _, id := range []string{"coord", "data-1", "data-3"} {
		if len(byNode[id]) != 0 {
			t.Fatalf("node %s has %d alerts, want 0 — attribution leaked", id, len(byNode[id]))
		}
	}
}

// TestMultiNodeAlerts_NoAlarms is the quiet path: nothing raised anywhere
// means every node reports an empty alert list, not an error and not a
// missing stats block.
func TestMultiNodeAlerts_NoAlarms(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	for id, alerts := range alertsByNode(t, h) {
		if len(alerts) != 0 {
			t.Fatalf("node %s has %d alerts, want 0", id, len(alerts))
		}
	}
}

// TestMultiNodeAlerts_MultiNodeAttribution raises the SAME alert ID on two
// nodes plus a coordinator-local alarm, and verifies each stays attributed
// to its own node — "disk protect engaged" on one node means something very
// different from all of them. Also covers clearing: resolving the condition
// on one node removes only that node's entry.
func TestMultiNodeAlerts_MultiNodeAttribution(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-1"].Raise("disk-space-exhausted", "storage1", "disk protect engaged on data-1")
	h.alerts["data-3"].Raise("disk-space-exhausted", "storage1", "disk protect engaged on data-3")
	// Coordinator-local alarm goes through the LocalStats path, not the
	// peer provider — both must surface.
	h.alerts["coord"].Raise("wal-reserve", "cluster-ctl", "reservation below floor")

	byNode := alertsByNode(t, h)
	if n := len(byNode["data-1"]); n != 1 {
		t.Fatalf("data-1 alerts = %d, want 1", n)
	}
	if got := byNode["data-1"][0].Detail; got != "disk protect engaged on data-1" {
		t.Fatalf("data-1 alert detail = %q — cross-node attribution broken", got)
	}
	if got := byNode["data-3"][0].Detail; got != "disk protect engaged on data-3" {
		t.Fatalf("data-3 alert detail = %q — cross-node attribution broken", got)
	}
	if n := len(byNode["coord"]); n != 1 {
		t.Fatalf("coord alerts = %d, want 1 (LocalStats path)", n)
	}
	if got := byNode["coord"][0].Priority; got != gastrologv1.AlarmPriority_ALARM_PRIORITY_CRITICAL {
		t.Fatalf("coord alert priority = %v, want CRITICAL (wal-reserve catalog row)", got)
	}
	if n := len(byNode["data-2"]); n != 0 {
		t.Fatalf("data-2 alerts = %d, want 0", n)
	}

	// Alarms are state: the condition resolving on data-1 releases exactly
	// data-1's entry while data-3's identical ID keeps standing active.
	h.alerts["data-1"].Clear("disk-space-exhausted", "storage1")
	byNode = alertsByNode(t, h)
	if n := len(byNode["data-1"]); n != 0 {
		t.Fatalf("data-1 alerts after clear = %d, want 0 (released)", n)
	}
	if n := len(byNode["data-3"]); n != 1 {
		t.Fatalf("data-3 alerts after data-1 clear = %d, want 1", n)
	}
}

// TestMultiNodeAlerts_NothingSurvivesRestart: alarm state is in-memory only.
// A second harness is the in-process whole-cluster restart: nothing carries
// over, and the re-detected condition simply stands again once re-raised.
func TestMultiNodeAlerts_NothingSurvivesRestart(t *testing.T) {
	h1 := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())
	h1.alerts["data-2"].Raise("chunking-underreplicated", "vault1", "segments below minimum")
	if n := len(alertsByNode(t, h1)["data-2"]); n != 1 {
		t.Fatalf("pre-restart alerts = %d, want 1", n)
	}

	// Restart: a fresh cluster starts empty.
	h2 := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())
	if n := len(alertsByNode(t, h2)["data-2"]); n != 0 {
		t.Fatalf("alarm state survived restart: %d alerts", n)
	}

	// The raiser re-detects the standing condition after boot; the alarm is
	// simply standing again.
	h2.alerts["data-2"].Raise("chunking-underreplicated", "vault1", "segments below minimum")
	got := alertsByNode(t, h2)["data-2"]
	if len(got) != 1 || string(got[0].Id) != "chunking-underreplicated:vault1" {
		t.Fatalf("re-raised alarm not visible after restart: %+v", got)
	}
}

// TestMultiNodeAlerts_DelayOnSuppressionIsPerNode drives the suppression
// phase through the aggregation surface: suppression state is per-node
// collector state, so a condition flapping on one node must never chatter
// into the aggregated view, while the SAME condition persisting on another
// node annunciates there — and only there. Every collector runs on one
// deterministic harness clock; no sleeps.
func TestMultiNodeAlerts_DelayOnSuppressionIsPerNode(t *testing.T) {
	leaderlessType, ok := alert.TypeByID("vault-leaderless")
	if !ok || leaderlessType.DelayOn <= 0 {
		t.Fatal("vault-leaderless must carry a catalog DelayOn")
	}
	var mu sync.Mutex
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithAlertClock(clock))

	// data-2's condition flaps (placement edit resolving and re-resolving);
	// data-3's persists. Same alarm ID on both nodes.
	h.alerts["data-2"].Raise("vault-leaderless", "vault1", "no leader (flap)")
	h.alerts["data-3"].Raise("vault-leaderless", "vault1", "no leader (stuck)")
	advance(leaderlessType.DelayOn / 2)
	h.alerts["data-2"].Clear("vault-leaderless", "vault1")
	h.alerts["data-2"].Raise("vault-leaderless", "vault1", "no leader (flap)")

	// Inside the window nothing is aggregated from either node.
	for id, alerts := range alertsByNode(t, h) {
		if len(alerts) != 0 {
			t.Fatalf("node %s shows %d alarms inside the delay-on window", id, len(alerts))
		}
	}

	// The window elapses. data-2's flap restarted its window (still
	// pending); data-3's condition persisted the whole time and must be
	// the ONLY alarm in the aggregated view.
	advance(leaderlessType.DelayOn/2 + time.Second)
	byNode := alertsByNode(t, h)
	if n := len(byNode["data-2"]); n != 0 {
		t.Fatalf("data-2's flapping condition chattered into the aggregation: %d alarms", n)
	}
	if n := len(byNode["data-3"]); n != 1 {
		t.Fatalf("data-3's sustained condition must annunciate: %d alarms", n)
	}
	if got := byNode["data-3"][0].Detail; got != "no leader (stuck)" {
		t.Fatalf("data-3 alarm detail = %q", got)
	}

	// data-2's flap finally dies down cleared: nothing ever surfaces from
	// it, no occurrence, no residue — while data-3 keeps standing.
	h.alerts["data-2"].Clear("vault-leaderless", "vault1")
	advance(time.Hour)
	byNode = alertsByNode(t, h)
	if n := len(byNode["data-2"]); n != 0 {
		t.Fatalf("data-2 residue after the flap cleared: %d alarms", n)
	}
	if n := len(byNode["data-3"]); n != 1 {
		t.Fatalf("data-3 sustained alarm lost: %d alarms", n)
	}
}
