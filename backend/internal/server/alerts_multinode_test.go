package server_test

// Multi-node coverage for the standing-alarm surface (gastrolog-33d9n2):
// alarms are raised per-node in an alert.Collector, converted to
// NodeStats.alerts by the real cluster.StatsCollector, and served to any
// node's clients through GetClusterStatus — the RPC both the CLI
// (`gastrolog alerts`, `cluster status`) and the inspector's System Alerts
// panel read. These tests drive that path through the harness coordinator,
// so "alarm raised on one node, read from another" is exercised end to end
// minus only the wire broadcast (stood in by mnPeerNodeStats).

import (
	"context"
	"testing"

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
// the incident shape from gastrolog-5ct2av: the node with the standing
// alarm is not the node the operator's shell is pointed at.
func TestMultiNodeAlerts_PeerAlarmVisibleFromCoordinator(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-2"].Set("disk-space:vault1", alert.Error, "diskguard", "disk protect engaged")

	byNode := alertsByNode(t, h)
	if len(byNode) != 4 {
		t.Fatalf("cluster status returned %d nodes, want 4", len(byNode))
	}
	got := byNode["data-2"]
	if len(got) != 1 {
		t.Fatalf("data-2 alerts = %d, want 1", len(got))
	}
	a := got[0]
	if string(a.Id) != "disk-space:vault1" || a.Source != "diskguard" ||
		a.Message != "disk protect engaged" ||
		a.Severity != gastrologv1.AlertSeverity_ALERT_SEVERITY_ERROR {
		t.Fatalf("alert fields wrong: %+v", a)
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

	h.alerts["data-1"].Set("disk-space:vault1", alert.Error, "diskguard", "disk protect engaged on data-1")
	h.alerts["data-3"].Set("disk-space:vault1", alert.Error, "diskguard", "disk protect engaged on data-3")
	// Coordinator-local alarm goes through the LocalStats path, not the
	// peer provider — both must surface.
	h.alerts["coord"].Set("wal:latency", alert.Warning, "raftwal", "append latency degraded")

	byNode := alertsByNode(t, h)
	if n := len(byNode["data-1"]); n != 1 {
		t.Fatalf("data-1 alerts = %d, want 1", n)
	}
	if got := byNode["data-1"][0].Message; got != "disk protect engaged on data-1" {
		t.Fatalf("data-1 alert message = %q — cross-node attribution broken", got)
	}
	if got := byNode["data-3"][0].Message; got != "disk protect engaged on data-3" {
		t.Fatalf("data-3 alert message = %q — cross-node attribution broken", got)
	}
	if n := len(byNode["coord"]); n != 1 {
		t.Fatalf("coord alerts = %d, want 1 (LocalStats path)", n)
	}
	if got := byNode["coord"][0].Severity; got != gastrologv1.AlertSeverity_ALERT_SEVERITY_WARNING {
		t.Fatalf("coord alert severity = %v, want WARNING", got)
	}
	if n := len(byNode["data-2"]); n != 0 {
		t.Fatalf("data-2 alerts = %d, want 0", n)
	}

	// Alarms are state: the condition resolving on data-1 clears exactly
	// data-1's entry while data-3's identical ID keeps standing.
	h.alerts["data-1"].Clear("disk-space:vault1")
	byNode = alertsByNode(t, h)
	if n := len(byNode["data-1"]); n != 0 {
		t.Fatalf("data-1 alerts after clear = %d, want 0", n)
	}
	if n := len(byNode["data-3"]); n != 1 {
		t.Fatalf("data-3 alerts after data-1 clear = %d, want 1", n)
	}
}
