package cli

import (
	"testing"
	"time"

	v1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// mkAlertNode builds a ClusterNode carrying alerts for row-builder tests.
func mkAlertNode(id, name string, alerts ...*v1.SystemAlert) *v1.ClusterNode {
	return &v1.ClusterNode{
		Id:    []byte(id),
		Name:  name,
		Stats: &v1.NodeStats{Alerts: alerts},
	}
}

func mkAlert(id string, sev v1.AlertSeverity, source, msg string, firstSeen time.Time) *v1.SystemAlert {
	return &v1.SystemAlert{
		Id:        []byte(id),
		Severity:  sev,
		Source:    source,
		Message:   msg,
		FirstSeen: timestamppb.New(firstSeen),
		LastSeen:  timestamppb.New(firstSeen.Add(time.Minute)),
	}
}

// TestCollectNodeAlerts_Attribution verifies each alert row carries the name
// of the node that raised it — the same alert ID on two nodes stays two
// attributed rows, never merged (gastrolog-33d9n2).
func TestCollectNodeAlerts_Attribution(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodes := []*v1.ClusterNode{
		mkAlertNode("node-A", "alpha", mkAlert("disk:v1", v1.AlertSeverity_ALERT_SEVERITY_ERROR, "diskguard", "disk protect engaged", t0.Add(2*time.Second))),
		mkAlertNode("node-B", "beta", mkAlert("disk:v1", v1.AlertSeverity_ALERT_SEVERITY_ERROR, "diskguard", "disk protect engaged", t0)),
	}

	got := collectNodeAlerts(nodes, "")
	if len(got) != 2 {
		t.Fatalf("collectNodeAlerts returned %d rows, want 2", len(got))
	}
	// Sorted by FirstSeen: beta's alert is older.
	if got[0].node != "beta" || got[1].node != "alpha" {
		t.Fatalf("attribution/order wrong: got [%s, %s], want [beta, alpha]", got[0].node, got[1].node)
	}
	if string(got[0].alert.Id) != "disk:v1" || string(got[1].alert.Id) != "disk:v1" {
		t.Fatalf("alert IDs lost in collection: %q, %q", got[0].alert.Id, got[1].alert.Id)
	}
}

// TestCollectNodeAlerts_NodeFilter verifies --node matching against
// configured name, broadcast name, and node ID, case-insensitively.
func TestCollectNodeAlerts_NodeFilter(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	a := mkAlertNode("node-A", "alpha", mkAlert("x", v1.AlertSeverity_ALERT_SEVERITY_WARNING, "chunking", "m1", t0))
	b := mkAlertNode("node-B", "", mkAlert("y", v1.AlertSeverity_ALERT_SEVERITY_ERROR, "diskguard", "m2", t0))
	b.Stats.NodeName = "beta-broadcast"
	nodes := []*v1.ClusterNode{a, b}

	tests := []struct {
		name     string
		filter   string
		wantIDs  []string
		wantRows int
	}{
		{"empty matches all", "", []string{"x", "y"}, 2},
		{"configured name", "alpha", []string{"x"}, 1},
		{"case-insensitive", "ALPHA", []string{"x"}, 1},
		{"broadcast name", "beta-broadcast", []string{"y"}, 1},
		{"node id", "node-B", []string{"y"}, 1},
		{"no match", "gamma", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := collectNodeAlerts(nodes, tc.filter)
			if len(got) != tc.wantRows {
				t.Fatalf("filter %q: got %d rows, want %d", tc.filter, len(got), tc.wantRows)
			}
			for i, id := range tc.wantIDs {
				if string(got[i].alert.Id) != id {
					t.Fatalf("filter %q row %d: alert %q, want %q", tc.filter, i, got[i].alert.Id, id)
				}
			}
		})
	}
}

// TestCollectNodeAlerts_EmptyAndNil covers the no-alarms path and defensive
// nil handling: nodes without stats (offline peers), nil nodes, nil alerts.
func TestCollectNodeAlerts_EmptyAndNil(t *testing.T) {
	t.Parallel()
	if got := collectNodeAlerts(nil, ""); len(got) != 0 {
		t.Fatalf("nil nodes: got %d rows, want 0", len(got))
	}
	nodes := []*v1.ClusterNode{
		nil,
		{Id: []byte("node-A"), Name: "alpha"}, // no Stats (peer never broadcast)
		mkAlertNode("node-B", "beta"),         // Stats but zero alerts
		{Id: []byte("node-C"), Name: "gamma", Stats: &v1.NodeStats{Alerts: []*v1.SystemAlert{nil}}},
	}
	if got := collectNodeAlerts(nodes, ""); len(got) != 0 {
		t.Fatalf("no standing alerts: got %d rows, want 0", len(got))
	}
}

// TestAnyNodeMatches verifies the typo guard: a --node value naming no
// cluster member must be distinguishable from a healthy node, so the
// command can error instead of printing a reassuring "no standing alerts".
func TestAnyNodeMatches(t *testing.T) {
	t.Parallel()
	nodes := []*v1.ClusterNode{
		nil,
		{Id: []byte("node-A"), Name: "alpha"}, // matches even without stats
	}
	if !anyNodeMatches(nodes, "alpha") {
		t.Fatal("configured name should match")
	}
	if !anyNodeMatches(nodes, "node-A") {
		t.Fatal("node ID should match")
	}
	if anyNodeMatches(nodes, "alpha-typo") {
		t.Fatal("unknown node must not match")
	}
}

// TestNodesWithoutStats verifies offline nodes (no broadcast stats) are
// reported as unknown-alert-state rather than silently omitted.
func TestNodesWithoutStats(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodes := []*v1.ClusterNode{
		mkAlertNode("node-A", "alpha", mkAlert("x", v1.AlertSeverity_ALERT_SEVERITY_WARNING, "chunking", "m", t0)),
		{Id: []byte("node-B"), Name: "beta"}, // never broadcast
		nil,
	}
	got := nodesWithoutStats(nodes, "")
	if len(got) != 1 || got[0] != "beta" {
		t.Fatalf("nodesWithoutStats = %v, want [beta]", got)
	}
	if got := nodesWithoutStats(nodes, "alpha"); len(got) != 0 {
		t.Fatalf("filtered to node with stats: got %v, want none", got)
	}
	if got := nodesWithoutStats(nodes, "beta"); len(got) != 1 {
		t.Fatalf("filtered to stats-less node: got %v, want [beta]", got)
	}
}

// TestAlertNodeName verifies the attribution fallback chain: configured
// name → broadcast self-reported name → node ID. Never blank.
func TestAlertNodeName(t *testing.T) {
	t.Parallel()
	n := mkAlertNode("node-A", "alpha")
	if got := alertNodeName(n); got != "alpha" {
		t.Fatalf("configured name: got %q", got)
	}
	n.Name = ""
	n.Stats.NodeName = "self-reported"
	if got := alertNodeName(n); got != "self-reported" {
		t.Fatalf("broadcast name fallback: got %q", got)
	}
	n.Stats.NodeName = ""
	if got := alertNodeName(n); got != "node-A" {
		t.Fatalf("ID fallback: got %q", got)
	}
}

// TestAlertSeverityStr pins the one severity→display mapping point.
// When the alarm line replaces Severity with Priority, this test and
// alertSeverityStr are the whole CLI-side change.
func TestAlertSeverityStr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sev  v1.AlertSeverity
		want string
	}{
		{v1.AlertSeverity_ALERT_SEVERITY_WARNING, "WARNING"},
		{v1.AlertSeverity_ALERT_SEVERITY_ERROR, "ERROR"},
		{v1.AlertSeverity_ALERT_SEVERITY_UNSPECIFIED, "UNSPECIFIED"},
	}
	for _, tc := range tests {
		if got := alertSeverityStr(tc.sev); got != tc.want {
			t.Errorf("alertSeverityStr(%v) = %q, want %q", tc.sev, got, tc.want)
		}
	}
}

// TestSystemAlertRows verifies the cluster-status table rows: attributed,
// severity-labeled, and absent (nil) when nothing stands.
func TestSystemAlertRows(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodes := []*v1.ClusterNode{
		mkAlertNode("node-A", "alpha", mkAlert("wal:stall", v1.AlertSeverity_ALERT_SEVERITY_WARNING, "raftwal", "append latency degraded", t0)),
	}
	rows := systemAlertRows(nodes)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	want := []string{"alpha", "WARNING", "raftwal", "append latency degraded", "2026-07-17 10:00:00 UTC"}
	for i, w := range want {
		if rows[0][i] != w {
			t.Fatalf("row col %d = %q, want %q", i, rows[0][i], w)
		}
	}
	if rows := systemAlertRows(nil); len(rows) != 0 {
		t.Fatalf("no alerts: got %d rows, want 0", len(rows))
	}
}

// TestAlertsToJSON verifies the -o json shape keeps attribution and renders
// severity through the single mapping function.
func TestAlertsToJSON(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodes := []*v1.ClusterNode{
		mkAlertNode("node-A", "alpha", mkAlert("disk:v1", v1.AlertSeverity_ALERT_SEVERITY_ERROR, "diskguard", "disk protect engaged", t0)),
	}
	out := alertsToJSON(collectNodeAlerts(nodes, ""))
	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1", len(out))
	}
	j := out[0]
	if j.Node != "alpha" || j.NodeID != "node-A" {
		t.Fatalf("attribution: node=%q node_id=%q", j.Node, j.NodeID)
	}
	if j.Severity != "ERROR" || j.Source != "diskguard" || j.ID != "disk:v1" {
		t.Fatalf("fields: %+v", j)
	}
	if j.FirstSeen != "2026-07-17T10:00:00Z" {
		t.Fatalf("first_seen = %q", j.FirstSeen)
	}
	// Empty input marshals as an empty array, not null.
	if out := alertsToJSON(nil); out == nil || len(out) != 0 {
		t.Fatalf("empty input: got %#v, want empty non-nil slice", out)
	}
}
