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

func mkAlert(id string, pri v1.AlarmPriority, source, detail string, firstSeen time.Time) *v1.SystemAlert {
	return &v1.SystemAlert{
		Id:        []byte(id),
		Priority:  pri,
		Source:    source,
		Detail:    detail,
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
		mkAlertNode("node-A", "alpha", mkAlert("disk-space-exhausted:v1", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "disk protect engaged", t0.Add(2*time.Second))),
		mkAlertNode("node-B", "beta", mkAlert("disk-space-exhausted:v1", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "disk protect engaged", t0)),
	}

	got := collectNodeAlerts(nodes, "")
	if len(got) != 2 {
		t.Fatalf("collectNodeAlerts returned %d rows, want 2", len(got))
	}
	// Sorted by FirstSeen: beta's alert is older.
	if got[0].node != "beta" || got[1].node != "alpha" {
		t.Fatalf("attribution/order wrong: got [%s, %s], want [beta, alpha]", got[0].node, got[1].node)
	}
	if string(got[0].alert.Id) != "disk-space-exhausted:v1" || string(got[1].alert.Id) != "disk-space-exhausted:v1" {
		t.Fatalf("alert IDs lost in collection: %q, %q", got[0].alert.Id, got[1].alert.Id)
	}
}

// TestCollectNodeAlerts_NodeFilter verifies --node matching against
// configured name, broadcast name, and node ID, case-insensitively.
func TestCollectNodeAlerts_NodeFilter(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	a := mkAlertNode("node-A", "alpha", mkAlert("x", v1.AlarmPriority_ALARM_PRIORITY_LOW, "chunking", "m1", t0))
	b := mkAlertNode("node-B", "", mkAlert("y", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "m2", t0))
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
		mkAlertNode("node-A", "alpha", mkAlert("x", v1.AlarmPriority_ALARM_PRIORITY_LOW, "chunking", "m", t0)),
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

// TestAlarmPriorityStr pins the one priority→display mapping point.
// Software faults sit outside the priority scale and render as FAULT.
func TestAlarmPriorityStr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		alert *v1.SystemAlert
		want  string
	}{
		{&v1.SystemAlert{Priority: v1.AlarmPriority_ALARM_PRIORITY_LOW}, "LOW"},
		{&v1.SystemAlert{Priority: v1.AlarmPriority_ALARM_PRIORITY_HIGH}, "HIGH"},
		{&v1.SystemAlert{Priority: v1.AlarmPriority_ALARM_PRIORITY_CRITICAL}, "CRITICAL"},
		{&v1.SystemAlert{Priority: v1.AlarmPriority_ALARM_PRIORITY_UNSPECIFIED}, "UNSPECIFIED"},
		{&v1.SystemAlert{SoftwareFault: true}, "FAULT"},
		// A fault outranks any priority value that might also be set.
		{&v1.SystemAlert{Priority: v1.AlarmPriority_ALARM_PRIORITY_CRITICAL, SoftwareFault: true}, "FAULT"},
	}
	for _, tc := range tests {
		if got := alarmPriorityStr(tc.alert); got != tc.want {
			t.Errorf("alarmPriorityStr(%+v) = %q, want %q", tc.alert, got, tc.want)
		}
	}
}

// TestAlarmTypeID verifies type extraction from full alarm IDs: instance
// alarms carry "type:instance", node-scoped alarms are bare type IDs.
func TestAlarmTypeID(t *testing.T) {
	t.Parallel()
	if got := alarmTypeID("disk-space-exhausted:vault1"); got != "disk-space-exhausted" {
		t.Fatalf("instance-scoped: got %q", got)
	}
	if got := alarmTypeID("node-disk-space-exhausted"); got != "node-disk-space-exhausted" {
		t.Fatalf("node-scoped: got %q", got)
	}
}

// TestAlarmResponseLines verifies the response section dedupes by alarm
// type: response text is a property of the type, so many instances of one
// type yield one guidance line, and alarms with no response yield none.
func TestAlarmResponseLines(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	withResponse := func(a *v1.SystemAlert, resp string) *v1.SystemAlert {
		a.Response = resp
		return a
	}
	alerts := []nodeAlert{
		{node: "alpha", alert: withResponse(mkAlert("disk-space-exhausted:v1", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "d1", t0), "free disk space")},
		{node: "beta", alert: withResponse(mkAlert("disk-space-exhausted:v2", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "d2", t0), "free disk space")},
		{node: "alpha", alert: mkAlert("no-response-type", v1.AlarmPriority_ALARM_PRIORITY_LOW, "x", "d3", t0)},
	}
	lines := alarmResponseLines(alerts)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 (deduped by type, no-response skipped): %v", len(lines), lines)
	}
	if lines[0] != "  disk-space-exhausted: free disk space" {
		t.Fatalf("line = %q", lines[0])
	}
	if lines := alarmResponseLines(nil); len(lines) != 0 {
		t.Fatalf("no alerts: got %v", lines)
	}
}

// TestSystemAlertRows verifies the cluster-status table rows: attributed,
// priority-labeled, and absent (nil) when nothing stands.
func TestSystemAlertRows(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodes := []*v1.ClusterNode{
		mkAlertNode("node-A", "alpha", mkAlert("wal-reserve:wal1", v1.AlarmPriority_ALARM_PRIORITY_CRITICAL, "raft", "reserve below floor", t0)),
	}
	rows := systemAlertRows(nodes)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	want := []string{"alpha", "active", "CRITICAL", "raft", "reserve below floor", "2026-07-17 10:00:00 UTC"}
	for i, w := range want {
		if rows[0][i] != w {
			t.Fatalf("row col %d = %q, want %q", i, rows[0][i], w)
		}
	}
	if rows := systemAlertRows(nil); len(rows) != 0 {
		t.Fatalf("no alerts: got %d rows, want 0", len(rows))
	}
}

// TestAlertsToJSON verifies the -o json shape keeps attribution, renders
// priority through the single mapping function, and carries the catalog
// text (cause/response) so scripted consumers get the full alarm.
func TestAlertsToJSON(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	a := mkAlert("disk-space-exhausted:v1", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "diskguard", "disk protect engaged", t0)
	a.Cause = "the vault's volume is out of space"
	a.Response = "free disk space or raise the budget"
	nodes := []*v1.ClusterNode{mkAlertNode("node-A", "alpha", a)}
	out := alertsToJSON(collectNodeAlerts(nodes, ""))
	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1", len(out))
	}
	j := out[0]
	if j.Node != "alpha" || j.NodeID != "node-A" {
		t.Fatalf("attribution: node=%q node_id=%q", j.Node, j.NodeID)
	}
	if j.Priority != "HIGH" || j.Source != "diskguard" || j.ID != "disk-space-exhausted:v1" {
		t.Fatalf("fields: %+v", j)
	}
	if j.Detail != "disk protect engaged" || j.Cause == "" || j.Response == "" {
		t.Fatalf("catalog text missing: %+v", j)
	}
	if j.SoftwareFault {
		t.Fatalf("software_fault should be false: %+v", j)
	}
	if j.FirstSeen != "2026-07-17T10:00:00Z" {
		t.Fatalf("first_seen = %q", j.FirstSeen)
	}
	// Empty input marshals as an empty array, not null.
	if out := alertsToJSON(nil); out == nil || len(out) != 0 {
		t.Fatalf("empty input: got %#v, want empty non-nil slice", out)
	}
}

// TestAlarmStateStr pins the single lifecycle-state→display mapping
// (gastrolog-1z5gg4): active, acked (with who), cleared, shelved (with
// expiry) — and the wire back-compat default of "active" for UNSPECIFIED.
func TestAlarmStateStr(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	a := mkAlert("x", v1.AlarmPriority_ALARM_PRIORITY_HIGH, "s", "d", t0)
	if got := alarmStateStr(a); got != "active" {
		t.Errorf("unspecified = %q, want active", got)
	}
	a.State = v1.AlarmState_ALARM_STATE_ACTIVE_UNACKED
	if got := alarmStateStr(a); got != "active" {
		t.Errorf("active-unacked = %q, want active", got)
	}
	a.State = v1.AlarmState_ALARM_STATE_ACTIVE_ACKED
	a.AckedBy = "alice"
	if got := alarmStateStr(a); got != "acked:alice" {
		t.Errorf("active-acked = %q, want acked:alice", got)
	}
	a.State = v1.AlarmState_ALARM_STATE_CLEARED_UNACKED
	if got := alarmStateStr(a); got != "cleared" {
		t.Errorf("cleared-unacked = %q, want cleared", got)
	}
	a.State = v1.AlarmState_ALARM_STATE_SHELVED
	a.ShelvedUntil = timestamppb.New(t0.Add(time.Hour))
	if got := alarmStateStr(a); got != "shelved→2026-07-17 11:00:00 UTC" {
		t.Errorf("shelved = %q", got)
	}
}
