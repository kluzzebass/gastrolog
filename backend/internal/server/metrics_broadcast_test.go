package server

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/cluster"
)

// stubBroadcastMetrics returns a fixed snapshot for tests.
type stubBroadcastMetrics struct {
	sent, errs []cluster.BroadcastCounter
}

func (s *stubBroadcastMetrics) MetricsSnapshot() ([]cluster.BroadcastCounter, []cluster.BroadcastCounter) {
	return s.sent, s.errs
}

// stubPeerAges returns a fixed receive-timestamp map for tests.
type stubPeerAges struct {
	at map[string]time.Time
}

func (s *stubPeerAges) ReceivedAt() map[string]time.Time { return s.at }

// TestWriteBroadcastCounters_SortedAndLabeled verifies the counter family
// emits HELP/TYPE headers and one line per (peer, type) with stable
// ordering (peer, then type).
func TestWriteBroadcastCounters_SortedAndLabeled(t *testing.T) {
	counters := []cluster.BroadcastCounter{
		{Peer: "b", Type: "node_jobs", Value: 2},
		{Peer: "a", Type: "node_stats", Value: 5},
		{Peer: "a", Type: "node_jobs", Value: 3},
	}

	var buf bytes.Buffer
	writeBroadcastCounters(&buf, "gastrolog_broadcast_send_total", "Total.", counters)
	got := buf.String()

	wantLines := []string{
		`# HELP gastrolog_broadcast_send_total Total.`,
		`# TYPE gastrolog_broadcast_send_total counter`,
		`gastrolog_broadcast_send_total{peer="a",type="node_jobs"} 3`,
		`gastrolog_broadcast_send_total{peer="a",type="node_stats"} 5`,
		`gastrolog_broadcast_send_total{peer="b",type="node_jobs"} 2`,
	}
	for _, want := range wantLines {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	// Verify ordering: "a" lines appear before "b" lines.
	idxA := strings.Index(got, `{peer="a"`)
	idxB := strings.Index(got, `{peer="b"`)
	if idxA < 0 || idxB < 0 || idxA > idxB {
		t.Errorf("peer ordering wrong: idxA=%d idxB=%d output:\n%s", idxA, idxB, got)
	}
}

// TestWriteBroadcastCounters_EmptyStillEmitsHeaders verifies we always
// emit the HELP/TYPE lines even when there's no data yet — keeps the
// exposition stable for Prometheus and makes the metric discoverable.
func TestWriteBroadcastCounters_EmptyStillEmitsHeaders(t *testing.T) {
	var buf bytes.Buffer
	writeBroadcastCounters(&buf, "name", "help", nil)
	got := buf.String()
	if !strings.Contains(got, "# HELP name help\n") {
		t.Errorf("missing HELP line: %s", got)
	}
	if !strings.Contains(got, "# TYPE name counter\n") {
		t.Errorf("missing TYPE line: %s", got)
	}
}

// TestWriteBroadcastPeerAge_MergesAndSkipsZero verifies the peer-age gauge
// merges stats + jobs providers, skips zero timestamps (never received),
// and produces sorted output.
func TestWriteBroadcastPeerAge_MergesAndSkipsZero(t *testing.T) {
	now := time.Now()
	statsAges := &stubPeerAges{at: map[string]time.Time{
		"peer-a": now.Add(-10 * time.Second),
		"peer-b": {}, // zero — skipped
	}}
	jobsAges := &stubPeerAges{at: map[string]time.Time{
		"peer-a": now.Add(-2 * time.Second),
	}}

	var buf bytes.Buffer
	writeBroadcastPeerAge(&buf, "gastrolog_broadcast_peer_message_age_seconds", "help.",
		statsAges, "node_stats", jobsAges, "node_jobs")
	got := buf.String()

	// peer-a appears twice (once per type), peer-b is skipped entirely.
	if !strings.Contains(got, `{peer="peer-a",type="node_jobs"}`) {
		t.Errorf("missing peer-a jobs line: %s", got)
	}
	if !strings.Contains(got, `{peer="peer-a",type="node_stats"}`) {
		t.Errorf("missing peer-a stats line: %s", got)
	}
	if strings.Contains(got, `peer-b`) {
		t.Errorf("zero-timestamp peer should be skipped: %s", got)
	}
	// type ordering within same peer: "node_jobs" before "node_stats" (alphabetical).
	idxJobs := strings.Index(got, `type="node_jobs"`)
	idxStats := strings.Index(got, `type="node_stats"`)
	if idxJobs < 0 || idxStats < 0 || idxJobs > idxStats {
		t.Errorf("type ordering wrong: idxJobs=%d idxStats=%d output:\n%s", idxJobs, idxStats, got)
	}
}

// TestWriteBroadcastPeerAge_NilProviders verifies both providers can be nil
// without crashing (single-node deployments).
func TestWriteBroadcastPeerAge_NilProviders(t *testing.T) {
	var buf bytes.Buffer
	writeBroadcastPeerAge(&buf, "name", "help", nil, "node_stats", nil, "node_jobs")
	got := buf.String()
	// Still emits HELP/TYPE.
	if !strings.Contains(got, "# HELP name") {
		t.Errorf("missing HELP: %s", got)
	}
	if !strings.Contains(got, "# TYPE name gauge") {
		t.Errorf("missing TYPE: %s", got)
	}
	// No value lines.
	if strings.Contains(got, "{peer=") {
		t.Errorf("nil providers should not emit value lines: %s", got)
	}
}

// TestWriteBroadcastPeerAge_AgeValueApprox verifies the gauge value is
// approximately now - received (in seconds). Tolerates some slack for test
// latency.
func TestWriteBroadcastPeerAge_AgeValueApprox(t *testing.T) {
	now := time.Now()
	statsAges := &stubPeerAges{at: map[string]time.Time{
		"peer-a": now.Add(-5 * time.Second),
	}}
	var buf bytes.Buffer
	writeBroadcastPeerAge(&buf, "name", "help.", statsAges, "node_stats", nil, "node_jobs")
	got := buf.String()

	// Expect a line like: name{peer="peer-a",type="node_stats"} 5.001
	if !strings.Contains(got, `name{peer="peer-a",type="node_stats"} 5.`) &&
		!strings.Contains(got, `name{peer="peer-a",type="node_stats"} 4.9`) {
		// Loose tolerance — clock jitter, test scheduler — allow 4.9–5.3s.
		if !strings.Contains(got, `name{peer="peer-a",type="node_stats"} 5`) {
			t.Errorf("age value out of expected range:\n%s", got)
		}
	}
}
