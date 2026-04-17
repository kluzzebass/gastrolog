package cluster

import (
	"io"
	"log/slog"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

func quietMetricsLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestPayloadType covers every oneof variant plus nil/unknown fallbacks so
// the Prometheus label for a broadcast message is stable.
func TestPayloadType(t *testing.T) {
	tests := []struct {
		name string
		msg  *gastrologv1.BroadcastMessage
		want string
	}{
		{"nil", nil, "unknown"},
		{"empty-payload", &gastrologv1.BroadcastMessage{}, "unknown"},
		{
			"node-stats",
			&gastrologv1.BroadcastMessage{Payload: &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{}}},
			"node_stats",
		},
		{
			"node-jobs",
			&gastrologv1.BroadcastMessage{Payload: &gastrologv1.BroadcastMessage_NodeJobs{NodeJobs: &gastrologv1.NodeJobs{}}},
			"node_jobs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PayloadType(tt.msg); got != tt.want {
				t.Errorf("PayloadType = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBroadcaster_MetricsSnapshot_Empty verifies a fresh Broadcaster reports
// no counters.
func TestBroadcaster_MetricsSnapshot_Empty(t *testing.T) {
	b := NewBroadcaster(nil, nil)
	b.logger = quietMetricsLogger()
	sent, errs := b.MetricsSnapshot()
	if len(sent) != 0 || len(errs) != 0 {
		t.Errorf("fresh broadcaster reports counters: sent=%v errors=%v", sent, errs)
	}
}

// TestBroadcaster_MetricsSnapshot_RecordAndAggregate verifies the counters
// aggregate correctly across (peer, type) combinations.
func TestBroadcaster_MetricsSnapshot_RecordAndAggregate(t *testing.T) {
	b := NewBroadcaster(nil, nil)
	b.logger = quietMetricsLogger()

	b.recordSent("a", "node_stats")
	b.recordSent("a", "node_stats")
	b.recordSent("a", "node_jobs")
	b.recordSent("b", "node_stats")
	b.recordError("a", "node_stats")
	b.recordError("b", "node_jobs")
	b.recordError("b", "node_jobs")

	sent, errs := b.MetricsSnapshot()

	want := map[metricsKey]int64{
		{"a", "node_stats"}: 2,
		{"a", "node_jobs"}:  1,
		{"b", "node_stats"}: 1,
	}
	if got := countersToMap(sent); !mapsEqual(got, want) {
		t.Errorf("sent snapshot: got=%v want=%v", got, want)
	}

	wantErr := map[metricsKey]int64{
		{"a", "node_stats"}: 1,
		{"b", "node_jobs"}:  2,
	}
	if got := countersToMap(errs); !mapsEqual(got, wantErr) {
		t.Errorf("error snapshot: got=%v want=%v", got, wantErr)
	}
}

// TestBroadcaster_MetricsSnapshot_Independent verifies that sent and errors
// are tracked independently for the same (peer, type) key.
func TestBroadcaster_MetricsSnapshot_Independent(t *testing.T) {
	b := NewBroadcaster(nil, nil)
	b.logger = quietMetricsLogger()
	b.recordSent("a", "node_stats")
	b.recordError("a", "node_stats")

	sent, errs := b.MetricsSnapshot()
	if len(sent) != 1 || sent[0].Value != 1 {
		t.Errorf("unexpected sent: %v", sent)
	}
	if len(errs) != 1 || errs[0].Value != 1 {
		t.Errorf("unexpected errors: %v", errs)
	}
}

func countersToMap(cs []BroadcastCounter) map[metricsKey]int64 {
	m := make(map[metricsKey]int64, len(cs))
	for _, c := range cs {
		m[metricsKey{peer: c.Peer, typ: c.Type}] = c.Value
	}
	return m
}

func mapsEqual(a, b map[metricsKey]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
