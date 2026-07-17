package cluster

import (
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
)

// TestStatsCollector_AlarmBroadcastShape pins the wire shape of alarms in
// the NodeStats broadcast: priority, cause and response come from the alarm
// catalog and travel with the alarm, so every peer (and the UI aggregating
// via PeerState) renders the same verdict and guidance regardless of which
// node raised it. This is the single conversion point — PeerState stores
// the NodeStats message verbatim, so what this test pins IS what remote
// nodes serve.
func TestStatsCollector_AlarmBroadcastShape(t *testing.T) {
	t.Parallel()
	// vault-leaderless carries a catalog DelayOn; the collector clock is
	// injected and advanced past it so the alarm is active on the wire —
	// suppression tests never wait on wall time.
	leaderlessType, ok := alert.TypeByID("vault-leaderless")
	if !ok {
		t.Fatal("vault-leaderless missing from the catalog")
	}
	var clockMu sync.Mutex
	now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	alarms := alert.NewWithClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	collector := NewStatsCollector(StatsCollectorConfig{
		Alerts:     alarms,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	alarms.Raise("vault-leaderless", "vault-1", "Vault a has had no placement leader for 90s.")
	clockMu.Lock()
	now = now.Add(leaderlessType.DelayOn + time.Second)
	clockMu.Unlock()
	alarms.Raise("orchestrator-lock-leak", "", "read hold stuck for 2m")
	alarms.RaiseOperator(alert.OperatorAlarm{
		TypeID: "retention-rate", InstanceKey: "vault-2",
		Priority: alert.Low, Source: "retention",
		Detail: "rate 12.00/s", Cause: "threshold crossed", Response: "review the rule",
	})

	stats := collector.CollectLocalTick(time.Now())
	if len(stats.Alerts) != 3 {
		t.Fatalf("alerts = %d, want 3", len(stats.Alerts))
	}
	byID := make(map[string]*gastrologv1.SystemAlert, len(stats.Alerts))
	for _, a := range stats.Alerts {
		byID[string(a.Id)] = a
	}

	catalog := byID["vault-leaderless:vault-1"]
	if catalog == nil {
		t.Fatal("cataloged alarm missing from broadcast")
	}
	if catalog.Priority != gastrologv1.AlarmPriority_ALARM_PRIORITY_HIGH {
		t.Errorf("priority = %v, want HIGH (stamped from the catalog)", catalog.Priority)
	}
	if catalog.Source != "placement" || catalog.Cause == "" || catalog.Response == "" {
		t.Errorf("catalog fields must travel on the wire: %+v", catalog)
	}
	if catalog.Detail != "Vault a has had no placement leader for 90s." {
		t.Errorf("detail = %q", catalog.Detail)
	}
	if catalog.SoftwareFault {
		t.Error("process alarm marked as software fault")
	}
	if catalog.FirstSeen == nil || catalog.LastSeen == nil {
		t.Error("timestamps missing")
	}

	fault := byID["orchestrator-lock-leak"]
	if fault == nil {
		t.Fatal("software fault missing from broadcast")
	}
	if !fault.SoftwareFault {
		t.Error("lock-leak must broadcast as a software fault")
	}
	if fault.Priority != gastrologv1.AlarmPriority_ALARM_PRIORITY_UNSPECIFIED {
		t.Errorf("software faults sit outside the priority scale, got %v", fault.Priority)
	}

	operator := byID["retention-rate:vault-2"]
	if operator == nil {
		t.Fatal("operator-defined alarm missing from broadcast")
	}
	if operator.Priority != gastrologv1.AlarmPriority_ALARM_PRIORITY_LOW {
		t.Errorf("operator priority = %v, want LOW (from the rule)", operator.Priority)
	}
	if operator.Cause != "threshold crossed" || operator.Response != "review the rule" {
		t.Errorf("operator rule text must travel on the wire: %+v", operator)
	}

	// Clearing removes the alarm from the next broadcast.
	alarms.Clear("vault-leaderless", "vault-1")
	stats = collector.CollectLocalTick(time.Now().Add(time.Second))
	if len(stats.Alerts) != 2 {
		t.Fatalf("alerts = %d after clear, want 2", len(stats.Alerts))
	}
}

// TestStatsCollector_AlarmRateGaugeAndTypeID pins the self-monitoring
// additions to the broadcast: the per-node rolling alarm-rate gauge
// (NodeStats.alarm_rate_10m) and the explicit type_id on every alarm, which
// the UI's flood collapse groups by. The monitor's clock is injected — the
// window is a time construct and is never tested with sleeps.
func TestStatsCollector_AlarmRateGaugeAndTypeID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	alarms := alert.New()
	monitor := alert.NewRateMonitor(alarms, func() time.Time { return now })
	alarms.SetOnActivate(monitor.Observe)

	collector := NewStatsCollector(StatsCollectorConfig{
		Alerts:     alarms,
		AlarmRate:  monitor,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	// Zero-delay catalog types: activation is immediate, so the gauge sees
	// them on this tick. (vault-leaderless would sit pending in its 60s
	// delay-on window and never count here.)
	alarms.Raise("disk-space-exhausted", "vault-1", "volume full")
	alarms.Raise("node-unreachable", "peer-1", "gone")
	alarms.Raise("node-unreachable", "peer-1", "still gone") // refresh, not an activation

	stats := collector.CollectLocalTick(now)
	if stats.AlarmRate_10M != 2 {
		t.Errorf("alarm_rate_10m = %d, want 2 (refresh must not count)", stats.AlarmRate_10M)
	}
	for _, a := range stats.Alerts {
		if a.TypeId == "" {
			t.Errorf("alert %q broadcast without type_id", a.Id)
		}
	}

	// Aged-out activations leave the gauge; the collector snapshot follows.
	now = now.Add(11 * time.Minute)
	stats = collector.CollectLocalTick(now)
	if stats.AlarmRate_10M != 0 {
		t.Errorf("alarm_rate_10m = %d after the window rolled, want 0", stats.AlarmRate_10M)
	}
}
