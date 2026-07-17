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
	alarms.Raise("retention-rate", "vault-2", "rate 12.00/s")

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

	// retention-rate is an ordinary catalog row (gastrolog-1cruar) — its
	// priority, cause and response come from the catalog like any other type.
	rate := byID["retention-rate:vault-2"]
	if rate == nil {
		t.Fatal("retention-rate alarm missing from broadcast")
	}
	if rate.Priority != gastrologv1.AlarmPriority_ALARM_PRIORITY_LOW {
		t.Errorf("retention-rate priority = %v, want LOW (stamped from the catalog)", rate.Priority)
	}
	if rate.Source != "retention" || rate.Cause == "" || rate.Response == "" {
		t.Errorf("retention-rate catalog fields must travel on the wire: %+v", rate)
	}
	if rate.Detail != "rate 12.00/s" {
		t.Errorf("retention-rate detail = %q", rate.Detail)
	}

	// Every broadcast alarm carries its lifecycle state and the catalog's
	// shelveability verdict (gastrolog-1z5gg4).
	if catalog.State != gastrologv1.AlarmState_ALARM_STATE_ACTIVE_UNACKED {
		t.Errorf("state = %v, want ACTIVE_UNACKED", catalog.State)
	}
	if !catalog.Shelveable {
		t.Error("process alarm must broadcast as shelveable")
	}
	if fault.Shelveable {
		t.Error("software fault must broadcast as unshelveable")
	}
	if catalog.Occurrences != 1 {
		t.Errorf("occurrences = %d, want 1", catalog.Occurrences)
	}

	// Clearing an unacknowledged alarm retains it as cleared-unacked on the
	// wire — "it fired while you were away" stays visible until acked
	// (gastrolog-1z5gg4); acknowledging it releases it from the broadcast.
	alarms.Clear("vault-leaderless", "vault-1")
	stats = collector.CollectLocalTick(time.Now().Add(time.Second))
	if len(stats.Alerts) != 3 {
		t.Fatalf("alerts = %d after clear, want 3 (cleared-unacked retained)", len(stats.Alerts))
	}
	for _, a := range stats.Alerts {
		if string(a.Id) == "vault-leaderless:vault-1" && a.State != gastrologv1.AlarmState_ALARM_STATE_CLEARED_UNACKED {
			t.Errorf("cleared alarm state = %v, want CLEARED_UNACKED", a.State)
		}
	}
	if err := alarms.Ack("vault-leaderless:vault-1", "op"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	stats = collector.CollectLocalTick(time.Now().Add(2 * time.Second))
	if len(stats.Alerts) != 2 {
		t.Fatalf("alerts = %d after ack of cleared alarm, want 2", len(stats.Alerts))
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
