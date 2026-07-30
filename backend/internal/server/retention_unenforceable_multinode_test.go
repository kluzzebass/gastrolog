package server_test

// Multi-node pin for the retention-unenforceable alarm lifecycle. The
// orchestrator-level tests (internal/orchestrator/
// retention_unenforceable_test.go) already cover retentionTargetForInstance
// and retentionSweepAll directly; these tests instead exercise the seam the
// unit tests cannot reach: a real per-node retention-sweep CRON tick
// (orchestrator.New registers it at construction — defaultRetentionSchedule,
// "0 * * * * *" — every node in this harness has one running from the
// moment setupMultiNode returns) raising an alarm that becomes visible
// through the cluster alarm surface (GetClusterStatus, the same RPC
// alerts_multinode_test.go's tests read), on a vault whose retention_rules
// were attached via the shared config store rather than an in-process call.
//
// Under WithClusterStats the harness builds each node's alert.Collector
// (h.alerts[id]) before node creation and threads it into
// orchestrator.Config, so production alarm-raising code — o.alerts.Raise /
// Clear, e.g. raiseRetentionUnenforceable — reaches the same collector
// GetClusterStatus reads from. Tests that call h.alerts[id].Raise(...)
// directly drive the bare collector and bypass that path; these do not.

import (
	"context"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// retentionSweepPeriod mirrors orchestrator.defaultRetentionSchedule ("0 * *
// * * *" — every minute on the minute). Unexported in the orchestrator
// package and there is no test-only trigger for it reachable from this
// external package (server_test) or any exported Orchestrator method — see
// this file's package comment. Waiting for the real cron is therefore the
// only non-stub way to observe this end to end from here.
const retentionSweepPeriod = 60 * time.Second

// mnAlarmWaitDeadline is comfortably more than one retentionSweepPeriod so a
// test that starts right after a tick still catches the next one, plus
// margin for scheduler jitter and slow CI.
const mnAlarmWaitDeadline = 75 * time.Second

// seedTriggerLessRetentionVault attaches a policy with no trigger set (no
// maxAge/maxSize/maxChunks) to nodeID's vault, writing directly to the
// harness's shared config store. That mirrors how such state reaches disk
// in the field: a Raft-log replay of an old wire format decoded as a bare
// {id, name}, below any RPC validation. PutRetentionPolicy the RPC rejects
// an empty policy outright (IsEmpty gate: "retention policy must set at
// least one of maxAge, maxBytes, or maxChunks") — going through the store
// directly is not a test shortcut, it is the only path that can reproduce
// that state at all, so this file's "no chunk destroyed" and "alarm
// visible" assertions run against production code exactly as the on-disk
// state presents it.
func seedTriggerLessRetentionVault(t *testing.T, h *multiNodeHarness, nodeID, policyName string) (policyID glid.GLID, alarmID string) {
	t.Helper()
	ctx := context.Background()
	target := h.Node(t, nodeID)

	policyID = glid.New()
	if err := h.cfgStore.PutRetentionPolicy(ctx, system.RetentionPolicyConfig{
		ID:   policyID,
		Name: policyName,
	}); err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}

	vaultCfg, err := h.cfgStore.GetVault(ctx, target.vaultID)
	if err != nil || vaultCfg == nil {
		t.Fatalf("GetVault: %v (cfg=%v)", err, vaultCfg)
	}
	vaultCfg.RetentionRules = []system.RetentionRule{{RetentionPolicyID: policyID}}
	if err := h.cfgStore.PutVault(ctx, *vaultCfg); err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	return policyID, "retention-unenforceable:" + target.vaultID.String()
}

// waitForMNAlarmPresence polls the coordinator's GetClusterStatus (via
// alertsByNode, alerts_multinode_test.go) until the given alarm ID's
// presence on nodeID matches want, or mnAlarmWaitDeadline expires. Mirrors
// waitForMNRouteStats' polling shape (multinode_test.go): poll for the
// observable effect, never a fixed sleep-as-sync.
func waitForMNAlarmPresence(t *testing.T, h *multiNodeHarness, nodeID, alarmID string, want bool) []*byNodeAlarmDetail {
	t.Helper()
	deadline := time.Now().Add(mnAlarmWaitDeadline)
	for {
		byNode := alertsByNode(t, h)
		var got []*byNodeAlarmDetail
		found := false
		for _, a := range byNode[nodeID] {
			got = append(got, &byNodeAlarmDetail{id: string(a.Id), detail: a.Detail})
			if string(a.Id) == alarmID {
				found = true
			}
		}
		if found == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("alarm %s presence=%v on node %s not reached within %s; node's alarms: %+v",
				alarmID, want, nodeID, mnAlarmWaitDeadline, got)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// byNodeAlarmDetail is a minimal projection of gastrologv1.SystemAlert used
// only for waitForMNAlarmPresence's failure message.
type byNodeAlarmDetail struct {
	id     string
	detail string
}

// TestMultiNodeRetentionUnenforceableAlarmVisibleAndNoDestruction covers
// the case where a trigger-less policy attached to a vault, across a
// real multi-node harness, makes the retention-unenforceable alarm visible
// through the cluster alarm surface, and destroys nothing while the
// condition holds.
func TestMultiNodeRetentionUnenforceableAlarmVisibleAndNoDestruction(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a real retention-sweep cron tick (defaultRetentionSchedule, 60s cadence)")
	}
	t.Parallel()

	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithClusterStats())
	target := h.Node(t, "data-1")

	// Seed a sealed, long-past-any-reasonable-TTL chunk: proof material for
	// "no destruction". A working TTL policy would destroy this on the very
	// sweep that (with a real trigger) would raise the alarm; the
	// trigger-less policy must leave it alone.
	addMNRecordsAt(t, target, "trigger-less", 5, time.Now().Add(-2*time.Hour))
	if err := target.vault.CM.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metasBefore, err := target.vault.CM.List()
	if err != nil || len(metasBefore) == 0 {
		t.Fatalf("fixture setup: want >=1 sealed chunk, got %d (err=%v)", len(metasBefore), err)
	}

	_, alarmID := seedTriggerLessRetentionVault(t, h, "data-1", "no-op-policy")

	got := waitForMNAlarmPresence(t, h, "data-1", alarmID, true)
	var detail string
	for _, a := range got {
		if a.id == alarmID {
			detail = a.detail
		}
	}
	if !strings.Contains(detail, "no-op-policy") {
		t.Errorf("alarm detail must name the trigger-less policy; got: %s", detail)
	}

	// Attribution: this alarm is instance-keyed by data-1's vault ID, which
	// no other node's vault shares, so it must never appear elsewhere —
	// mirrors alerts_multinode_test.go's TestMultiNodeAlerts_MultiNodeAttribution.
	byNode := alertsByNode(t, h)
	for _, id := range []string{"coord", "data-2"} {
		for _, a := range byNode[id] {
			if string(a.Id) == alarmID {
				t.Fatalf("node %s must not see data-1's vault-scoped alarm — attribution leaked", id)
			}
		}
	}

	metasAfter, err := target.vault.CM.List()
	if err != nil {
		t.Fatalf("List after sweep: %v", err)
	}
	if len(metasAfter) != len(metasBefore) {
		t.Fatalf("a trigger-less-policy vault must never have chunks destroyed while the alarm is standing: before=%d after=%d",
			len(metasBefore), len(metasAfter))
	}
}

// TestMultiNodeRetentionUnenforceableAlarmClearsWhenTriggerRestored covers
// the case where restoring a trigger on the policy (PutRetentionPolicy
// with max_age set) clears the alarm on a subsequent real sweep tick.
func TestMultiNodeRetentionUnenforceableAlarmClearsWhenTriggerRestored(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for two real retention-sweep cron ticks (defaultRetentionSchedule, 60s cadence)")
	}
	t.Parallel()

	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithClusterStats())
	policyID, alarmID := seedTriggerLessRetentionVault(t, h, "data-1", "no-op-policy")

	waitForMNAlarmPresence(t, h, "data-1", alarmID, true)

	ctx := context.Background()
	polCfg, err := h.cfgStore.GetRetentionPolicy(ctx, policyID)
	if err != nil || polCfg == nil {
		t.Fatalf("GetRetentionPolicy: %v (cfg=%v)", err, polCfg)
	}
	maxAge := "1h"
	polCfg.MaxAge = &maxAge
	if err := h.cfgStore.PutRetentionPolicy(ctx, *polCfg); err != nil {
		t.Fatalf("PutRetentionPolicy (restore trigger): %v", err)
	}

	waitForMNAlarmPresence(t, h, "data-1", alarmID, false)
}
