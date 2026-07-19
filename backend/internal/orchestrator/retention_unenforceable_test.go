package orchestrator

// gastrolog-1xl29s: a vault whose retention_rules resolve to zero usable
// policies (every referenced policy is trigger-less — no maxAge, maxSize, or
// maxChunks set) used to exit retentionTargetForInstance silently: no log,
// no alarm, and the vault's only drain simply never ran again. Same class
// for the HasRaftLeader()==false early return. These tests pin the fix:
// case 3 (trigger-less policies) raises alarmRetentionUnenforceable and logs
// a throttled warn; case 1 (no vault-ctl Raft leader) logs a throttled warn
// (a genuinely different condition than the vault-leaderless alarm, which
// tracks config PLACEMENT leader resolution, not this vault's own Raft
// group election state — see buildVaultRaftCallbacks' hasLeader callback).
// A vault with no retention_rules at all stays silent, and no chunk is ever
// destroyed while the condition holds.

import (
	"log/slog"
	"strings"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// triggerLessPolicyCfg returns a vault + retention-policy config pair whose
// single referenced policy has none of MaxAge/MaxSize/MaxChunks set — the
// live-incident shape: ToRetentionPolicy() returns (nil, nil) for it, and
// resolveRetentionRulesFromVault silently drops it (reconfig_vaults.go).
func triggerLessPolicyCfg(vaultID, policyID glid.GLID, vaultName, policyName string) *system.Config {
	return &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    vaultName,
			Enabled: true,
			RetentionRules: []system.RetentionRule{{
				RetentionPolicyID: policyID,
			}},
		}},
		RetentionPolicies: []system.RetentionPolicyConfig{{
			ID:   policyID,
			Name: policyName,
			// MaxAge / MaxSize / MaxChunks all nil: trigger-less.
		}},
	}
}

func newUnenforceableTestOrch(t *testing.T, sink *recordingSink, logSink *syncBuffer) *Orchestrator {
	t.Helper()
	orch := newTestOrch(t, Config{Logger: slog.New(slog.NewTextHandler(logSink, nil))})
	orch.alerts = sink
	orch.retentionLogger = orch.logger
	return orch
}

func TestRetentionTargetRaisesUnenforceableAlarmWhenAllPoliciesTriggerLess(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	cfg := triggerLessPolicyCfg(vaultID, policyID, "first-vault", "no-op-policy")

	sink := &recordingSink{}
	logSink := &syncBuffer{}
	orch := newUnenforceableTestOrch(t, sink, logSink)

	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: &retentionFakeChunkManager{}, Indexes: &retentionFakeIndexManager{}}
	active := make(map[string]bool)

	target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active)
	if target != nil {
		t.Fatal("a vault whose rules all resolve trigger-less must not produce a sweep target")
	}

	sink.mu.Lock()
	raises := append([]string(nil), sink.raises...)
	sink.mu.Unlock()
	if len(raises) != 1 {
		t.Fatalf("want exactly 1 alarm raise, got %d: %v", len(raises), raises)
	}
	got := raises[0]
	if !strings.HasPrefix(got, alarmRetentionUnenforceable+"|"+vaultID.String()+"|") {
		t.Errorf("raise must be typed and instance-keyed by vault: %s", got)
	}
	for _, want := range []string{"first-vault", "no-op-policy"} {
		if !strings.Contains(got, want) {
			t.Errorf("alarm detail must name the vault and the trigger-less polic(ies); got: %s", got)
		}
	}

	logs := logSink.String()
	if !strings.Contains(logs, "first-vault") || !strings.Contains(logs, "no-op-policy") {
		t.Errorf("a throttled log line must name the vault and the trigger-less policy; got:\n%s", logs)
	}
}

func TestRetentionTargetClearsUnenforceableAlarmWhenTriggerRestored(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	cfg := triggerLessPolicyCfg(vaultID, policyID, "first-vault", "no-op-policy")

	sink := &recordingSink{}
	orch := newUnenforceableTestOrch(t, sink, &syncBuffer{})

	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: &retentionFakeChunkManager{}, Indexes: &retentionFakeIndexManager{}}
	active := make(map[string]bool)

	if target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active); target != nil {
		t.Fatal("expected nil target on the first (trigger-less) pass")
	}
	sink.mu.Lock()
	raisedBefore := len(sink.raises)
	sink.mu.Unlock()
	if raisedBefore != 1 {
		t.Fatalf("want 1 raise before the fix, got %d", raisedBefore)
	}

	// Restore a real trigger on the same policy.
	cfg.RetentionPolicies[0].MaxAge = strPtr("1h")
	active2 := make(map[string]bool)
	target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active2)
	if target == nil {
		t.Fatal("a restored trigger must produce a live sweep target on the next build")
	}
	if len(target.rules) != 1 {
		t.Fatalf("want exactly 1 resolved rule, got %d", len(target.rules))
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := false
	for _, c := range sink.clears {
		if c == alarmRetentionUnenforceable+"|"+vaultID.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("restoring the trigger must clear the alarm; clears=%v", sink.clears)
	}
}

func TestRetentionTargetNoRulesAtAllStaysSilent(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "unconfigured-vault",
			Enabled: true,
			// No RetentionRules at all — legitimately unconfigured.
		}},
	}

	sink := &recordingSink{}
	logSink := &syncBuffer{}
	orch := newUnenforceableTestOrch(t, sink, logSink)

	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: &retentionFakeChunkManager{}, Indexes: &retentionFakeIndexManager{}}
	active := make(map[string]bool)

	if target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active); target != nil {
		t.Fatal("expected nil target for an unconfigured vault")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 0 {
		t.Fatalf("an unconfigured vault must never raise an alarm; got %v", sink.raises)
	}
	if strings.Contains(logSink.String(), "unenforceable") {
		t.Errorf("an unconfigured vault must stay silent; got log:\n%s", logSink.String())
	}
}

// TestRetentionTargetNoRulesClearsAlarmWhenRulesRemoved covers the
// "rules removed entirely" no-strand path: a vault previously raised the
// alarm (trigger-less policy attached) and the operator then removes the
// vault's retention_rules outright rather than fixing the policy. The
// condition no longer holds — the alarm must clear even though this branch
// stays silent about logging.
func TestRetentionTargetNoRulesClearsAlarmWhenRulesRemoved(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	cfg := triggerLessPolicyCfg(vaultID, policyID, "first-vault", "no-op-policy")

	sink := &recordingSink{}
	orch := newUnenforceableTestOrch(t, sink, &syncBuffer{})

	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: &retentionFakeChunkManager{}, Indexes: &retentionFakeIndexManager{}}
	active := make(map[string]bool)
	if target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active); target != nil {
		t.Fatal("expected nil target on the trigger-less pass")
	}

	// Operator removes the vault's retention_rules entirely.
	cfg.Vaults[0].RetentionRules = nil
	active2 := make(map[string]bool)
	if target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active2); target != nil {
		t.Fatal("expected nil target once retention_rules is empty")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := false
	for _, c := range sink.clears {
		if c == alarmRetentionUnenforceable+"|"+vaultID.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("removing retention_rules entirely must clear the alarm; clears=%v", sink.clears)
	}
}

// TestRetentionTargetHasRaftLeaderFalseLogsThrottledWarnNoAlarm pins case 1:
// no vault-ctl Raft leader is a distinct condition from both
// vault-leaderless (config PLACEMENT leader — see leaderless_alarm.go) and
// retention-unenforceable (case 3). It gets a throttled log, not this
// alarm — vault-leaderless already covers the adjacent placement-leader
// gap, and this raw-Raft-election condition doesn't have the sustained-vs-
// transient distinction (mid-election blips) that would justify inventing
// a second alarm type here.
func TestRetentionTargetHasRaftLeaderFalseLogsThrottledWarnNoAlarm(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Name: "first-vault", Enabled: true}},
	}

	sink := &recordingSink{}
	logSink := &syncBuffer{}
	orch := newUnenforceableTestOrch(t, sink, logSink)

	vaultInst := &VaultInstance{
		VaultID:       vaultID,
		Chunks:        &retentionFakeChunkManager{},
		Indexes:       &retentionFakeIndexManager{},
		HasRaftLeader: func() bool { return false },
	}
	active := make(map[string]bool)

	if target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active); target != nil {
		t.Fatal("no raft leader must never produce a sweep target")
	}

	sink.mu.Lock()
	raises := len(sink.raises)
	sink.mu.Unlock()
	if raises != 0 {
		t.Fatalf("no-raft-leader must not raise retention-unenforceable (vault-leaderless already covers the adjacent gap); got %d raises", raises)
	}

	logs := logSink.String()
	if !strings.Contains(logs, "first-vault") {
		t.Errorf("a throttled warn naming the vault must be emitted; got:\n%s", logs)
	}

	// Second call immediately after: throttled, so it must not double the
	// log line count for this reason.
	active2 := make(map[string]bool)
	orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active2)
	if got := strings.Count(logSink.String(), "no leader"); got > 1 {
		t.Errorf("second call within the throttle interval must be suppressed; got %d occurrences", got)
	}
}

// TestRetentionSweepAllClearsUnenforceableAlarmOnRunnerGC mirrors
// TestRetentionSweepAllClearsAlarmOnRunnerGC (retention_deferral_test.go)
// for the new alarm: a pruned runner's standing retention-unenforceable
// alarm must be cleared by the GC pass, the same no-strand guarantee the
// deferral alarm already has (disk_guard.go retainVaultGuards is the third
// precedent for this pattern).
func TestRetentionSweepAllClearsUnenforceableAlarmOnRunnerGC(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	o.alerts = sink

	store := sysmem.NewStore()
	_ = store.PutVault(t.Context(), system.VaultConfig{ID: glid.New(), Name: "other", Type: system.VaultTypeMemory})
	o.sysLoader = &transitionSystemLoader{store: store}

	vaultID := glid.New()
	o.mu.Lock()
	if o.retention == nil {
		o.retention = make(map[string]*retentionRunner)
	}
	o.retention[vaultID.String()] = &retentionRunner{vaultID: vaultID, vaultName: "gc-vault"}
	o.mu.Unlock()

	o.retentionSweepAll()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := false
	for _, c := range sink.clears {
		if c == alarmRetentionUnenforceable+"|"+vaultID.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("runner GC must clear the unenforceable alarm too; clears=%v", sink.clears)
	}
}

// TestRetentionSweepAllDoesNotDestroyChunksWhenPolicyTriggerLess proves the
// "no chunk destruction" half of the acceptance criteria at the sweep-all
// seam: a vault with sealed chunks and a trigger-less retention rule must
// come out of a full retentionSweepAll pass with nothing deleted, because
// retentionTargetForInstance excludes it from `targets` entirely — sweep()
// (the only place that destroys chunks) is never invoked for it.
func TestRetentionSweepAllDoesNotDestroyChunksWhenPolicyTriggerLess(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	cfg := triggerLessPolicyCfg(vaultID, policyID, "first-vault", "no-op-policy")

	sink := &recordingSink{}
	orch := newUnenforceableTestOrch(t, sink, &syncBuffer{})
	orch.sysLoader = testSystemLoader{cfg: cfg}

	cm := &retentionFakeChunkManager{chunks: []chunk.ChunkMeta{{
		ID:     chunk.ChunkID{},
		Sealed: true,
	}}}
	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: cm, Indexes: &retentionFakeIndexManager{}}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	orch.retentionSweepAll()

	cm.mu.Lock()
	deleted := len(cm.deleted)
	cm.mu.Unlock()
	if deleted != 0 {
		t.Fatalf("a trigger-less-policy vault must never have chunks destroyed; deleted=%d", deleted)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 1 {
		t.Fatalf("want exactly 1 alarm raise from the full sweep pass, got %d: %v", len(sink.raises), sink.raises)
	}
}

// TestRetentionSweepAllNeverInvokesSweepOnFollowerInstance answers the
// brief's last acceptance bullet directly: the pre-incident cluster.log had
// ZERO "retention sweep idle" lines from the "not placement leader" reason,
// ever — including from config followers, which the code comment inside
// sweep() implies should hit it periodically. This test proves why: sweep()
// is invoked ONLY from the targets loop in retentionSweepAll, which is
// itself gated on vaultInst.IsLeader() before a runner is even minted (see
// retentionSweepAll's target-building loop). A follower instance never gets
// a retention runner in the first place — so sweep()'s "not placement
// leader" branch is unreachable via the only production call path. That is
// not a bug in the branch's logic; it means the branch is dead code and the
// "should log every 10 minutes on followers" expectation was simply wrong.
func TestRetentionSweepAllNeverInvokesSweepOnFollowerInstance(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	cfg := triggerLessPolicyCfg(vaultID, policyID, "first-vault", "no-op-policy")
	// Give it a real, working trigger too — if sweep() were ever invoked on
	// this follower, it would actually try to destroy the (long-sealed)
	// chunk, which the deleted-count assertion below would catch.
	cfg.RetentionPolicies[0].MaxAge = strPtr("1ns")

	logSink := &syncBuffer{}
	orch := newUnenforceableTestOrch(t, &recordingSink{}, logSink)
	orch.sysLoader = testSystemLoader{cfg: cfg}

	cm := &retentionFakeChunkManager{chunks: []chunk.ChunkMeta{{
		ID:     chunk.ChunkID{},
		Sealed: true,
	}}}
	// IsFollower: true — this node holds a replica but is not the config
	// placement leader for this vault.
	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: cm, Indexes: &retentionFakeIndexManager{}, IsFollower: true}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	orch.retentionSweepAll()

	orch.mu.RLock()
	_, hasRunner := orch.retention[retentionKey(vaultID, "")]
	orch.mu.RUnlock()
	if hasRunner {
		t.Fatal("a follower instance must never get a retention runner")
	}

	cm.mu.Lock()
	deleted := len(cm.deleted)
	cm.mu.Unlock()
	if deleted != 0 {
		t.Fatalf("sweep() must never run against a follower instance; deleted=%d", deleted)
	}

	if strings.Contains(logSink.String(), "not placement leader") {
		t.Error("the not-placement-leader idle note must never fire via retentionSweepAll (no runner is ever minted for a follower)")
	}
}
