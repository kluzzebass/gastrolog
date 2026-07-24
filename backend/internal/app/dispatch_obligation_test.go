package app

import (
	"errors"
	"testing"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// gastrolog-1ovdy: the config dispatcher no longer swallows the errors of the
// orchestrator side effects it fires from inside FSM.Apply. A failed side
// effect becomes a standing per-entity reconcile obligation plus a
// config-side-effect-failed alarm; the next successful dispatch of the same
// entity clears both. These tests exercise that seam directly.

// newAlertingDispatcher wires a test dispatcher to a real alarm collector so
// obligation transitions can be observed as standing alarms.
func newAlertingDispatcher(orch orchActions, store system.Store, h *captureHandler, c *alert.Collector) *configDispatcher {
	d := newTestDispatcher(orch, store, h)
	d.alerts = c
	return d
}

// standingConfigAlarm returns the config-side-effect-failed alarm standing on
// the collector for the given instance key, or nil.
func standingConfigAlarm(c *alert.Collector, instanceKey string) *alert.Alarm {
	for _, a := range c.Standing() {
		if a.TypeID == configSideEffectAlarmType && a.InstanceKey == instanceKey {
			return a
		}
	}
	return nil
}

// A side effect that fails records a per-entity obligation and raises the
// standing alarm; the node keeps serving (a later, unrelated dispatch still
// processes normally and lands its own independent obligation).
func TestObligation_FaultRecordsObligationAndAlarm(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	h := &captureHandler{}
	c := alert.New()
	mo := &mockOrch{addVaultErr: errors.New("factory boom")}
	d := newAlertingDispatcher(mo, &stubCfgStore{
		vault: &system.VaultConfig{ID: vaultID, Enabled: true},
	}, h, c)

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID})

	// Obligation recorded for the vault entity.
	key := obligationKey(entVault, vaultID.String())
	d.mu.Lock()
	ob, ok := d.obligations[key]
	d.mu.Unlock()
	if !ok {
		t.Fatalf("expected a reconcile obligation for %q", key)
	}
	if ob.entityType != entVault || ob.entityID != vaultID.String() || ob.op != "vault-put" || ob.err == nil {
		t.Fatalf("obligation missing fields: %+v", ob)
	}

	// Standing alarm raised, node-scoped source, detail names the entity.
	a := standingConfigAlarm(c, key)
	if a == nil {
		t.Fatal("expected a standing config-side-effect-failed alarm")
	}
	if a.Source != "config-dispatch" {
		t.Fatalf("unexpected alarm source %q", a.Source)
	}
	if a.Priority != alert.High {
		t.Fatalf("expected High priority, got %v", a.Priority)
	}

	// Node keeps serving: an unrelated dispatch still processes and lands its
	// own independent obligation without disturbing the first.
	mo.reconcileErr = errors.New("reconcile boom")
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: glid.New()})
	if d.obligationCount() != 2 {
		t.Fatalf("expected the vault and ingester obligations to coexist, got %d", d.obligationCount())
	}
	if standingConfigAlarm(c, obligationKey(entIngester, "")) == nil {
		t.Fatal("expected an independent alarm for the ingester obligation")
	}
	// The original vault obligation and alarm are untouched.
	if standingConfigAlarm(c, key) == nil {
		t.Fatal("vault alarm should still stand after an unrelated failure")
	}
}

// The next successful dispatch of the same entity clears both the obligation
// and the standing alarm.
func TestObligation_SuccessfulRetryClears(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	h := &captureHandler{}
	c := alert.New()
	mo := &mockOrch{addVaultErr: errors.New("factory boom")}
	d := newAlertingDispatcher(mo, &stubCfgStore{
		vault: &system.VaultConfig{ID: vaultID, Enabled: true},
	}, h, c)

	// First dispatch fails.
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID})
	key := obligationKey(entVault, vaultID.String())
	if standingConfigAlarm(c, key) == nil || d.obligationCount() != 1 {
		t.Fatal("precondition: expected a standing obligation + alarm after the failed dispatch")
	}

	// The fault clears; the next dispatch of the same vault succeeds.
	mo.addVaultErr = nil
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID})

	if d.obligationCount() != 0 {
		t.Fatalf("expected the obligation cleared on successful retry, got %d", d.obligationCount())
	}
	if standingConfigAlarm(c, key) != nil {
		t.Fatal("expected the standing alarm cleared on successful retry")
	}
	if !h.hasMessage("reconcile obligation cleared") {
		t.Fatal("expected a recovery log line on the clear edge")
	}
}

// A restart heals divergence via startup reconcile: a fresh dispatcher starts
// with no obligations, and ReplayConfigFromStore re-applies the config. With
// the fault gone the replay records nothing and raises no alarm.
func TestObligation_RestartReplayClearsDivergence(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	h := &captureHandler{}
	c := alert.New()
	// Fresh process: empty orchestrator, healthy store, no injected fault.
	mo := &mockOrch{}
	store := &stubCfgStore{
		vaultList: []system.VaultConfig{{ID: vaultID, Enabled: true}},
		vault:     &system.VaultConfig{ID: vaultID, Enabled: true},
	}
	d := newAlertingDispatcher(mo, store, h, c)

	d.ReplayConfigFromStore(t.Context())

	if d.obligationCount() != 0 {
		t.Fatalf("startup reconcile should record no obligations when the fault is gone, got %d", d.obligationCount())
	}
	if len(c.Standing()) != 0 {
		t.Fatalf("startup reconcile should raise no alarms, got %d standing", len(c.Standing()))
	}
	// The vault was actually (re)registered by the replay.
	if len(mo.addVaultCalls) != 1 || mo.addVaultCalls[0] != vaultID {
		t.Fatalf("expected replay to register the vault, got %v", mo.addVaultCalls)
	}
}

// A restart while the underlying fault persists re-detects the divergence: the
// replay records the obligation and raises the alarm again (alarms do not
// persist across restart, so this is a fresh standing occurrence).
func TestObligation_RestartReplayReRaisesWhenFaultPersists(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	h := &captureHandler{}
	c := alert.New()
	mo := &mockOrch{addVaultErr: errors.New("still broken")}
	store := &stubCfgStore{
		vaultList: []system.VaultConfig{{ID: vaultID, Enabled: true}},
		vault:     &system.VaultConfig{ID: vaultID, Enabled: true},
	}
	d := newAlertingDispatcher(mo, store, h, c)

	d.ReplayConfigFromStore(t.Context())

	key := obligationKey(entVault, vaultID.String())
	if standingConfigAlarm(c, key) == nil {
		t.Fatal("expected the divergence re-detected on replay to raise the alarm")
	}
	if d.obligationCount() != 1 {
		t.Fatalf("expected 1 obligation after replay with the fault present, got %d", d.obligationCount())
	}
}

// Multi-node: a side-effect failure is node-scoped. The same committed
// mutation is dispatched to two nodes (independent dispatchers, each with its
// own orchestrator and alarm collector). Only the node whose orchestrator
// fails records the obligation and raises the alarm; the healthy node is
// unaffected. This is exactly the per-node isolation the in-memory obligation
// map and node-local alert.Collector provide.
func TestObligation_FailureIsNodeScoped(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	vaultCfg := &system.VaultConfig{ID: vaultID, Enabled: true}

	// Node A: orchestrator rejects the add.
	hA := &captureHandler{}
	cA := alert.New()
	dA := newAlertingDispatcher(&mockOrch{addVaultErr: errors.New("disk full on A")},
		&stubCfgStore{vault: vaultCfg}, hA, cA)
	dA.localNodeID = "node-a"

	// Node B: orchestrator applies it cleanly.
	hB := &captureHandler{}
	cB := alert.New()
	dB := newAlertingDispatcher(&mockOrch{}, &stubCfgStore{vault: vaultCfg}, hB, cB)
	dB.localNodeID = "node-b"

	note := raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID}
	dA.Handle(note)
	dB.Handle(note)

	key := obligationKey(entVault, vaultID.String())

	// Node A: obligation + alarm standing.
	if dA.obligationCount() != 1 {
		t.Fatalf("node A should carry 1 obligation, got %d", dA.obligationCount())
	}
	if standingConfigAlarm(cA, key) == nil {
		t.Fatal("node A should have the standing alarm")
	}

	// Node B: nothing — the failure did not leak across nodes.
	if dB.obligationCount() != 0 {
		t.Fatalf("node B should carry no obligation, got %d", dB.obligationCount())
	}
	if len(cB.Standing()) != 0 {
		t.Fatalf("node B should have no standing alarms, got %d", len(cB.Standing()))
	}
}

// A nil alert sink (single-node / pre-wiring) must not panic: obligations are
// still tracked, alarms are simply not raised.
func TestObligation_NilAlertSinkStillTracks(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	h := &captureHandler{}
	mo := &mockOrch{addVaultErr: errors.New("boom")}
	d := newTestDispatcher(mo, &stubCfgStore{
		vault: &system.VaultConfig{ID: vaultID, Enabled: true},
	}, h) // alerts left nil

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID})

	if d.obligationCount() != 1 {
		t.Fatalf("obligation must be tracked even with a nil alert sink, got %d", d.obligationCount())
	}

	// Recovery still clears the obligation without an alert sink.
	mo.addVaultErr = nil
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultID})
	if d.obligationCount() != 0 {
		t.Fatalf("obligation must clear on retry without an alert sink, got %d", d.obligationCount())
	}
}
