package server_test

// Multi-node coverage for the alarm shelving RPCs: ShelveAlarm /
// UnshelveAlarm served from ANY node, fanned out to every raiser of the
// alarm ID, and applied in each raiser's collector. The harness coordinator
// serves the RPCs; forwarded local_only legs run through the same
// BuildInternalHandler dispatch production ForwardRPC uses. Alarm state is
// in-memory only — the restart test asserts NOTHING survives (no shelve,
// no operator state; a re-detected condition is simply active again).
// Alarm clocks are injected wherever a test touches a time construct —
// zero sleeps.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"

	"connectrpc.com/connect"
)

// alertStateOn fetches the aggregated cluster status and returns the state
// of the given alarm ID as attributed to nodeID, or ok=false if the node
// does not carry it.
func alertStateOn(t *testing.T, h *multiNodeHarness, nodeID, alarmID string) (gastrologv1.AlarmState, bool) {
	t.Helper()
	for _, a := range alertsByNode(t, h)[nodeID] {
		if string(a.Id) == alarmID {
			return a.State, true
		}
	}
	return gastrologv1.AlarmState_ALARM_STATE_UNSPECIFIED, false
}

// TestAlarmLifecycle_ShelveFromNonOwningNode: the operator's shell points
// at the coordinator; the alarm stands on data-2. The shelve RPC forwards
// to the owner and applies in ITS collector.
func TestAlarmLifecycle_ShelveFromNonOwningNode(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-2"].Raise("disk-space-exhausted", "vault1", "disk protect engaged")

	resp, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-exhausted:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "alice",
	}))
	if err != nil {
		t.Fatalf("ShelveAlarm: %v", err)
	}
	if resp.Msg.Applied != 1 || resp.Msg.ShelvedUntil == nil {
		t.Fatalf("shelve response = %+v, want applied=1 with expiry", resp.Msg)
	}

	// The owner's collector holds the shelve — checked directly and
	// through the aggregation surface every UI reads.
	var shelved *alert.Alarm
	for _, a := range h.alerts["data-2"].Standing() {
		if a.ID == "disk-space-exhausted:vault1" {
			shelved = a
		}
	}
	if shelved == nil || shelved.State != alert.StateShelved || shelved.ShelvedUntil.IsZero() {
		t.Fatalf("owner collector state = %+v, want shelved with expiry", shelved)
	}
	if st, ok := alertStateOn(t, h, "data-2", "disk-space-exhausted:vault1"); !ok ||
		st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
		t.Fatalf("aggregated state = %v (present=%v), want SHELVED", st, ok)
	}
}

// TestAlarmLifecycle_ShelveFansOutToAllRaisers: a cluster-wide condition
// (same alarm ID raised by three nodes, coordinator included) shelves
// EVERYWHERE in one call.
func TestAlarmLifecycle_ShelveFansOutToAllRaisers(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	for _, n := range []string{"coord", "data-1", "data-3"} {
		h.alerts[n].Raise("vault-underreplicated", "vault1", "RF unmet on "+n)
	}

	resp, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("vault-underreplicated:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "alice",
	}))
	if err != nil {
		t.Fatalf("ShelveAlarm: %v", err)
	}
	if resp.Msg.Applied != 3 {
		t.Fatalf("applied = %d, want 3 (every raiser, local + remote)", resp.Msg.Applied)
	}

	// Re-aggregate: every raiser reports the alarm shelved.
	byNode := alertsByNode(t, h)
	for _, n := range []string{"coord", "data-1", "data-3"} {
		st, ok := alertStateOn(t, h, n, "vault-underreplicated:vault1")
		if !ok || st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
			t.Fatalf("node %s aggregated state = %v (present=%v), want SHELVED — the alarm would stay loud", n, st, ok)
		}
	}
	if n := len(byNode["data-2"]); n != 0 {
		t.Fatalf("data-2 (non-raiser) has %d alarms, want 0", n)
	}
}

// TestAlarmLifecycle_NothingSurvivesRestart: alarm state is in-memory only.
// A second harness is the in-process whole-cluster restart: the re-detected
// condition annunciates plain ACTIVE — the pre-restart shelve is gone, and
// that is the design (loud is safe).
func TestAlarmLifecycle_NothingSurvivesRestart(t *testing.T) {
	h1 := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())
	h1.alerts["data-2"].Raise("chunking-underreplicated", "vault1", "segments below minimum")
	if _, err := h1.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("chunking-underreplicated:vault1"),
		DurationSeconds: 86400,
		ShelvedBy:       "alice",
	})); err != nil {
		t.Fatalf("ShelveAlarm: %v", err)
	}
	if st, _ := alertStateOn(t, h1, "data-2", "chunking-underreplicated:vault1"); st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
		t.Fatalf("pre-restart state = %v, want SHELVED", st)
	}

	// Restart: a fresh cluster. The raiser re-detects the standing
	// condition after boot; nothing is carried over.
	h2 := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())
	h2.alerts["data-2"].Raise("chunking-underreplicated", "vault1", "segments below minimum")

	st, ok := alertStateOn(t, h2, "data-2", "chunking-underreplicated:vault1")
	if !ok || st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("state after restart = %v (present=%v), want plain ACTIVE (nothing persists)", st, ok)
	}
	for _, a := range alertsByNode(t, h2)["data-2"] {
		if string(a.Id) == "chunking-underreplicated:vault1" && a.ShelvedUntil != nil {
			t.Fatalf("shelve resurrected across restart: %+v", a)
		}
	}
}

// TestAlarmLifecycle_ShelveCrossNodeAndUnshelve: shelve from the
// coordinator applies on the remote owner with the mandatory expiry;
// unshelve returns it to ACTIVE.
func TestAlarmLifecycle_ShelveCrossNodeAndUnshelve(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-3"].Raise("disk-space-low", "vault1", "below warn band")

	resp, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-low:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "alice",
	}))
	if err != nil {
		t.Fatalf("ShelveAlarm: %v", err)
	}
	if resp.Msg.Applied != 1 || resp.Msg.ShelvedUntil == nil {
		t.Fatalf("shelve response = %+v, want applied=1 with expiry", resp.Msg)
	}
	if st, ok := alertStateOn(t, h, "data-3", "disk-space-low:vault1"); !ok ||
		st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
		t.Fatalf("state = %v (present=%v), want SHELVED", st, ok)
	}

	if _, err := h.lifecycleClient.UnshelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.UnshelveAlarmRequest{
		AlarmId: []byte("disk-space-low:vault1"),
	})); err != nil {
		t.Fatalf("UnshelveAlarm: %v", err)
	}
	if st, ok := alertStateOn(t, h, "data-3", "disk-space-low:vault1"); !ok ||
		st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("state after unshelve = %v (present=%v), want ACTIVE", st, ok)
	}
}

// TestAlarmLifecycle_ShelveExpiryReturnsToActive: the shelve lapses on the
// shared injected clock while the condition still stands — the alarm
// returns to ACTIVE in the aggregation, demanding fresh attention.
func TestAlarmLifecycle_ShelveExpiryReturnsToActive(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithAlertClock(clock))

	h.alerts["data-1"].Raise("disk-space-low", "vault1", "below warn band")
	if _, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-low:vault1"),
		DurationSeconds: 1800,
	})); err != nil {
		t.Fatalf("ShelveAlarm: %v", err)
	}
	if st, _ := alertStateOn(t, h, "data-1", "disk-space-low:vault1"); st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
		t.Fatalf("state = %v, want SHELVED", st)
	}

	mu.Lock()
	now = now.Add(31 * time.Minute)
	mu.Unlock()
	st, ok := alertStateOn(t, h, "data-1", "disk-space-low:vault1")
	if !ok || st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("state after expiry = %v (present=%v), want ACTIVE — condition still true", st, ok)
	}
}

// TestAlarmLifecycle_ShelveRejectsNoExpiry: a shelve without a positive
// duration is rejected at the API boundary before any fan-out.
func TestAlarmLifecycle_ShelveRejectsNoExpiry(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithClusterStats())
	h.alerts["data-1"].Raise("disk-space-low", "vault1", "below warn band")

	for _, secs := range []int64{0, -60} {
		_, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
			AlarmId:         []byte("disk-space-low:vault1"),
			DurationSeconds: secs,
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("ShelveAlarm(duration=%d) code = %v, want InvalidArgument", secs, connect.CodeOf(err))
		}
	}
	// Untouched by the rejected requests.
	if st, ok := alertStateOn(t, h, "data-1", "disk-space-low:vault1"); !ok ||
		st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("state = %v (present=%v), want ACTIVE", st, ok)
	}
}

// TestAlarmLifecycle_ShelveRefusedForUnshelveableType: orchestrator-lock-leak
// refuses shelve with a reason (software fault — deferral is a lie), and
// the wire carries shelveable=false so the UI never renders the control.
// Its full lifecycle is pinned in internal/alert; here the cross-node
// boundary behavior: rejection happens on the SERVING node before fan-out,
// and the fault simply keeps standing — there is no release path short of
// restarting the wedged node.
func TestAlarmLifecycle_ShelveRefusedForUnshelveableType(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())
	h.alerts["data-2"].Raise("orchestrator-lock-leak", "", "read hold stuck for 2m")

	_, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("orchestrator-lock-leak"),
		DurationSeconds: 3600,
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("ShelveAlarm(lock-leak) code = %v, want FailedPrecondition", connect.CodeOf(err))
	}
	if !strings.Contains(err.Error(), "orchestrator-lock-leak") {
		t.Fatalf("refusal must name the type: %v", err)
	}

	// The wire tells the UI not to render a shelve control at all — and the
	// fault stands ACTIVE regardless.
	for _, a := range alertsByNode(t, h)["data-2"] {
		if string(a.Id) == "orchestrator-lock-leak" {
			if a.Shelveable {
				t.Fatal("lock-leak broadcast as shelveable — UI would render a control that always errors")
			}
			if a.State != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
				t.Fatalf("lock-leak state = %v, want ACTIVE (stands until restart)", a.State)
			}
		}
	}
}

// TestAlarmLifecycle_ShelveUnknownAndReleased: unknown IDs are NotFound
// cluster-wide; an alarm whose condition resolved is released and therefore
// unknown again.
func TestAlarmLifecycle_ShelveUnknownAndReleased(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	_, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("no-such-alarm:anywhere"),
		DurationSeconds: 3600,
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("ShelveAlarm(unknown) code = %v, want NotFound", connect.CodeOf(err))
	}

	// A cleared condition releases its alarm — there is no retained state,
	// and a shelve for it finds nothing.
	h.alerts["data-1"].Raise("disk-space-exhausted", "vault1", "full")
	h.alerts["data-1"].Clear("disk-space-exhausted", "vault1")
	if _, ok := alertStateOn(t, h, "data-1", "disk-space-exhausted:vault1"); ok {
		t.Fatal("cleared alarm must release from the aggregation")
	}
	_, err = h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-exhausted:vault1"),
		DurationSeconds: 3600,
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("ShelveAlarm(released) code = %v, want NotFound", connect.CodeOf(err))
	}
}

// TestAlarmLifecycle_OwnerUnreachableSurfacesError: the raiser is known
// from its last broadcast but the forward fails — the error names the node
// and nothing pretends the shelve applied; the alarm stands active.
func TestAlarmLifecycle_OwnerUnreachableSurfacesError(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-2"].Raise("disk-space-exhausted", "vault1", "disk protect engaged")
	h.routingFwd.dropNode("data-2")

	_, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-exhausted:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "alice",
	}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("ShelveAlarm(unreachable owner) code = %v, want Unavailable", connect.CodeOf(err))
	}
	if !strings.Contains(err.Error(), "data-2") {
		t.Fatalf("error must name the unreachable raiser: %v", err)
	}
	if st, ok := alertStateOn(t, h, "data-2", "disk-space-exhausted:vault1"); !ok ||
		st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("state = %v (present=%v), want ACTIVE — a failed forward must not fake a shelve", st, ok)
	}
}

// TestAlarmLifecycle_PartialFanOutReportsFailure: one raiser reachable, one
// not — the reachable one shelves, the RPC still errors naming the failed
// node, and a retry (idempotent) is the operator's path to convergence.
func TestAlarmLifecycle_PartialFanOutReportsFailure(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-1"].Raise("vault-underreplicated", "vault1", "RF unmet on data-1")
	h.alerts["data-3"].Raise("vault-underreplicated", "vault1", "RF unmet on data-3")
	h.routingFwd.dropNode("data-3")

	_, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("vault-underreplicated:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "alice",
	}))
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "data-3") {
		t.Fatalf("partial fan-out must surface the failed node: %v", err)
	}
	// The reachable raiser applied; the unreachable one still stands active.
	if st, _ := alertStateOn(t, h, "data-1", "vault-underreplicated:vault1"); st != gastrologv1.AlarmState_ALARM_STATE_SHELVED {
		t.Fatalf("data-1 state = %v, want SHELVED (partial application is real)", st)
	}
	if st, _ := alertStateOn(t, h, "data-3", "vault-underreplicated:vault1"); st != gastrologv1.AlarmState_ALARM_STATE_ACTIVE {
		t.Fatalf("data-3 state = %v, want ACTIVE", st)
	}
}
