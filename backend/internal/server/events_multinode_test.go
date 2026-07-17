package server_test

// Multi-node coverage for the event journal (gastrolog-1m3e0d): journals
// are per-node in-memory rings, and ListEvents on ANY node merges its own
// ring with a local_only ForwardRPC leg to every peer — the same fan-out
// shape as the ack/shelve RPCs. These tests drive that path end to end
// through the harness coordinator: transitions on a non-coordinator node's
// collector must be readable from the coordinator's RPC surface, filters
// must apply on every leg, and an unreachable node must be NAMED rather
// than silently presented as quiet history.

import (
	"context"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// listEvents fetches the merged journal from the coordinator.
func listEvents(t *testing.T, h *multiNodeHarness, req *gastrologv1.ListEventsRequest) *gastrologv1.ListEventsResponse {
	t.Helper()
	resp, err := h.lifecycleClient.ListEvents(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	return resp.Msg
}

// eventsFromNode filters a merged response down to one node's entries.
func eventsFromNode(resp *gastrologv1.ListEventsResponse, nodeID string) []*gastrologv1.SystemEvent {
	var out []*gastrologv1.SystemEvent
	for _, e := range resp.Events {
		if string(e.NodeId) == nodeID {
			out = append(out, e)
		}
	}
	return out
}

// TestMultiNodeEvents_PeerTransitionsVisibleFromCoordinator drives a full
// lifecycle on a NON-coordinator node and reads the audit trail from the
// coordinator — the journal entries for raise, ack and clear must arrive
// attributed to the raising node, in order, exactly one per transition.
func TestMultiNodeEvents_PeerTransitionsVisibleFromCoordinator(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-2"].Raise("disk-space-exhausted", "vault1", "disk protect engaged")
	if err := h.alerts["data-2"].Ack("disk-space-exhausted:vault1", "op"); err != nil {
		t.Fatalf("Ack on data-2: %v", err)
	}
	h.alerts["data-2"].Clear("disk-space-exhausted", "vault1")

	resp := listEvents(t, h, &gastrologv1.ListEventsRequest{})
	if len(resp.UnreachableNodes) != 0 {
		t.Fatalf("unreachable nodes reported on a healthy cluster: %v", resp.UnreachableNodes)
	}

	// Every node contributes at least its node-started seed — the restart
	// decision made visible: history begins at boot, per node.
	for _, id := range []string{"coord", "data-1", "data-2", "data-3"} {
		evs := eventsFromNode(resp, id)
		if len(evs) == 0 || evs[0].Type != alert.EventNodeStarted {
			t.Fatalf("node %s journal does not begin with node-started: %+v", id, evs)
		}
	}

	// data-2 carries the lifecycle trail: seed, raised, acked, cleared.
	evs := eventsFromNode(resp, "data-2")
	wantTypes := []string{
		alert.EventNodeStarted,
		alert.EventAlarmRaised,
		alert.EventAlarmAcked,
		alert.EventAlarmCleared,
	}
	if len(evs) != len(wantTypes) {
		t.Fatalf("data-2 has %d entries, want %d: %+v", len(evs), len(wantTypes), evs)
	}
	for i, w := range wantTypes {
		if evs[i].Type != w {
			t.Fatalf("data-2 entry %d type = %s, want %s", i, evs[i].Type, w)
		}
	}
	raised := evs[1]
	if string(raised.AlarmId) != "disk-space-exhausted:vault1" || raised.Source != "storage" ||
		raised.Detail != "disk protect engaged" {
		t.Fatalf("raised entry fields wrong: %+v", raised)
	}
	if evs[2].By != "op" {
		t.Fatalf("acked entry missing operator identity: %+v", evs[2])
	}
	// Quiet nodes carry ONLY their seed — no attribution leak.
	for _, id := range []string{"coord", "data-1", "data-3"} {
		if n := len(eventsFromNode(resp, id)); n != 1 {
			t.Fatalf("node %s has %d entries, want 1 (seed only) — attribution leaked", id, n)
		}
	}
}

// TestMultiNodeEvents_FiltersApplyAcrossNodes exercises type, source and
// time-range filters against a mixed multi-node history on a shared
// deterministic clock.
func TestMultiNodeEvents_FiltersApplyAcrossNodes(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithAlertClock(clock))
	boot := clock()
	advance(time.Minute) // separate the seeds from the first transition

	h.alerts["data-1"].Raise("disk-space-exhausted", "vault1", "disk protect engaged on data-1")
	advance(time.Minute)
	cutoff := clock()
	// vault-underreplicated: zero-delay type whose catalog source
	// ("placement") differs from disk-space-exhausted's ("storage").
	h.alerts["data-3"].Raise("vault-underreplicated", "vault1", "one placement of two")
	h.alerts["coord"].Raise("disk-space-exhausted", "vault2", "disk protect engaged on coord")

	// Type filter: only alarm-raised entries, from every raising node.
	byType := listEvents(t, h, &gastrologv1.ListEventsRequest{Type: alert.EventAlarmRaised})
	if len(byType.Events) != 3 {
		t.Fatalf("type filter returned %d events, want 3: %+v", len(byType.Events), byType.Events)
	}
	for _, e := range byType.Events {
		if e.Type != alert.EventAlarmRaised {
			t.Fatalf("type filter leaked %s", e.Type)
		}
	}

	// Source filter: only the storage-sourced raises survive; data-3's
	// placement-sourced entry is excluded.
	bySource := listEvents(t, h, &gastrologv1.ListEventsRequest{Source: "storage"})
	for _, e := range bySource.Events {
		if e.Source != "storage" {
			t.Fatalf("source filter leaked %s", e.Source)
		}
	}
	if n := len(bySource.Events); n != 2 {
		t.Fatalf("source=storage returned %d events, want 2 (data-1 + coord raises)", n)
	}

	// Time range: since=cutoff excludes the seeds and data-1's raise.
	sinceResp := listEvents(t, h, &gastrologv1.ListEventsRequest{Since: timestamppb.New(cutoff)})
	if n := len(sinceResp.Events); n != 2 {
		t.Fatalf("since filter returned %d events, want 2: %+v", n, sinceResp.Events)
	}
	// until=boot keeps only the seeds.
	untilResp := listEvents(t, h, &gastrologv1.ListEventsRequest{Until: timestamppb.New(boot)})
	for _, e := range untilResp.Events {
		if e.Type != alert.EventNodeStarted {
			t.Fatalf("until filter leaked %s", e.Type)
		}
	}
	if n := len(untilResp.Events); n != 4 {
		t.Fatalf("until=boot returned %d events, want the 4 seeds", n)
	}

	// Merged order is chronological across nodes.
	all := listEvents(t, h, &gastrologv1.ListEventsRequest{})
	for i := 1; i < len(all.Events); i++ {
		if all.Events[i].Time.AsTime().Before(all.Events[i-1].Time.AsTime()) {
			t.Fatalf("merged events out of order at %d: %v after %v",
				i, all.Events[i].Time.AsTime(), all.Events[i-1].Time.AsTime())
		}
	}
}

// TestMultiNodeEvents_LimitKeepsNewest floods one node's journal and
// asserts the merged limit keeps the newest entries.
func TestMultiNodeEvents_LimitKeepsNewest(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	// 20 distinct alarm occurrences on data-1: raise, ack (releases on
	// clear), clear — a growing journal with a known tail.
	for range 20 {
		h.alerts["data-1"].Raise("wal-reserve", "cluster-ctl", "reservation below floor")
		if err := h.alerts["data-1"].Ack("wal-reserve:cluster-ctl", "op"); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		h.alerts["data-1"].Clear("wal-reserve", "cluster-ctl")
	}

	resp := listEvents(t, h, &gastrologv1.ListEventsRequest{Limit: 5})
	if len(resp.Events) != 5 {
		t.Fatalf("limit=5 returned %d events", len(resp.Events))
	}
	// The newest survive: the last entry is data-1's final alarm-cleared.
	last := resp.Events[len(resp.Events)-1]
	if string(last.NodeId) != "data-1" || last.Type != alert.EventAlarmCleared {
		t.Fatalf("limit did not keep the newest entries; last = %+v", last)
	}
}

// TestMultiNodeEvents_UnreachableNodeIsNamed drops one node's forward
// handler and asserts the response NAMES it instead of silently serving a
// partial view as if it were whole.
func TestMultiNodeEvents_UnreachableNodeIsNamed(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.routingFwd.dropNode("data-2")

	resp := listEvents(t, h, &gastrologv1.ListEventsRequest{})
	if len(resp.UnreachableNodes) != 1 || resp.UnreachableNodes[0] != "data-2" {
		t.Fatalf("unreachable_nodes = %v, want [data-2]", resp.UnreachableNodes)
	}
	if n := len(eventsFromNode(resp, "data-2")); n != 0 {
		t.Fatalf("events attributed to the unreachable node: %d", n)
	}
	// The reachable nodes still answer.
	for _, id := range []string{"coord", "data-1", "data-3"} {
		if n := len(eventsFromNode(resp, id)); n != 1 {
			t.Fatalf("node %s has %d entries, want its seed", id, n)
		}
	}
}

// TestMultiNodeEvents_ShelveTrailCrossNode shelves via the coordinator's
// fan-out RPC an alarm raised on a peer, then unshelves — the journal
// entries must land on the RAISING node's ring (where the transition
// happened), visible again through the merge.
func TestMultiNodeEvents_ShelveTrailCrossNode(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithClusterStats())

	h.alerts["data-3"].Raise("disk-space-exhausted", "vault1", "disk protect engaged")

	if _, err := h.lifecycleClient.ShelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.ShelveAlarmRequest{
		AlarmId:         []byte("disk-space-exhausted:vault1"),
		DurationSeconds: 3600,
		ShelvedBy:       "op",
	})); err != nil {
		t.Fatalf("ShelveAlarm via coordinator: %v", err)
	}
	if _, err := h.lifecycleClient.UnshelveAlarm(context.Background(), connect.NewRequest(&gastrologv1.UnshelveAlarmRequest{
		AlarmId: []byte("disk-space-exhausted:vault1"),
	})); err != nil {
		t.Fatalf("UnshelveAlarm via coordinator: %v", err)
	}

	resp := listEvents(t, h, &gastrologv1.ListEventsRequest{})
	evs := eventsFromNode(resp, "data-3")
	wantTypes := []string{
		alert.EventNodeStarted,
		alert.EventAlarmRaised,
		alert.EventAlarmShelved,
		alert.EventAlarmUnshelved,
	}
	if len(evs) != len(wantTypes) {
		t.Fatalf("data-3 has %d entries, want %d: %+v", len(evs), len(wantTypes), evs)
	}
	for i, w := range wantTypes {
		if evs[i].Type != w {
			t.Fatalf("data-3 entry %d type = %s, want %s", i, evs[i].Type, w)
		}
	}
	if evs[2].By != "op" {
		t.Fatalf("cross-node shelve entry lost operator identity: %+v", evs[2])
	}
}
