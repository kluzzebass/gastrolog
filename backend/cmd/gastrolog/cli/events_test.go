package cli

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func TestParseEventTimeFlag(t *testing.T) {
	if ts, err := parseEventTimeFlag("--since", ""); err != nil || ts != nil {
		t.Fatalf("empty flag: got %v, %v; want nil, nil", ts, err)
	}
	before := time.Now()
	ts, err := parseEventTimeFlag("--since", "2h")
	if err != nil {
		t.Fatalf("duration form: %v", err)
	}
	got := ts.AsTime()
	want := before.Add(-2 * time.Hour)
	if got.Before(want.Add(-time.Minute)) || got.After(want.Add(time.Minute)) {
		t.Fatalf("duration form resolved to %v, want ~%v", got, want)
	}
	ts, err = parseEventTimeFlag("--until", "2026-07-01T08:00:00Z")
	if err != nil {
		t.Fatalf("RFC3339 form: %v", err)
	}
	if !ts.AsTime().Equal(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("RFC3339 form resolved to %v", ts.AsTime())
	}
	if _, err := parseEventTimeFlag("--since", "yesterday-ish"); err == nil {
		t.Fatal("garbage accepted")
	}
	if _, err := parseEventTimeFlag("--since", "-2h"); err == nil {
		t.Fatal("negative duration accepted")
	}
}

func mkEvent(node, name, typ, source, detail string) *v1.SystemEvent {
	return &v1.SystemEvent{
		NodeId:   []byte(node),
		NodeName: name,
		Time:     timestamppb.New(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)),
		Type:     typ,
		Source:   source,
		Detail:   detail,
	}
}

func TestFilterEventsByNode(t *testing.T) {
	events := []*v1.SystemEvent{
		mkEvent("n1", "alpha", "node-started", "node", "journal begins"),
		mkEvent("n2", "bravo", "node-started", "node", "journal begins"),
	}
	if got := filterEventsByNode(events, "ALPHA"); len(got) != 1 || string(got[0].NodeId) != "n1" {
		t.Fatalf("name match failed: %+v", got)
	}
	if got := filterEventsByNode(events, "n2"); len(got) != 1 || got[0].NodeName != "bravo" {
		t.Fatalf("ID match failed: %+v", got)
	}
	if got := filterEventsByNode(events, "charlie"); len(got) != 0 {
		t.Fatalf("unknown node matched: %+v", got)
	}
}

func TestEventDetailRendering(t *testing.T) {
	e := mkEvent("n1", "alpha", "alarm-acked", "storage", "acknowledged")
	e.AlarmId = []byte("disk-space-exhausted:vault1")
	e.By = "op"
	got := eventDetail(e)
	want := "disk-space-exhausted:vault1: acknowledged (by op)"
	if got != want {
		t.Fatalf("eventDetail = %q, want %q", got, want)
	}
	plain := mkEvent("n1", "alpha", "election-storm", "raft", "engaged")
	if got := eventDetail(plain); got != "engaged" {
		t.Fatalf("plain eventDetail = %q", got)
	}
}

func TestEventsToJSONCarriesUnreachable(t *testing.T) {
	out := eventsToJSON([]*v1.SystemEvent{mkEvent("n1", "alpha", "node-started", "node", "x")}, []string{"n9"})
	if len(out.Events) != 1 || out.Events[0].Node != "alpha" {
		t.Fatalf("events json wrong: %+v", out)
	}
	if len(out.UnreachableNodes) != 1 || out.UnreachableNodes[0] != "n9" {
		t.Fatalf("unreachable list lost: %+v", out.UnreachableNodes)
	}
}
