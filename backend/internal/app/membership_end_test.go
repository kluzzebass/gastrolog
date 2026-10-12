package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"gastrolog/internal/glid"
)

// Membership ends on the first word of removal, once, and keeps that word's
// reason: a node that hears it from the leader and from refusing members
// alike records one removal.
func TestMembershipEndLatchesTheFirstRemoval(t *testing.T) {
	t.Parallel()
	ctx, leave := context.WithCancelCause(context.Background())
	defer leave(nil)
	end := newMembershipEnd(leave)

	end.removed("removed from cluster by leader")
	end.removed("refused as removed by cluster member x")

	if !errors.Is(context.Cause(ctx), errRemovedFromCluster) {
		t.Fatalf("membership ended with %v, want the removal", context.Cause(ctx))
	}
	r := end.removal.Load()
	if r == nil || r.Reason != "removed from cluster by leader" || r.At.IsZero() {
		t.Fatalf("removal %+v, want the first reason and a time", r)
	}
}

// A removal that arrives while the node is stopping anyway — a rolling
// restart racing the operator's removal — is still recorded, so the node
// comes back removed rather than as a member the cluster refuses. It does
// not keep the stopping process up to serve it.
func TestFinishMembershipRecordsARemovalLearnedWhileStopping(t *testing.T) {
	t.Parallel()
	hd := newTestHome(t)
	ident, err := resolveNodeID(hd, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	stopping, stop := context.WithCancel(context.Background())
	_, leave := context.WithCancelCause(stopping)
	defer leave(nil)
	end := newMembershipEnd(leave)
	stop()
	end.removed("removed from cluster by leader")

	if err := finishMembership(stopping, discardLogger(), RunConfig{}, hd, ident.ID, end, nil); err != nil {
		t.Fatalf("finishMembership while stopping: %v", err)
	}
	got, err := resolveNodeID(hd, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != ident.ID || got.Removal == nil || got.Removal.Reason != "removed from cluster by leader" {
		t.Fatalf("home after a removal learned while stopping: %+v, want %s removed", got, ident.ID)
	}
}

// A home whose identity store cannot take the record says so: its next
// start relearns the removal from the members that refuse it.
func TestFinishMembershipReportsARemovalItCannotRecord(t *testing.T) {
	t.Parallel()
	logs := &logBuffer{}
	stopping, stop := context.WithCancel(context.Background())
	stop()
	_, leave := context.WithCancelCause(stopping)
	end := newMembershipEnd(leave)
	end.removed("removed from cluster by leader")

	hd := newTestHome(t) // never resolved: no cluster-ctl WAL to write to
	if err := finishMembership(stopping, slog.New(slog.NewTextHandler(logs, nil)), RunConfig{}, hd, glid.New(), end, nil); err != nil {
		t.Fatal(err)
	}
	if lines := logs.lines("level=ERROR", "could not record this node's removal"); len(lines) != 1 {
		t.Fatalf("logged %d failures to record the removal, want 1:\n%s", len(lines), strings.Join(lines, "\n"))
	}
}
