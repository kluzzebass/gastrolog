package raftfsm

// The removal record is what keeps a removed node out after it has left the
// configuration. It is worth nothing if it does not outlive the process that
// wrote it: a node restarts from its latest snapshot plus the log after it,
// and a follower that falls behind is caught up by a snapshot alone.

import (
	"bytes"
	"context"
	"io"
	"testing"

	"gastrolog/internal/system/command"
)

func TestApplyMarkNodeRemoved(t *testing.T) {
	t.Parallel()
	fsm := New()
	applyCmd(t, fsm, command.NewMarkNodeRemoved("node-gone"))

	ctx := context.Background()
	if removed, err := fsm.Store().IsNodeRemoved(ctx, "node-gone"); err != nil || !removed {
		t.Fatalf("IsNodeRemoved(node-gone) = %v, %v; want true", removed, err)
	}
	if removed, err := fsm.Store().IsNodeRemoved(ctx, "node-other"); err != nil || removed {
		t.Fatalf("IsNodeRemoved(node-other) = %v, %v; a record for one node covered another", removed, err)
	}

	// Removal is recorded once whatever the number of attempts that
	// carried it, so a retried removal is not an error.
	applyCmd(t, fsm, command.NewMarkNodeRemoved("node-gone"))
}

// The record is often the only thing a cluster that has removed a node and
// nothing else holds, so it must survive a snapshot on its own rather than
// riding along with other state.
func TestSnapshotRestore_RemovedNodesSurvive(t *testing.T) {
	t.Parallel()
	fsm1 := New()
	applyCmd(t, fsm1, command.NewMarkNodeRemoved("node-a"))
	applyCmd(t, fsm1, command.NewMarkNodeRemoved("node-b"))

	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	ctx := context.Background()
	for _, id := range []string{"node-a", "node-b"} {
		if removed, err := fsm2.Store().IsNodeRemoved(ctx, id); err != nil || !removed {
			t.Fatalf("after restore IsNodeRemoved(%s) = %v, %v; the removed node would be admitted again", id, removed, err)
		}
	}
	sys, err := fsm2.Store().Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sys == nil || len(sys.Runtime.RemovedNodes) != 2 {
		t.Fatalf("restored runtime carries removed nodes %v, want [node-a node-b]", sys)
	}
}
