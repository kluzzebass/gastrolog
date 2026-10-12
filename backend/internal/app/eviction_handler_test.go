package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Membership ends on the first notification whether or not the removal
// could be written down, ends once, and keeps the leader's reason. A home
// that could not record the removal is said to be one, because its next
// start comes up as a member the cluster refuses.
func TestEvictionHandlerEndsMembershipOnce(t *testing.T) {
	t.Parallel()
	logs := &logBuffer{}
	ctx, leave := context.WithCancelCause(context.Background())
	defer leave(nil)
	handle := makeEvictionHandler(system.NewStoreProxy(sysmem.NewStore()), leave, slog.New(slog.NewTextHandler(logs, nil)))

	handle("removed from cluster by leader")
	handle("a second notification")

	var removed *removedFromCluster
	if !errors.As(context.Cause(ctx), &removed) {
		t.Fatalf("membership ended with %v, want the removal", context.Cause(ctx))
	}
	if removed.removal.Reason != "removed from cluster by leader" || removed.removal.At.IsZero() {
		t.Fatalf("removal %+v, want the first notification's reason and a time", removed.removal)
	}
	if lines := logs.lines("level=ERROR", "could not record this node's removal"); len(lines) != 1 {
		t.Fatalf("logged %d failures to record the removal, want 1:\n%s", len(lines), strings.Join(lines, "\n"))
	}
}
