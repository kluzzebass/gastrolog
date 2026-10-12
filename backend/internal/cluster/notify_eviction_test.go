package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

func testServer() *Server {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	return s
}

// A removal is taken in whether or not the server is stopping: a node removed
// during a rolling restart must come back removed, not as a member the
// cluster refuses. The handler has run by the time the RPC answers, which is
// what lets a graceful stop, which waits for in-flight calls, hand it on.
func TestNotifyEviction_RunsTheHandlerBeforeAnswering(t *testing.T) {
	t.Parallel()
	for _, stopping := range []bool{false, true} {
		var got []string
		s := testServer()
		s.SetEvictionHandler(func(reason string) { got = append(got, reason) })
		if stopping {
			s.stopCancel()
		} else {
			defer s.stopCancel()
		}
		if _, err := s.notifyEviction(context.Background(), &gastrologv1.NotifyEvictionRequest{Reason: "removed"}); err != nil {
			t.Fatalf("notifyEviction (stopping=%v): %v", stopping, err)
		}
		if len(got) != 1 || got[0] != "removed" {
			t.Fatalf("stopping=%v: handler saw %v by the time the RPC answered, want [removed]", stopping, got)
		}
	}
}
