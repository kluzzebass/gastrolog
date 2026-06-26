package raftgroup

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

type stubRaft struct {
	term  uint64
	state hraft.RaftState
}

func (s stubRaft) CurrentTerm() uint64    { return s.term }
func (s stubRaft) State() hraft.RaftState { return s.state }

func TestLogRaftObservation_includesGroupAttr(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})).
		With("group", ClusterControlPlaneGroupID)
	r := stubRaft{term: 3, state: hraft.Follower}

	logRaftObservation(logger, r, hraft.Observation{
		Data: hraft.LeaderObservation{},
	}, 500*time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "group=cluster-ctl") {
		t.Fatalf("expected group attr, got: %s", out)
	}
	if !strings.Contains(out, "raft lost leader") {
		t.Fatalf("expected raft lost leader message, got: %s", out)
	}
	if strings.Contains(out, "cluster lost leader") {
		t.Fatalf("old message text must not appear, got: %s", out)
	}
}
