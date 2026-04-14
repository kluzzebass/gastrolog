package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
)

// stubScheduler implements JobScheduler for testing.
type stubScheduler struct {
	jobs map[string]orchestrator.JobInfo
}

func (s *stubScheduler) GetJob(id string) (orchestrator.JobInfo, bool) {
	info, ok := s.jobs[id]
	return info, ok
}

func (s *stubScheduler) ListJobs() []orchestrator.JobInfo {
	out := make([]orchestrator.JobInfo, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

// stubPeerJobs provides jobs from simulated peer nodes.
type stubPeerJobs struct {
	peers map[string][]*apiv1.Job
}

func (s *stubPeerJobs) GetAll() map[string][]*apiv1.Job {
	return s.peers
}

func TestGetJob_LocalOnly(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{
		"local-1": {ID: "local-1", Name: "compact", Schedule: "0 * * * *"},
	}}
	srv := &JobServer{scheduler: sched, localNodeID: "node-A"}

	resp, err := srv.GetJob(context.Background(), connect.NewRequest(&apiv1.GetJobRequest{Id: []byte("local-1")}))
	if err != nil {
		t.Fatalf("GetJob local: %v", err)
	}
	if string(resp.Msg.Job.Id) != "local-1" {
		t.Errorf("got ID %q, want local-1", resp.Msg.Job.Id)
	}
	if string(resp.Msg.Job.NodeId) != "node-A" {
		t.Errorf("got NodeId %q, want node-A", resp.Msg.Job.NodeId)
	}
}

func TestGetJob_FallbackToPeer(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	peers := &stubPeerJobs{peers: map[string][]*apiv1.Job{
		"node-B": {
			{Id: []byte("peer-1"), Name: "migrate", NodeId: []byte("node-B")},
		},
	}}
	srv := &JobServer{scheduler: sched, localNodeID: "node-A", peerJobs: peers}

	resp, err := srv.GetJob(context.Background(), connect.NewRequest(&apiv1.GetJobRequest{Id: []byte("peer-1")}))
	if err != nil {
		t.Fatalf("GetJob peer fallback: %v", err)
	}
	if string(resp.Msg.Job.Id) != "peer-1" {
		t.Errorf("got ID %q, want peer-1", resp.Msg.Job.Id)
	}
	if string(resp.Msg.Job.NodeId) != "node-B" {
		t.Errorf("got NodeId %q, want node-B", resp.Msg.Job.NodeId)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	peers := &stubPeerJobs{peers: map[string][]*apiv1.Job{}}
	srv := &JobServer{scheduler: sched, localNodeID: "node-A", peerJobs: peers}

	_, err := srv.GetJob(context.Background(), connect.NewRequest(&apiv1.GetJobRequest{Id: []byte("nonexistent")}))
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("got code %v, want NotFound", connect.CodeOf(err))
	}
}

func TestGetJob_NoPeers_NotFound(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	// peerJobs is nil (single-node mode)
	srv := &JobServer{scheduler: sched, localNodeID: "node-A"}

	_, err := srv.GetJob(context.Background(), connect.NewRequest(&apiv1.GetJobRequest{Id: []byte("anything")}))
	if err == nil {
		t.Fatal("expected error for nonexistent job in single-node mode")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("got code %v, want NotFound", connect.CodeOf(err))
	}
}

func TestGetJob_LocalPreferredOverPeer(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{
		"shared-id": {ID: "shared-id", Name: "local-version", Schedule: "once"},
	}}
	peers := &stubPeerJobs{peers: map[string][]*apiv1.Job{
		"node-B": {
			{Id: []byte("shared-id"), Name: "peer-version", NodeId: []byte("node-B")},
		},
	}}
	srv := &JobServer{scheduler: sched, localNodeID: "node-A", peerJobs: peers}

	resp, err := srv.GetJob(context.Background(), connect.NewRequest(&apiv1.GetJobRequest{Id: []byte("shared-id")}))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Local job should take precedence.
	if string(resp.Msg.Job.NodeId) != "node-A" {
		t.Errorf("got NodeId %q, want node-A (local preferred)", resp.Msg.Job.NodeId)
	}
}
