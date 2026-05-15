package server_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// YieldLeadership is the preStop-time leadership handoff that replaces
// demote-self. Three behaviors to pin:
//
//   1. Leader → calls LeadershipTransfer, returns transferred=true.
//   2. Follower → no-op, returns transferred=false (preStop must NOT
//      error on follower pods, otherwise rolling restart blocks every
//      non-leader pod).
//   3. Single-node / no cluster → no-op, no error.
//
// See gastrolog-2yeie.

func yieldClient(t *testing.T, mc server.ClusterStatusProvider) gastrologv1connect.LifecycleServiceClient {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{
		Cluster: mc,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")
}

func TestYieldLeadership_LeaderTransfers(t *testing.T) {
	mc := &mockCluster{isLeader: true}
	client := yieldClient(t, mc)

	resp, err := client.YieldLeadership(context.Background(), connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
	if err != nil {
		t.Fatalf("YieldLeadership: %v", err)
	}
	if !resp.Msg.Transferred {
		t.Error("expected transferred=true on leader")
	}
	if mc.transferCalls != 1 {
		t.Errorf("LeadershipTransfer calls = %d, want 1", mc.transferCalls)
	}
}

func TestYieldLeadership_FollowerNoops(t *testing.T) {
	mc := &mockCluster{isLeader: false}
	client := yieldClient(t, mc)

	resp, err := client.YieldLeadership(context.Background(), connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
	if err != nil {
		t.Fatalf("YieldLeadership on follower must not error: %v", err)
	}
	if resp.Msg.Transferred {
		t.Error("expected transferred=false on follower")
	}
	if mc.transferCalls != 0 {
		t.Errorf("LeadershipTransfer must not fire on follower, got %d calls", mc.transferCalls)
	}
}

func TestYieldLeadership_NoClusterNoops(t *testing.T) {
	client := yieldClient(t, nil)

	resp, err := client.YieldLeadership(context.Background(), connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
	if err != nil {
		t.Fatalf("YieldLeadership in single-node must not error: %v", err)
	}
	if resp.Msg.Transferred {
		t.Error("expected transferred=false with no cluster")
	}
}

func TestYieldLeadership_LeaderTransferErrorPropagates(t *testing.T) {
	mc := &mockCluster{isLeader: true, transferErr: errors.New("raft: leadership transfer failed")}
	client := yieldClient(t, mc)

	_, err := client.YieldLeadership(context.Background(), connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
	if err == nil {
		t.Fatal("expected error when LeadershipTransfer fails")
	}
}
