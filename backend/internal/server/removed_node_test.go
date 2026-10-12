package server_test

// A node the cluster removed keeps serving one thing: the account of its
// removal. Status, health and the preStop hook's question get answers;
// everything else, the UI's first call and every administrative verb
// included, is refused with that account and nothing behind it runs.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

func removedNodeAPI(t *testing.T) (server.RemovedNode, *server.Client, *httptest.Server) {
	t.Helper()
	n := server.RemovedNode{
		NodeID:    "06giv0bgl1pmffimofp5q1pgko",
		RemovedAt: time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC),
		Reason:    "removed from cluster by leader",
	}
	api := httptest.NewServer(server.RemovedNodeHandler(n, "test-version"))
	t.Cleanup(api.Close)
	return n, server.NewClientWithHTTP(api.Client(), api.URL), api
}

func TestRemovedNodeAnswersStatusWithItsRemoval(t *testing.T) {
	t.Parallel()
	n, client, _ := removedNodeAPI(t)
	ctx := context.Background()

	st, err := client.Lifecycle.GetClusterStatus(ctx, connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	got := st.Msg
	if got.Removal == nil || !got.Removal.RemovedAt.AsTime().Equal(n.RemovedAt) || got.Removal.Reason != n.Reason {
		t.Fatalf("removal %+v, want %+v", got.Removal, n)
	}
	if string(got.LocalNodeId) != n.NodeID || !got.ClusterEnabled {
		t.Fatalf("status %+v, want the removed node %s in cluster mode", got, n.NodeID)
	}
	if len(got.Nodes) != 0 || got.LocalStats != nil || len(got.LeaderId) != 0 {
		t.Fatalf("a removed node reports cluster state: %+v", got)
	}

	health, err := client.Lifecycle.Health(ctx, connect.NewRequest(&gastrologv1.HealthRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if health.Msg.Status != gastrologv1.Status_STATUS_UNHEALTHY || health.Msg.Version != "test-version" {
		t.Fatalf("health %+v, want unhealthy at the running version", health.Msg)
	}

	yield, err := client.Lifecycle.YieldLeadership(ctx, connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
	if err != nil || yield.Msg.Transferred {
		t.Fatalf("yield-leadership: %v, %v; want answered with nothing transferred", yield, err)
	}
}

func TestRemovedNodeRefusesEveryOtherCall(t *testing.T) {
	t.Parallel()
	n, client, _ := removedNodeAPI(t)
	ctx := context.Background()

	calls := map[string]func() error{
		"Shutdown": func() error {
			_, err := client.Lifecycle.Shutdown(ctx, connect.NewRequest(&gastrologv1.ShutdownRequest{}))
			return err
		},
		"CreateJoinToken": func() error {
			_, err := client.Lifecycle.CreateJoinToken(ctx, connect.NewRequest(&gastrologv1.CreateJoinTokenRequest{}))
			return err
		},
		"RemoveNode": func() error {
			_, err := client.Lifecycle.RemoveNode(ctx, connect.NewRequest(&gastrologv1.RemoveNodeRequest{NodeId: []byte(n.NodeID), AllowSelf: true}))
			return err
		},
		"JoinCluster": func() error {
			_, err := client.Lifecycle.JoinCluster(ctx, connect.NewRequest(&gastrologv1.JoinClusterRequest{MemberAddress: "elsewhere:4566", JoinToken: "token"}))
			return err
		},
		"SetNodeSuffrage": func() error {
			_, err := client.Lifecycle.SetNodeSuffrage(ctx, connect.NewRequest(&gastrologv1.SetNodeSuffrageRequest{NodeId: []byte(n.NodeID), Voter: true}))
			return err
		},
		"GetSystem": func() error {
			_, err := client.System.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{}))
			return err
		},
		"GetAuthStatus": func() error {
			_, err := client.Auth.GetAuthStatus(ctx, connect.NewRequest(&gastrologv1.GetAuthStatusRequest{}))
			return err
		},
		"WatchSystemStatus": func() error {
			stream, err := client.Lifecycle.WatchSystemStatus(ctx, connect.NewRequest(&gastrologv1.WatchSystemStatusRequest{}))
			if err != nil {
				return err
			}
			defer func() { _ = stream.Close() }()
			for stream.Receive() {
				return nil
			}
			return stream.Err()
		},
	}
	for name, call := range calls {
		err := call()
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), n.Explanation()) {
			t.Errorf("%s on a removed node: %v, want FailedPrecondition with the removal", name, err)
		}
	}
}

func TestRemovedNodeProbesAndPages(t *testing.T) {
	t.Parallel()
	n, _, api := removedNodeAPI(t)
	for path, want := range map[string]int{
		"/healthz": http.StatusOK,
		"/readyz":  http.StatusServiceUnavailable,
		"/":        http.StatusServiceUnavailable,
		"/search":  http.StatusServiceUnavailable,
	} {
		resp, err := api.Client().Get(api.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
		if want == http.StatusServiceUnavailable && !strings.Contains(string(body), n.Explanation()) {
			t.Errorf("GET %s says %q, want the removal and its remedy", path, body)
		}
	}
	if !strings.Contains(n.Explanation(), server.RemovedNodeRemedy) || !strings.Contains(n.Explanation(), n.NodeID) {
		t.Fatalf("explanation %q does not name the node and the remedy", n.Explanation())
	}
}
