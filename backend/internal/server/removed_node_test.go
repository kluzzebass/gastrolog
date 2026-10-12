package server_test

// A node the cluster removed keeps serving one thing: the account of its
// removal, on its Unix socket. Status, health and the preStop hook's
// question get answers there; everything else, every administrative verb
// included, is refused with that account and nothing behind it runs.

import (
	"context"
	"errors"
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

// Anyone who reaches the TCP listener of a removed node learns what its
// readiness probe already says and nothing more: not which node it was, not
// when or why it was removed, not its version. The account is for the Unix
// socket, whose file permissions are the access control.
func TestRemovedNodePublicSurfaceRevealsNothingMore(t *testing.T) {
	t.Parallel()
	n, _, _ := removedNodeAPI(t)
	api := httptest.NewServer(server.RemovedNodePublicHandler())
	t.Cleanup(api.Close)
	client := server.NewClientWithHTTP(api.Client(), api.URL)
	secrets := []string{n.NodeID, n.Reason, n.RemovedAt.Format(time.RFC3339), "test-version", server.RemovedNodeRemedy}
	leaks := func(where, text string) {
		t.Helper()
		for _, secret := range secrets {
			if strings.Contains(text, secret) {
				t.Errorf("%s reveals %q: %q", where, secret, text)
			}
		}
	}

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
		if want == http.StatusServiceUnavailable && strings.TrimSpace(string(body)) != server.RemovedNodePublicNotice {
			t.Errorf("GET %s says %q, want only %q", path, body, server.RemovedNodePublicNotice)
		}
		leaks("GET "+path, string(body))
	}

	ctx := context.Background()
	for name, call := range map[string]func() error{
		"GetClusterStatus": func() error {
			_, err := client.Lifecycle.GetClusterStatus(ctx, connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
			return err
		},
		"Health": func() error {
			_, err := client.Lifecycle.Health(ctx, connect.NewRequest(&gastrologv1.HealthRequest{}))
			return err
		},
		"YieldLeadership": func() error {
			_, err := client.Lifecycle.YieldLeadership(ctx, connect.NewRequest(&gastrologv1.YieldLeadershipRequest{}))
			return err
		},
		"Shutdown": func() error {
			_, err := client.Lifecycle.Shutdown(ctx, connect.NewRequest(&gastrologv1.ShutdownRequest{}))
			return err
		},
	} {
		err := call()
		var cerr *connect.Error
		if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition || cerr.Message() != server.RemovedNodePublicNotice {
			t.Errorf("%s over TCP: %v, want FailedPrecondition %q only", name, err, server.RemovedNodePublicNotice)
			continue
		}
		leaks(name+" over TCP", err.Error())
	}
}

func TestRemovedNodeExplanationNamesNodeAndRemedy(t *testing.T) {
	t.Parallel()
	n, _, _ := removedNodeAPI(t)
	if !strings.Contains(n.Explanation(), server.RemovedNodeRemedy) || !strings.Contains(n.Explanation(), n.NodeID) {
		t.Fatalf("explanation %q does not name the node and the remedy", n.Explanation())
	}
}
