package cluster

// A node asking to join addresses whatever member it was told about. The
// member it reaches is usually not the leader, so what matters here is that
// the request gets served anyway, and that the joiner can tell "ask again"
// apart from "no".

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestMembershipServesTheCallerThatReachedANonLeader(t *testing.T) {
	t.Parallel()

	var gotID, gotAddr string
	s := &Server{}
	// The handler app registers routes to the leader by itself. Whether this
	// node leads is therefore invisible from here, which is the property the
	// joiner depends on.
	s.SetMembershipHandler(func(_ context.Context, nodeID, nodeAddr string) error {
		gotID, gotAddr = nodeID, nodeAddr
		return nil
	})

	_, err := s.requestMembership(context.Background(), &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte("node-2"),
		NodeAddr: "node-2:4566",
	})
	if err != nil {
		t.Fatalf("a member refused a membership request: %v", err)
	}
	if gotID != "node-2" || gotAddr != "node-2:4566" {
		t.Fatalf("the request reached the cluster altered: id=%q addr=%q", gotID, gotAddr)
	}
}

// The address has to arrive on the request: the node it describes is not in
// the configuration the cluster would otherwise resolve it from. Admitting
// without one would put an unreachable entry in the configuration.
func TestRequestMembershipRefusesAnIncompleteRequest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  *gastrologv1.RequestMembershipRequest
	}{
		{"no address", &gastrologv1.RequestMembershipRequest{NodeId: []byte("node-2")}},
		{"no node id", &gastrologv1.RequestMembershipRequest{NodeAddr: "node-2:4566"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &Server{}
			s.SetMembershipHandler(func(context.Context, string, string) error {
				t.Fatal("an incomplete request reached the configuration")
				return nil
			})
			if _, err := s.requestMembership(context.Background(), tc.req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v, want InvalidArgument", err)
			}
		})
	}
}

// A cluster mid-election has no leader to forward to. That resolves on its
// own within an election timeout, so the joiner has to be told to come back
// rather than to give up — and the only thing carrying that distinction
// across the wire is the status code.
func TestRequestMembershipTellsAJoinerToRetryDuringAnElection(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.SetMembershipHandler(func(context.Context, string, string) error {
		return fmt.Errorf("forward suffrage: %w", ErrNoLeader)
	})

	_, err := s.requestMembership(context.Background(), &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte("node-2"),
		NodeAddr: "node-2:4566",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want Unavailable so the joiner retries", err)
	}
	if !isTransientMembershipErr(err) {
		t.Fatal("the joiner would treat an election as fatal and stop asking")
	}
}

// A refusal must not be retried: repeating it neither succeeds nor tells the
// operator anything, and the joiner should fail startup with the reason.
func TestRequestMembershipSurfacesARefusalAsFinal(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.SetMembershipHandler(func(context.Context, string, string) error {
		return errors.New("node id already in the configuration at another address")
	})

	_, err := s.requestMembership(context.Background(), &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte("node-2"),
		NodeAddr: "node-2:4566",
	})
	if err == nil {
		t.Fatal("a refused membership change was reported as success")
	}
	if isTransientMembershipErr(err) {
		t.Fatalf("the joiner would retry a refusal until its deadline: %v", err)
	}
}

// A node the cluster removed is refused for good. The refusal has to read as
// one — PermissionDenied, not an internal failure an operator would chase —
// and the joiner has to stop asking, because no amount of waiting brings a
// removed identity back.
func TestRequestMembershipRefusesARemovedNodeAsFinal(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.SetMembershipHandler(func(_ context.Context, nodeID, _ string) error {
		return fmt.Errorf("%s: %w", nodeID, ErrNodeRemoved)
	})

	_, err := s.requestMembership(context.Background(), &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte("node-gone"),
		NodeAddr: "node-gone:4566",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("got %v, want PermissionDenied", err)
	}
	if isTransientMembershipErr(err) {
		t.Fatalf("the joiner would keep asking to be let back in: %v", err)
	}
}

func TestIsTransientMembershipErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
		why  string
	}{
		{"a member still binding its listener", status.Error(codes.Unavailable, "connection refused"), true,
			"the member is starting; it will answer shortly"},
		{"a direct dial to a closed port", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true,
			"same condition, reached without gRPC in between"},
		{"a cluster mid-election", status.Error(codes.Unavailable, "membership change: no leader available"), true,
			"an election ends on its own"},
		{"a certificate the cluster will not accept", status.Error(codes.Unauthenticated, "client certificate required"), false,
			"waiting does not produce a certificate"},
		{"a request the cluster refuses", status.Error(codes.InvalidArgument, "node id and address are required"), false,
			"the request is wrong, not early"},
		{"nothing went wrong", nil, false, "there is no failure to classify"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isTransientMembershipErr(tc.err); got != tc.want {
				t.Fatalf("classified retryable=%v, but %s", got, tc.why)
			}
		})
	}
}

// Without a handler the node cannot admit anyone, and saying so as
// Unavailable keeps the joiner asking — the next member it reaches, or this
// one once startup finishes wiring, can serve it.
func TestRequestMembershipBeforeWiringIsRetryable(t *testing.T) {
	t.Parallel()

	s := &Server{}
	_, err := s.requestMembership(context.Background(), &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte("node-2"),
		NodeAddr: "node-2:4566",
	})
	if !isTransientMembershipErr(err) {
		t.Fatalf("got %v, want a retryable failure", err)
	}
}
