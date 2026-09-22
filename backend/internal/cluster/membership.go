package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ErrNoLeader reports that an operation needing the leader found none. The
// cluster elects one within an election timeout, so every caller treats this
// as worth retrying rather than as a failure.
var ErrNoLeader = errors.New("no leader available")

const (
	// membershipInitialBackoff is the first wait between retries. Short
	// enough not to add perceptible startup latency in the common case
	// where the member answers immediately.
	membershipInitialBackoff = 100 * time.Millisecond
	// membershipMaxBackoff caps the exponential backoff. Two seconds is
	// well above the configuration-change commit latency of a healthy
	// cluster, so once the wait reaches the cap the problem is no longer
	// contention but a member that is genuinely not answering — which is
	// what the caller's context deadline is for.
	membershipMaxBackoff = 2 * time.Second
)

// MembershipHandler adds a node to the Raft configuration on behalf of a
// caller that asked to join. Registered via Server.SetMembershipHandler; the
// implementation forwards to the leader when this node does not lead, which
// is what lets a joiner address any member.
type MembershipHandler func(ctx context.Context, nodeID, nodeAddr string, voter bool) error

// SetMembershipHandler registers the callback invoked when a node asks to be
// put in the configuration.
func (s *Server) SetMembershipHandler(h MembershipHandler) {
	s.membershipHandler = h
}

// requestMembership handles the RequestMembership RPC.
func (s *Server) requestMembership(ctx context.Context, req *gastrologv1.RequestMembershipRequest) (*gastrologv1.RequestMembershipResponse, error) {
	if s.membershipHandler == nil {
		return nil, status.Error(codes.Unavailable, "membership handler not configured")
	}
	nodeID := string(req.GetNodeId())
	if nodeID == "" || req.GetNodeAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "node id and address are required")
	}
	if err := s.membershipHandler(ctx, nodeID, req.GetNodeAddr(), req.GetVoter()); err != nil {
		// A cluster mid-election has no leader to forward to yet. Saying
		// Unavailable rather than Internal is what tells the joiner to keep
		// asking instead of giving up on a condition that resolves itself.
		if errors.Is(err, ErrNoLeader) {
			return nil, status.Errorf(codes.Unavailable, "membership change: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "membership change: %v", err)
	}
	return &gastrologv1.RequestMembershipResponse{}, nil
}

// requestMembershipRPCHandler is the gRPC MethodDesc handler for the
// RequestMembership RPC.
func requestMembershipRPCHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.RequestMembershipRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.requestMembership(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/RequestMembership",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.requestMembership(ctx, req.(*gastrologv1.RequestMembershipRequest))
	}
	return interceptor(ctx, req, info, handler)
}

// membershipCredentials picks the transport for a membership request. Cluster
// TLS is bootstrapped or loaded before any node asks to join, so a holder
// without material means startup left it unloaded — the request is refused
// rather than retried in plaintext against a peer that will not answer it
// anyway. A caller with no holder at all runs without cluster TLS by
// construction.
func membershipCredentials(ctls *ClusterTLS) (credentials.TransportCredentials, error) {
	if ctls == nil {
		return insecure.NewCredentials(), nil
	}
	if ctls.State() == nil {
		return nil, ErrClusterTLSUnloaded
	}
	return ctls.TransportCredentials(), nil
}

// JoinCluster asks a cluster member to put this node in the Raft
// configuration, retrying until the caller's context deadline.
//
// addr is any member's cluster address. The member performs the
// configuration change itself when it leads and forwards to the leader when
// it does not, so nothing here discovers, follows, or cares about
// leadership. The joiner's own authority ends at presenting the certificate
// enrolment issued it; the cluster decides what that buys.
//
// Retries cover the startup races: a member dialled while its gRPC server is
// still binding, and a cluster that has not finished electing. Anything else
// — a TLS handshake failure, a refusal, a malformed advertise address —
// returns immediately, because waiting will not change it.
//
// logger may be nil, in which case retry attempts are silent.
func JoinCluster(ctx context.Context, logger *slog.Logger, addr, nodeID, nodeAddr string, ctls *ClusterTLS, voter bool) error {
	creds, err := membershipCredentials(ctls)
	if err != nil {
		return fmt.Errorf("join %s: %w", addr, err)
	}

	backoff := membershipInitialBackoff
	attempt := 0
	for {
		attempt++
		err := tryRequestMembership(ctx, addr, nodeID, nodeAddr, creds, voter)
		if err == nil {
			return nil
		}
		if !isTransientMembershipErr(err) {
			return err
		}

		if logger != nil {
			logger.Warn("request membership: transient failure, retrying",
				"attempt", attempt, "backoff", backoff, "addr", addr, "error", err)
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > membershipMaxBackoff {
			backoff = membershipMaxBackoff
		}
	}
}

// isTransientMembershipErr reports whether the failure is one that resolves
// by waiting: a member still starting its gRPC server, or a cluster still
// electing. A refused TCP connection surfaces as Unavailable through gRPC
// and as ECONNREFUSED from a direct dial; the member maps its own
// no-leader condition onto Unavailable too.
func isTransientMembershipErr(err error) bool {
	if err == nil {
		return false
	}
	return status.Code(err) == codes.Unavailable || errors.Is(err, syscall.ECONNREFUSED)
}

func tryRequestMembership(ctx context.Context, addr, nodeID, nodeAddr string, creds credentials.TransportCredentials, voter bool) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	req := &gastrologv1.RequestMembershipRequest{
		NodeId:   []byte(nodeID),
		NodeAddr: nodeAddr,
		Voter:    voter,
	}
	out := &gastrologv1.RequestMembershipResponse{}
	return conn.Invoke(ctx, "/gastrolog.v1.ClusterService/RequestMembership", req, out)
}
