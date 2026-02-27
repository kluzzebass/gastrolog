// Package cluster manages the dedicated cluster gRPC port used for Raft
// consensus and inter-node RPCs. The cluster port is separate from the
// HTTPS API port and uses plain gRPC (TLS is added by gastrolog-2lzw).
//
// Lifecycle:
//  1. New(cfg)           — create the server and bind the listen port
//  2. Transport()        — get the raft.Transport for raft.NewRaft()
//  3. SetRaft(r)         — provide the Raft instance after creation
//  4. SetApplyFn(fn)     — provide the leader's apply function
//  5. Start()            — register services and serve
//  6. Stop()             — graceful shutdown
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"gastrolog/internal/logging"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Config holds cluster server configuration.
type Config struct {
	// ClusterAddr is the listen address for the cluster gRPC port (e.g., ":4565").
	ClusterAddr string

	// LocalAddr is the advertised address other nodes use to reach this node's
	// cluster port. Defaults to ClusterAddr if empty.
	LocalAddr string

	// TLS holds atomic TLS state for mTLS on the cluster port.
	// When nil, the cluster port uses insecure credentials (tests, single-node).
	TLS *ClusterTLS

	// Logger for structured logging.
	Logger *slog.Logger
}

// Server manages the cluster gRPC port, Raft transport, and inter-node services.
type Server struct {
	cfg       Config
	grpcSrv   *grpc.Server
	tm        *transport.Manager
	listener  net.Listener
	localAddr string // advertised address (may differ from listen addr)
	logger    *slog.Logger

	// Set after Raft is created, before Start().
	raft *hraft.Raft

	// applyFn applies a pre-marshaled ConfigCommand on the leader.
	applyFn func(ctx context.Context, data []byte) error

	// enrollHandler handles the Enroll RPC for joining nodes.
	enrollHandler EnrollHandler
}

// New creates a new cluster Server and binds the listen port immediately.
// The port is bound early so the actual address (including resolved :0 ports)
// is available for Transport() to advertise to other nodes.
func New(cfg Config) (*Server, error) {
	ln, err := net.Listen("tcp", cfg.ClusterAddr)
	if err != nil {
		return nil, fmt.Errorf("listen cluster port %s: %w", cfg.ClusterAddr, err)
	}

	// Use the actual bound address as the advertised address unless explicitly set.
	localAddr := cfg.LocalAddr
	if localAddr == "" {
		localAddr = ln.Addr().String()
	}

	return &Server{
		cfg:      cfg,
		listener: ln,
		logger:   logging.Default(cfg.Logger),
		localAddr: localAddr,
	}, nil
}

// Transport creates the raft-grpc-transport Manager and returns a
// raft.Transport suitable for passing to raft.NewRaft().
// Must be called before Start().
func (s *Server) Transport() hraft.Transport {
	var creds credentials.TransportCredentials
	if s.cfg.TLS != nil {
		creds = s.cfg.TLS.TransportCredentials()
	} else {
		creds = insecure.NewCredentials()
	}

	s.tm = transport.New(
		hraft.ServerAddress(s.localAddr),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(creds),
		},
	)
	return s.tm.Transport()
}

// SetRaft provides the Raft instance after it is created.
// Must be called before Start().
func (s *Server) SetRaft(r *hraft.Raft) {
	s.raft = r
}

// AddVoter adds a new node to the Raft cluster as a voter.
// The leader must be the one calling this. Blocks until the change is committed
// or the timeout expires.
func (s *Server) AddVoter(id, addr string, timeout time.Duration) error {
	if s.raft == nil {
		return errors.New("raft not initialized")
	}
	return s.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, timeout).Error()
}

// SetApplyFn sets the function used by the ForwardApply handler to apply
// commands on the leader node.
func (s *Server) SetApplyFn(fn func(ctx context.Context, data []byte) error) {
	s.applyFn = fn
}

// Start creates the gRPC server, registers all services, and begins serving.
// The listener was already bound in New().
func (s *Server) Start() error {
	var opts []grpc.ServerOption

	if s.cfg.TLS != nil {
		tlsCfg := s.cfg.TLS.ServerTLSConfig()
		opts = append(opts,
			grpc.Creds(credentials.NewTLS(tlsCfg)),
			grpc.ChainUnaryInterceptor(s.mTLSUnaryInterceptor),
			grpc.ChainStreamInterceptor(s.mTLSStreamInterceptor),
		)
	}

	s.grpcSrv = grpc.NewServer(opts...)

	// Raft transport (AppendEntries, RequestVote, InstallSnapshot, etc.).
	s.tm.Register(s.grpcSrv)

	// Membership management (AddVoter, RemoveServer, GetConfiguration, etc.).
	if s.raft != nil {
		raftadmin.Register(s.grpcSrv, s.raft)
		leaderhealth.Setup(s.raft, s.grpcSrv, []string{"cluster"})
	}

	// Cluster service (ForwardApply + Enroll).
	registerClusterService(s.grpcSrv, s)

	s.logger.Info("cluster gRPC server starting", "addr", s.listener.Addr().String())

	go func() {
		if err := s.grpcSrv.Serve(s.listener); err != nil {
			s.logger.Error("cluster gRPC server error", "error", err)
		}
	}()

	return nil
}

// mTLSUnaryInterceptor enforces client certificates on all RPCs except Enroll.
func (s *Server) mTLSUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := requireClientCert(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// mTLSStreamInterceptor enforces client certificates on all streaming RPCs.
func (s *Server) mTLSStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := requireClientCert(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

// requireClientCert checks that the peer presented a verified client certificate.
// The Enroll RPC is exempt — joining nodes don't have a cert yet.
func requireClientCert(ctx context.Context, method string) error {
	if strings.HasSuffix(method, "/Enroll") {
		return nil
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer info")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return status.Error(codes.Unauthenticated, "no TLS info")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 {
		return status.Error(codes.Unauthenticated, "client certificate required")
	}
	return nil
}

// Stop gracefully stops the cluster gRPC server with a 10-second deadline.
func (s *Server) Stop() {
	if s.grpcSrv == nil {
		return
	}

	done := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.logger.Warn("cluster gRPC graceful stop timed out, forcing")
		s.grpcSrv.Stop()
	}

	if s.tm != nil {
		_ = s.tm.Close()
	}
}

// Addr returns the listener address, or empty if not started.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}
