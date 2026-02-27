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
	"fmt"
	"log/slog"
	"net"
	"time"

	"gastrolog/internal/logging"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config holds cluster server configuration.
type Config struct {
	// ClusterAddr is the listen address for the cluster gRPC port (e.g., ":4565").
	ClusterAddr string

	// LocalAddr is the advertised address other nodes use to reach this node's
	// cluster port. Defaults to ClusterAddr if empty.
	LocalAddr string

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
	s.tm = transport.New(
		hraft.ServerAddress(s.localAddr),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	)
	return s.tm.Transport()
}

// SetRaft provides the Raft instance after it is created.
// Must be called before Start().
func (s *Server) SetRaft(r *hraft.Raft) {
	s.raft = r
}

// SetApplyFn sets the function used by the ForwardApply handler to apply
// commands on the leader node.
func (s *Server) SetApplyFn(fn func(ctx context.Context, data []byte) error) {
	s.applyFn = fn
}

// Start creates the gRPC server, registers all services, and begins serving.
// The listener was already bound in New().
func (s *Server) Start() error {
	s.grpcSrv = grpc.NewServer()

	// Raft transport (AppendEntries, RequestVote, InstallSnapshot, etc.).
	s.tm.Register(s.grpcSrv)

	// Membership management (AddVoter, RemoveServer, GetConfiguration, etc.).
	if s.raft != nil {
		raftadmin.Register(s.grpcSrv, s.raft)
		leaderhealth.Setup(s.raft, s.grpcSrv, []string{"cluster"})
	}

	// Leader forwarding for config writes from followers.
	registerForwardService(s.grpcSrv, s)

	s.logger.Info("cluster gRPC server starting", "addr", s.listener.Addr().String())

	go func() {
		if err := s.grpcSrv.Serve(s.listener); err != nil {
			s.logger.Error("cluster gRPC server error", "error", err)
		}
	}()

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
