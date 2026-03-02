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

	// NodeID is this node's unique identifier, used to exclude self from peer lists.
	NodeID string

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

	// subscribers receives broadcast messages from peers.
	subscribers subscriberRegistry

	// recordAppender writes forwarded records into local vaults.
	// Set after the orchestrator is created, before forwarding starts.
	recordAppender RecordAppender

	// searchExecutor runs a search on a local vault for remote search requests.
	// Set after the orchestrator is created, before search forwarding starts.
	searchExecutor SearchExecutor

	// contextExecutor fetches surrounding records from a local vault for
	// remote GetContext requests.
	contextExecutor ContextExecutor

	// listChunksExecutor lists chunks in a local vault for remote ListChunks requests.
	listChunksExecutor ListChunksExecutor

	// getIndexesExecutor returns index status for a local chunk for remote GetIndexes requests.
	getIndexesExecutor GetIndexesExecutor

	// validateVaultExecutor validates a local vault for remote ValidateVault requests.
	validateVaultExecutor ValidateVaultExecutor

	// peerConns is the shared connection pool for all peer communication.
	// Created in SetRaft once the raft instance is available.
	peerConns *PeerConns
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
	s.peerConns = NewPeerConns(r, s.cfg.TLS, s.cfg.NodeID)
}

// PeerConns returns the shared peer connection pool. All components that
// need to communicate with peer nodes should use this single pool.
// Returns nil if SetRaft has not been called.
func (s *Server) PeerConns() *PeerConns {
	return s.peerConns
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

// AddNonvoter adds a new node to the Raft cluster as a nonvoter.
// Nonvoters receive log replication but do not participate in elections or quorum.
func (s *Server) AddNonvoter(id, addr string, timeout time.Duration) error {
	if s.raft == nil {
		return errors.New("raft not initialized")
	}
	return s.raft.AddNonvoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, timeout).Error()
}

// DemoteVoter demotes an existing voter to a nonvoter.
// The node continues receiving log replication but no longer participates in elections.
func (s *Server) DemoteVoter(id string, timeout time.Duration) error {
	if s.raft == nil {
		return errors.New("raft not initialized")
	}
	return s.raft.DemoteVoter(hraft.ServerID(id), 0, timeout).Error()
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
		s.logger.Debug("cluster gRPC graceful stop timed out, forcing")
		s.grpcSrv.Stop()
	}

	if s.peerConns != nil {
		_ = s.peerConns.Close()
	}
	if s.tm != nil {
		_ = s.tm.Close()
	}
}

// LeaderInfo returns the current Raft leader's address and server ID.
// Returns empty strings if there is no known leader.
func (s *Server) LeaderInfo() (address string, id string) {
	if s.raft == nil {
		return "", ""
	}
	addr, serverID := s.raft.LeaderWithID()
	return string(addr), string(serverID)
}

// Servers returns the current Raft configuration as a slice of server descriptions.
func (s *Server) Servers() ([]RaftServer, error) {
	if s.raft == nil {
		return nil, nil
	}
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	cfg := future.Configuration()
	servers := make([]RaftServer, 0, len(cfg.Servers))
	for _, srv := range cfg.Servers {
		var suffrage string
		switch srv.Suffrage {
		case hraft.Voter:
			suffrage = "Voter"
		case hraft.Nonvoter:
			suffrage = "Nonvoter"
		case hraft.Staging:
			suffrage = "Staging"
		}
		servers = append(servers, RaftServer{
			ID:       string(srv.ID),
			Address:  string(srv.Address),
			Suffrage: suffrage,
		})
	}
	return servers, nil
}

// RaftServer describes a single node in the Raft configuration.
type RaftServer struct {
	ID       string
	Address  string
	Suffrage string
}

// LocalStats returns the local Raft node's stats as a string map.
// Returns nil if Raft is not initialized.
func (s *Server) LocalStats() map[string]string {
	if s.raft == nil {
		return nil
	}
	return s.raft.Stats()
}

// Addr returns the listener address, or empty if not started.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}
