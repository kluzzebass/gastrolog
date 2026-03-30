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
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// maxChunkTransferBytes is the max gRPC receive message size for the cluster
// port. Covers the unary ForwardRecords batch ingestion RPC.
const maxChunkTransferBytes = 128 * 1024 * 1024 // 128 MB

// Config holds cluster server configuration.
type Config struct {
	// ClusterAddr is the listen address for the cluster gRPC port (e.g., ":4566").
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
	tm        *multiraft.Transport[string]
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

	// evictionHandler is called when this node receives a NotifyEviction RPC,
	// meaning it has been removed from the cluster and should shut down.
	evictionHandler func()

	// removeNodeFn handles the full node removal on the leader: Raft membership
	// change + eviction notification. Set by the composition root in main.go.
	removeNodeFn func(ctx context.Context, nodeID string) error

	// setNodeSuffrageFn handles promote/demote on the leader. Set by main.go.
	setNodeSuffrageFn func(ctx context.Context, nodeID, nodeAddr string, voter bool) error

	// recordAppender writes forwarded records into local vaults.
	// Set after the orchestrator is created, before forwarding starts.
	recordAppender RecordAppender

	// recordTierAppender writes forwarded records into a specific tier.
	// Used for inter-tier transition when tier_id is set on ForwardRecordsRequest.
	recordTierAppender RecordTierAppender

	// sealTierExecutor seals a specific tier's active chunk on this node.
	// Used for seal synchronization during replication.
	sealTierExecutor SealTierExecutor

	// deleteChunkExecutor deletes a chunk from a tier on this node.
	// Used by primary-driven retention: primary decides, secondaries obey.
	deleteChunkExecutor DeleteChunkExecutor

	// recordImporter imports records as a sealed chunk in a local vault.
	// Set after the orchestrator is created, before chunk transfer starts.
	recordImporter RecordImporter

	// tierRecordImporter imports records as a sealed chunk in a specific tier,
	// preserving the original chunk ID. Used for sealed-chunk replication.
	tierRecordImporter TierRecordImporter

	// tierStreamAppender appends streamed records to a tier's active chunk.
	// Used for tier transitions (records flow like normal ingestion).
	tierStreamAppender TierStreamAppender

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

	// explainExecutor returns explain plans for local vaults for remote Explain requests.
	explainExecutor ExplainExecutor

	// followExecutor runs a follow (tail -f) on local vaults for remote requests.
	followExecutor FollowExecutor

	// getChunkExecutor returns details for a specific chunk in a local vault.
	getChunkExecutor GetChunkExecutor

	// analyzeChunkExecutor runs index analysis on a local vault.
	analyzeChunkExecutor AnalyzeChunkExecutor

	// sealVaultExecutor seals the active chunk of a local vault.
	sealVaultExecutor SealVaultExecutor

	// reindexVaultExecutor rebuilds all indexes for a local vault.
	reindexVaultExecutor ReindexVaultExecutor

	// exportToVaultExecutor runs an export-to-vault job on a local vault.
	exportToVaultExecutor ExportToVaultExecutor

	// managedFileReader opens a managed file for streaming to peers.
	managedFileReader ManagedFileReader

	// managedFileIDs returns which managed files exist on this node.
	managedFileIDs ManagedFileIDsLister

	// internalHandler is the Connect mux used for dispatching ForwardRPC
	// requests. It has no routing interceptor (preventing loops) and uses
	// NoAuthInterceptor (mTLS already verified the peer). Set by the
	// composition root before Start().
	internalHandler http.Handler

	// forwardedReceived counts records received via ForwardRecords RPCs.
	forwardedReceived atomic.Int64

	// peerConns is the shared connection pool for all peer communication.
	// Created in SetRaft once the raft instance is available.
	peerConns *PeerConns
}

// ForwardedReceived returns the number of records received via ForwardRecords RPCs.
func (s *Server) ForwardedReceived() int64 {
	return s.forwardedReceived.Load()
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

// ConfigGroupID is the well-known group ID for the cluster config Raft group.
const ConfigGroupID = "config"

// Transport creates the multi-raft transport and returns a raft.Transport
// scoped to the config group, suitable for passing to raft.NewRaft().
// Must be called before Start().
func (s *Server) Transport() hraft.Transport {
	var creds credentials.TransportCredentials
	if s.cfg.TLS != nil {
		creds = s.cfg.TLS.TransportCredentials()
	} else {
		creds = insecure.NewCredentials()
	}

	s.tm = multiraft.New[string](
		hraft.ServerAddress(s.localAddr),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(creds),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  500 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   3 * time.Second,
				},
			}),
		},
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	return s.tm.GroupTransport(ConfigGroupID)
}

// MultiRaftTransport returns the underlying multi-raft transport for creating
// additional group transports (e.g., tier Raft groups).
func (s *Server) MultiRaftTransport() *multiraft.Transport[string] {
	return s.tm
}

// SetRaft provides the Raft instance after it is created.
// Must be called before Start(). If PeerConns already exists (rejoin case),
// it resets the pool with the new Raft instance instead of creating a new one.
func (s *Server) SetRaft(r *hraft.Raft) {
	s.raft = r
	if s.peerConns != nil {
		s.peerConns.Reset(r)
	} else {
		s.peerConns = NewPeerConns(r, s.cfg.TLS, s.cfg.NodeID)
	}
}

// PeerConns returns the shared peer connection pool. All components that
// need to communicate with peer nodes should use this single pool.
// Returns nil if SetRaft has not been called.
func (s *Server) PeerConns() *PeerConns {
	return s.peerConns
}

// NewPeerConnsPool creates an independent connection pool using the same
// Raft discovery and TLS config. Use for bulk traffic (replication, migration)
// that shouldn't compete for HTTP/2 flow control with queries and config RPCs.
func (s *Server) NewPeerConnsPool() *PeerConns {
	return NewPeerConns(s.raft, s.cfg.TLS, s.cfg.NodeID)
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

// RemoveServer removes a node from the Raft cluster entirely.
// Must be called on the leader. The removed node stops receiving
// log replication and is no longer part of quorum or elections.
func (s *Server) RemoveServer(id string, timeout time.Duration) error {
	if s.raft == nil {
		return errors.New("raft not initialized")
	}
	return s.raft.RemoveServer(hraft.ServerID(id), 0, timeout).Error()
}

// LeadershipTransfer transfers leadership to another voter in the cluster.
// Blocks until the transfer completes or the timeout expires.
func (s *Server) LeadershipTransfer() error {
	if s.raft == nil {
		return errors.New("raft not initialized")
	}
	return s.raft.LeadershipTransfer().Error()
}

// SetInternalHandler provides the Connect mux used for dispatching ForwardRPC
// requests. This should be a mux with NoAuthInterceptor and NO routing
// interceptor — ForwardRPC dispatches execute locally without re-routing.
func (s *Server) SetInternalHandler(h http.Handler) {
	s.internalHandler = h
}

// SetApplyFn sets the function used by the ForwardApply handler to apply
// commands on the leader node.
func (s *Server) SetApplyFn(fn func(ctx context.Context, data []byte) error) {
	s.applyFn = fn
}

// SetEvictionHandler registers the callback invoked when this node receives
// a NotifyEviction RPC (i.e., it has been removed from the cluster).
func (s *Server) SetEvictionHandler(fn func()) {
	s.evictionHandler = fn
}

// SetRemoveNodeFn registers the callback for the ForwardRemoveNode RPC.
// This is called on the leader to execute the Raft removal + notification.
func (s *Server) SetRemoveNodeFn(fn func(ctx context.Context, nodeID string) error) {
	s.removeNodeFn = fn
}

// SetNodeSuffrageFn registers the callback for the ForwardSetNodeSuffrage RPC.
// This is called on the leader to execute the Raft suffrage change.
func (s *Server) SetNodeSuffrageFn(fn func(ctx context.Context, nodeID, nodeAddr string, voter bool) error) {
	s.setNodeSuffrageFn = fn
}

// Start creates the gRPC server, registers all services, and begins serving.
// The listener was already bound in New().
func (s *Server) Start() error {
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(maxChunkTransferBytes))

	if s.cfg.TLS != nil {
		tlsCfg := s.cfg.TLS.ServerTLSConfig()
		opts = append(opts,
			grpc.Creds(credentials.NewTLS(tlsCfg)),
			grpc.ChainUnaryInterceptor(s.mTLSUnaryInterceptor),
			grpc.ChainStreamInterceptor(s.mTLSStreamInterceptor),
		)
	}

	s.grpcSrv = grpc.NewServer(opts...)

	// Multi-raft transport (AppendEntries, RequestVote, InstallSnapshot, etc.).
	// Multiplexes all Raft groups (config + future tier groups) over one gRPC service.
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

	// Close the transport FIRST — this unblocks any gRPC handlers stuck in
	// handleRPC waiting on rpcChan (by closing shutdownCh + group channels).
	// Without this, GracefulStop deadlocks: it waits for active RPCs to finish,
	// but the RPCs can't finish because nothing drains their rpcChan.
	if s.tm != nil {
		_ = s.tm.Close()
	}
	if s.peerConns != nil {
		_ = s.peerConns.Close()
	}

	done := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.logger.Debug("cluster gRPC graceful stop timed out, forcing")
		s.grpcSrv.Stop()
	}
}

// PrepareRejoin stops the cluster gRPC server and re-binds the listen port,
// returning a fresh transport for the new Raft instance. Because raftadmin
// captures the *raft.Raft pointer at registration time and gRPC doesn't
// support service re-registration, we must stop and restart the gRPC server.
//
// The caller must:
//  1. Create a new Raft with the returned transport
//  2. Call SetRaft(newRaft), SetApplyFn(fn), SetEnrollHandler(h)
//  3. Call Start() to restart the cluster gRPC server
//
// The cluster port is down for ~100-500ms. The API port stays up throughout.
func (s *Server) PrepareRejoin() (hraft.Transport, error) {
	s.Stop()

	ln, err := net.Listen("tcp", s.cfg.ClusterAddr)
	if err != nil {
		return nil, fmt.Errorf("re-listen cluster port %s: %w", s.cfg.ClusterAddr, err)
	}
	s.listener = ln

	// Update localAddr in case :0 was used (unlikely, but be correct).
	if s.cfg.LocalAddr == "" {
		s.localAddr = ln.Addr().String()
	}

	return s.Transport(), nil
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

// IsLeader returns true if this node is the current Raft leader.
func (s *Server) IsLeader() bool {
	if s.raft == nil {
		return false
	}
	return s.raft.State() == hraft.Leader
}

// RegisterLeaderObserver registers a channel to receive Raft LeaderObservation
// events. The placement manager uses this to react immediately to leadership
// changes rather than polling.
func (s *Server) RegisterLeaderObserver(ch chan hraft.Observation) {
	if s.raft == nil {
		return
	}
	s.raft.RegisterObserver(hraft.NewObserver(ch, true, func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.LeaderObservation)
		return ok
	}))
}

// Addr returns the listener address, or empty if not started.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}
