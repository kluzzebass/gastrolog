// Package cluster manages the dedicated cluster gRPC port used for Raft
// consensus and inter-node RPCs. The cluster port is separate from the
// HTTPS API port and speaks gRPC over mTLS when Config.TLS is set (plain
// gRPC otherwise).
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
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// maxServiceLaneMsgBytes is the largest message either end of a service-lane
// connection receives: the cluster port's receive cap, and the default call
// option on every outbound service-lane connection. Imports carry one record
// per message, so no larger record moves between nodes, and forwarded search
// and follow results carry any record that fits in it: one built from a whole
// HTTP or OTLP request body, or from a whole decompressed Fluent Forward batch.
const maxServiceLaneMsgBytes = 128 << 20

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

	// ServicePoolMaxPerPeer caps parallel outbound service-lane connections
	// per peer (default DefaultServicePoolMaxPerPeer).
	ServicePoolMaxPerPeer int

	// ByteMetrics tracks per-peer gRPC wire bytes on the cluster port.
	// Inbound: server stats handler (all lanes). Outbound: mirrored from
	// PeerConnManager when wired in SetRaft.
	ByteMetrics *PeerByteMetrics

	// Logger for structured logging.
	Logger *slog.Logger
}

// Server manages the cluster gRPC port, Raft transport, and inter-node services.
type Server struct {
	cfg              Config
	grpcSrv          *grpc.Server // service lane (ClusterService, leader health, …)
	raftLaneMu       sync.Mutex
	raftGroupServers map[string]*grpc.Server // per-group raft lanes (TLS mode)
	sniDemux         *sniDemuxListener
	tm               *multiraft.Transport[string]
	listener         net.Listener
	localAddr        string // advertised address (may differ from listen addr)
	logger           *slog.Logger

	// stopCtx is cancelled by Stop() to signal long-running stream handlers
	// that they should return cleanly. Handlers that block in
	// stream.RecvMsg() — vault replication, stream forward records, forward
	// import records — wrap their Recv in recvOrShutdown() so they observe
	// this cancellation within a few milliseconds rather than waiting for
	// grpcSrv.GracefulStop()'s transport-level drain.
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// Set after Raft is created, before Start().
	raft *hraft.Raft

	// applyFn applies a pre-marshaled ConfigCommand on the leader and returns
	// the Raft log index at which the command was applied. Followers use the
	// index to wait for their own FSM to catch up before reading post-mutation
	// state.
	applyFn func(ctx context.Context, data []byte) (uint64, error)

	// groupApplyFn applies a pre-marshaled command to the multiraft group
	// identified by groupID and returns the Raft log index at which the
	// command was applied. Used by ForwardVaultApply for both the
	// OpVaultChunkFSM-wrapped chunk-FSM case and the native vault-ctl
	// command case — both target the vault-ctl Raft group via this single
	// function. Followers use the index to wait for their own group FSM to
	// catch up before reading post-mutation state.
	groupApplyFn func(ctx context.Context, groupID string, data []byte) (uint64, error)

	// enrollHandler handles the Enroll RPC for joining nodes.
	enrollHandler EnrollHandler

	// membershipHandler handles the RequestMembership RPC, putting a node
	// that asked to join into the Raft configuration.
	membershipHandler MembershipHandler

	// subscribers receives broadcast messages from peers.
	subscribers subscriberRegistry

	// evictionHandler is called when this node receives a NotifyEviction RPC,
	// meaning it has been removed from the cluster and should shut down.
	evictionHandler func()

	// removeNodeFn handles the full node removal on the leader: gate
	// evaluation, Raft membership change, and eviction notification. Set
	// by the composition root in app.go.
	removeNodeFn RemoveNodeFunc

	// setNodeSuffrageFn handles promote/demote on the leader. Set by main.go.
	setNodeSuffrageFn func(ctx context.Context, nodeID, nodeAddr string, voter bool) error

	// replicaCatchupFn handles RequestReplicaCatchup on the placement leader:
	// for each requested chunk ID, fan out a sealed-chunk push to the
	// requesting follower via the existing replicateToFollower machinery.
	// Returns the count of chunks for which a push was actually scheduled
	// (after leader-side filtering: tombstoned, cloud-backed, missing-locally).
	// Set by the composition root in app.go.
	replicaCatchupFn func(ctx context.Context, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (int, error)

	// recordImporter imports records as a sealed chunk in a local vault.
	// Set after the orchestrator is created, before chunk transfer starts.
	recordImporter RecordImporter

	// vaultRecordImporter imports records as a sealed chunk in a specific vault,
	// preserving the original chunk ID. Used for sealed-chunk replication.
	vaultRecordImporter VaultRecordImporter

	// searchExecutor runs a search on a local vault for remote search requests.
	// Set after the orchestrator is created, before search forwarding starts.
	searchExecutor SearchExecutor

	// chunkEventSubscriber subscribes a peer-streaming ForwardWatchChunks
	// connection to this node's local chunk event bus. The callback
	// runs the subscription loop, invoking sendFn for each typed event
	// translated into the wire proto; it returns when the caller's ctx
	// is cancelled or the send fails. Set after the orchestrator is
	// created, before WatchChunks aggregation starts.
	chunkEventSubscriber ChunkEventSubscriber

	// contextExecutor fetches surrounding records from a local vault for
	// remote GetContext requests.
	contextExecutor ContextExecutor

	// listChunksExecutor lists chunks in a local vault for remote ListChunks requests.
	listChunksExecutor ListChunksExecutor

	// waitVaultReadyExecutor blocks until a local vault is ready for remote
	// WaitVaultReady requests (drain synchronization).
	waitVaultReadyExecutor WaitVaultReadyExecutor

	// pipelineBacklogDiskExecutor returns local pipeline disk counts for remote fan-out.
	pipelineBacklogDiskExecutor PipelineBacklogDiskExecutor

	// getIndexesExecutor returns index status for a local chunk for remote GetIndexes requests.
	getIndexesExecutor GetIndexesExecutor

	// validateVaultExecutor validates a local vault for remote ValidateVault requests.
	validateVaultExecutor ValidateVaultExecutor
	// reconcileCloudIndexExecutor rebuilds this node's cloud index for remote
	// ForwardReconcileCloudIndex requests.
	reconcileCloudIndexExecutor ReconcileCloudIndexExecutor
	// validateIngesterExecutor runs this node's ingester checks for remote
	// ForwardValidateIngester requests.
	validateIngesterExecutor ValidateIngesterExecutor

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

	// segmentPullServer streams a locally-held completed segment to a peer
	// collector (Rubicon C).
	segmentPullServer   SegmentPullServer
	chunkGLCBPullServer ChunkGLCBPullServer

	// internalHandler is the Connect mux used for dispatching ForwardRPC
	// requests. It has no routing interceptor (preventing loops) and uses
	// NoAuthInterceptor (mTLS already verified the peer). Set by the
	// composition root before Start().
	internalHandler http.Handler

	// peerConns is the outbound peer connection manager.
	peerConns *PeerConnManager

	// pauseGate, when non-nil, makes every gRPC handler block until the
	// channel is closed. Used exclusively by reliability tests to simulate
	// a SIGSTOPed peer: the TCP socket stays accepted and the connection
	// stays open, but no RPC response ever returns. Production code does
	// not call Pause/Unpause; the pauseGate stays nil and the interceptor
	// is a no-op.
	pauseMu   sync.Mutex
	pauseGate chan struct{}

	// slowDown, when > 0, adds an artificial sleep before dispatching
	// each handler. Distinct from Pause: responses still return, just
	// slowly. Used by reliability tests to catch perf-sensitive
	// regressions (backoff tuning, timeout miscalibration) that don't
	// surface under Pause's full stop. Zero duration = no effect.
	slowMu  sync.Mutex
	slowDur time.Duration
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

	stopCtx, stopCancel := context.WithCancel(context.Background())
	return &Server{
		cfg:        cfg,
		listener:   ln,
		logger:     logging.Default(cfg.Logger),
		localAddr:  localAddr,
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}, nil
}

// errShuttingDown is returned by recvOrShutdown when the cluster server's
// stopCtx is cancelled before RecvMsg completes. Handlers interpret this
// as "return cleanly with no error" rather than logging the shutdown as
// a failure.
var errShuttingDown = errors.New("cluster server shutting down")

// recvOrShutdown wraps grpc.ServerStream.RecvMsg so the caller can exit
// cleanly when the cluster server starts shutting down, instead of
// blocking in RecvMsg until grpcSrv.GracefulStop() closes the transport.
//
// Usage:
//
//	if err := s.recvOrShutdown(stream, msg); err != nil {
//	    if errors.Is(err, io.EOF) || errors.Is(err, errShuttingDown) {
//	        return nil
//	    }
//	    return err
//	}
//
// Implementation spawns one goroutine per call that performs the actual
// RecvMsg. If RecvMsg returns first, we return its result. If stopCtx
// fires first, we return errShuttingDown and leave the goroutine
// dangling — it will be unblocked moments later when grpcSrv.GracefulStop
// (or Stop) closes the transport, at which point it drops the result on
// the floor. This costs at most one goroutine per active stream during
// the tiny shutdown window.
func (s *Server) recvOrShutdown(stream grpc.ServerStream, msg any) error {
	// Fast path: already shutting down — do not even try to Recv.
	if s.stopCtx.Err() != nil {
		return errShuttingDown
	}

	recvErr := make(chan error, 1)
	go func() {
		recvErr <- stream.RecvMsg(msg)
	}()

	select {
	case err := <-recvErr:
		return err
	case <-s.stopCtx.Done():
		return errShuttingDown
	}
}

// ConfigGroupID is the well-known multiraft transport group ID for the
// cluster-ctl Raft group (the on-disk directory is "cluster-ctl"; this is the
// transport routing key).
const ConfigGroupID = "config"

// Transport creates the multi-raft transport and returns a raft.Transport
// scoped to the config group, suitable for passing to raft.NewRaft().
// Must be called before Start().
func (s *Server) Transport() hraft.Transport {
	s.tm = multiraft.New(
		hraft.ServerAddress(s.localAddr),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	return s.tm.GroupTransport(ConfigGroupID)
}

// MultiRaftTransport returns the underlying multi-raft transport for creating
// additional group transports (e.g., vault-ctl Raft groups).
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
		s.peerConns = NewPeerConnManager(PeerConnManagerConfig{
			Raft:                  r,
			ClusterTLS:            s.cfg.TLS,
			NodeID:                s.cfg.NodeID,
			Logger:                s.logger,
			ServicePoolMaxPerPeer: s.cfg.ServicePoolMaxPerPeer,
			ByteMetrics:           s.cfg.ByteMetrics,
		})
	}
	if s.tm != nil {
		s.tm.SetPeerConnPool(s.peerConns)
	}
}

// ByteMetrics returns the shared per-peer byte-counter tracker. Returns
// nil if byte tracking is not configured.
func (s *Server) ByteMetrics() *PeerByteMetrics {
	return s.cfg.ByteMetrics
}

// PeerConns returns the outbound peer connection manager.
// Returns nil if SetRaft has not been called.
func (s *Server) PeerConns() *PeerConnManager {
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

// Removal refusals and misses. Wrapped by the removal path so the RPC layer
// can answer with FailedPrecondition or NotFound without reading message
// text, and so the CLI can tell an already-gone node from a failure.
var (
	// ErrRemovalRefused matches every removal-gate refusal. Both gate
	// sentinels below satisfy it, and so does a refusal rehydrated after the
	// follower-to-leader hop — where which gate fired is carried only by the
	// message, so no specific gate sentinel can honestly be claimed.
	ErrRemovalRefused = errors.New("removal refused by a removal gate")
	// ErrWouldDropBelowRF is the sentinel wrapped by every RF-preservation
	// refusal: the removal would drop a vault below its replication factor.
	ErrWouldDropBelowRF error = &removalRefusal{msg: "removal would drop a vault below its replication factor"}
	// ErrWouldOrphanVaults is the sentinel wrapped by every orphan refusal:
	// the removal would leave a vault with no holder at all.
	ErrWouldOrphanVaults error = &removalRefusal{msg: "removal would orphan a vault"}
	// ErrNodeNotInCluster is returned when the node named is not in the
	// Raft configuration.
	ErrNodeNotInCluster = errors.New("node not in cluster configuration")
)

// removalRefusal is a removal-gate refusal: its own message, and a match for
// ErrRemovalRefused.
type removalRefusal struct{ msg string }

func (e *removalRefusal) Error() string { return e.msg }

func (e *removalRefusal) Is(target error) bool { return target == ErrRemovalRefused }

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
	cfgFuture := s.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}
	found := false
	for _, srv := range cfgFuture.Configuration().Servers {
		if string(srv.ID) == id {
			found = true
			break
		}
	}
	if !found {
		// hashicorp/raft's RemoveServer treats an unknown ID as success — it
		// commits a no-op configuration entry and reports nil with the voter
		// set unchanged — so without this check every caller logs "removed"
		// while nothing happened, and whatever acts on that report (an
		// operator, a preStop hook, a reconciler) acts on a lie.
		return fmt.Errorf("%s: %w", id, ErrNodeNotInCluster)
	}
	// A removal can race this check; the loser's RemoveServer no-ops, which
	// is acceptable — the configuration ends up exactly where the caller
	// asked. The check exists to catch targets that were never there.
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
// commands on the leader node. The function returns the Raft log index at
// which the command was applied so followers can wait for their own FSM
// to catch up before reading post-mutation state.
func (s *Server) SetApplyFn(fn func(ctx context.Context, data []byte) (uint64, error)) {
	s.applyFn = fn
}

// SetGroupApplyFn sets the function used by ForwardVaultApply handlers to
// apply commands to a multiraft group on this node. Callers typically pass
// a closure that resolves groupID via the GroupManager and calls Apply on
// the resulting Raft instance, returning the applied log index. See
// wireClusterRaftApplies in app.go for the canonical wiring.
func (s *Server) SetGroupApplyFn(fn func(ctx context.Context, groupID string, data []byte) (uint64, error)) {
	s.groupApplyFn = fn
}

// SetEvictionHandler registers the callback invoked when this node receives
// a NotifyEviction RPC (i.e., it has been removed from the cluster).
func (s *Server) SetEvictionHandler(fn func()) {
	s.evictionHandler = fn
}

// RemovalPolicy names who asked for a node removal, which decides how
// the leader-side gates treat a removal that degrades redundancy
// without orphaning anything.
type RemovalPolicy int

const (
	// RemovalPolicyOperator is the `gastrolog cluster remove-node` path:
	// a deliberate topology change with a human on the other end. The
	// RF-preservation gate is pessimistic here — a removal that would
	// leave a vault below its replication factor with nowhere to
	// re-place is refused, and the operator decides (add an eligible
	// node, drain the vault, or re-run with --force).
	RemovalPolicyOperator RemovalPolicy = iota

	// RemovalPolicySelf is the preStop `gastrolog cluster demote-self`
	// path: the node is asking for its own removal while it is already
	// terminating. The RF-preservation gate is optimistic here — the pod
	// leaves either way, so refusing would only strand a voter in the
	// Raft configuration; placement reconcile re-places the vault once
	// an eligible node is available. Kubernetes cannot tell a rolling
	// restart from a scale-down, which is exactly why this path must not
	// refuse (see the project's no-auto-remove stance).
	RemovalPolicySelf
)

// String renders the policy for logs.
func (p RemovalPolicy) String() string {
	if p == RemovalPolicySelf {
		return "self"
	}
	return "operator"
}

// RemoveNodeOptions carries the caller's intent into the leader-side
// removal gates.
type RemoveNodeOptions struct {
	// Force bypasses every removal gate — the orphan-refusal gate and
	// the RF-preservation gate alike — acknowledging data loss / reduced
	// redundancy. Every bypass is logged loudly on the leader.
	Force bool

	// Policy distinguishes operator-driven removal from preStop
	// self-removal. Zero value is the pessimistic operator policy.
	Policy RemovalPolicy
}

// RemoveNodeFunc executes a node removal on the leader: gate
// evaluation, Raft membership change, FSM node-config cleanup, and the
// eviction notification.
type RemoveNodeFunc func(ctx context.Context, nodeID string, opts RemoveNodeOptions) error

// SetRemoveNodeFn registers the callback for the ForwardRemoveNode RPC.
// This is called on the leader to execute the removal gates, the Raft
// removal, and the eviction notification.
func (s *Server) SetRemoveNodeFn(fn RemoveNodeFunc) {
	s.removeNodeFn = fn
}

// SetNodeSuffrageFn registers the callback for the ForwardSetNodeSuffrage RPC.
// This is called on the leader to execute the Raft suffrage change.
func (s *Server) SetNodeSuffrageFn(fn func(ctx context.Context, nodeID, nodeAddr string, voter bool) error) {
	s.setNodeSuffrageFn = fn
}

// SetReplicaCatchupFn registers the callback for the RequestReplicaCatchup
// RPC. Called on the placement leader to fan out per-chunk pushes to the
// requesting follower via the existing replicateToFollower machinery.
// Returns the count of chunks for which a push was actually scheduled
// (after leader-side filtering of tombstoned / cloud-backed / locally-
// missing chunks).
func (s *Server) SetReplicaCatchupFn(fn func(ctx context.Context, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (int, error)) {
	s.replicaCatchupFn = fn
}

// Pause installs a gate that causes every subsequent gRPC handler on this
// server to block until Unpause is called. TCP connections remain accepted,
// streams stay open; only application-level progress halts. Intended for
// reliability tests that simulate SIGSTOPed peers; production code never
// calls this. Idempotent — calling Pause while already paused is a no-op.
func (s *Server) Pause() {
	s.pauseMu.Lock()
	defer s.pauseMu.Unlock()
	if s.pauseGate == nil {
		s.pauseGate = make(chan struct{})
	}
}

// Unpause releases any handlers blocked by a previous Pause and clears the
// gate. Idempotent — calling when not paused is a no-op.
func (s *Server) Unpause() {
	s.pauseMu.Lock()
	gate := s.pauseGate
	s.pauseGate = nil
	s.pauseMu.Unlock()
	if gate != nil {
		close(gate)
	}
}

// awaitPauseRelease blocks until the pause gate is cleared or ctx is done.
// Returns nil when released normally or when not paused. Returns ctx.Err if
// the caller's context fires before Unpause.
func (s *Server) awaitPauseRelease(ctx context.Context) error {
	s.pauseMu.Lock()
	gate := s.pauseGate
	s.pauseMu.Unlock()
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCtx.Done():
		return errShuttingDown
	}
}

// pauseUnaryInterceptor blocks the handler until Unpause is called.
func (s *Server) pauseUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.awaitPauseRelease(ctx); err != nil {
		return nil, err
	}
	if err := s.awaitSlowDown(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// pauseStreamInterceptor blocks the handler until Unpause is called.
func (s *Server) pauseStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.awaitPauseRelease(ss.Context()); err != nil {
		return err
	}
	if err := s.awaitSlowDown(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

// SlowDown configures a per-handler artificial latency. Every subsequent
// gRPC call on this server sleeps for d before dispatching to its
// handler. d=0 disables the effect. Used by reliability tests to catch
// regressions that surface under slowness but not full stop. Production
// never calls this.
func (s *Server) SlowDown(d time.Duration) {
	s.slowMu.Lock()
	s.slowDur = d
	s.slowMu.Unlock()
}

// awaitSlowDown sleeps for the configured slow-down duration, honoring
// ctx cancellation. Returns nil if no slow-down is set.
func (s *Server) awaitSlowDown(ctx context.Context) error {
	s.slowMu.Lock()
	d := s.slowDur
	s.slowMu.Unlock()
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCtx.Done():
		return errShuttingDown
	}
}

// Start creates the gRPC servers, registers services, and begins serving.
// The listener was already bound in New(). With TLS, inbound connections are
// demuxed by SNI onto separate service and raft server stacks on the same port.
func (s *Server) Start() error {
	if s.cfg.TLS != nil {
		return s.startWithLaneIsolation()
	}
	return s.startCombined()
}

func (s *Server) startCombined() error {
	opts := s.baseServerOpts(maxServiceLaneMsgBytes)
	s.grpcSrv = grpc.NewServer(opts...)
	s.tm.Register(s.grpcSrv)
	if s.raft != nil {
		leaderhealth.Setup(s.raft, s.grpcSrv, []string{"cluster"})
	}
	registerClusterService(s.grpcSrv, s)
	return s.serveListener(s.listener, s.grpcSrv, "cluster")
}

func (s *Server) startWithLaneIsolation() error {
	registry := multiraft.NewInboundLaneRegistry(s.listener.Addr())
	s.tm.SetInboundLaneRegistry(registry)
	s.sniDemux = newSNIDemuxListener(s.listener, registry)

	serviceOpts := s.baseServerOpts(maxServiceLaneMsgBytes)
	s.grpcSrv = grpc.NewServer(serviceOpts...)
	if s.raft != nil {
		leaderhealth.Setup(s.raft, s.grpcSrv, []string{"cluster"})
	}
	registerClusterService(s.grpcSrv, s)

	if err := s.serveListener(s.sniDemux.ServiceListener(), s.grpcSrv, "cluster-service"); err != nil {
		return err
	}
	return s.EnsureRaftGroupLane(ConfigGroupID)
}

// EnsureRaftGroupLane starts a dedicated inbound gRPC stack for one multiraft
// group. No-op in combined (non-TLS) mode where all groups share grpcSrv.
func (s *Server) EnsureRaftGroupLane(groupID string) error {
	if s.cfg.TLS == nil {
		return nil
	}
	s.raftLaneMu.Lock()
	defer s.raftLaneMu.Unlock()
	if s.raftGroupServers == nil {
		s.raftGroupServers = make(map[string]*grpc.Server)
	}
	if _, ok := s.raftGroupServers[groupID]; ok {
		return nil
	}
	reg := s.tm.InboundLanes()
	if reg == nil {
		return errors.New("cluster: inbound raft lane registry not configured")
	}
	ln := reg.Listener(groupID)
	// Register transport group state before serving inbound RPCs so demuxed
	// connections never hit dispatchRPC with an unregistered group.
	s.tm.GroupTransport(groupID)
	srv := grpc.NewServer(s.baseServerOpts(maxRaftLaneRecvBytes)...)
	s.tm.RegisterGroup(srv, groupID)
	s.raftGroupServers[groupID] = srv
	return s.serveListener(ln, srv, "cluster-raft-"+groupID)
}

// RemoveRaftGroupLane stops and removes the inbound gRPC stack for groupID.
func (s *Server) RemoveRaftGroupLane(groupID string) {
	s.raftLaneMu.Lock()
	srv := s.raftGroupServers[groupID]
	delete(s.raftGroupServers, groupID)
	s.raftLaneMu.Unlock()
	if srv != nil {
		s.gracefulStopServer(srv)
	}
	if reg := s.tm.InboundLanes(); reg != nil {
		reg.Remove(groupID)
	}
}

// baseServerOpts builds the options shared by every inbound gRPC stack on the
// cluster port. Under TLS every stack — service lane and raft lanes alike —
// gets the mTLS interceptors: hashicorp/raft authenticates no peer of its own,
// so without them an uncredentialed dial to a lane commits entries to the
// replicated FSM.
func (s *Server) baseServerOpts(maxRecv int) []grpc.ServerOption {
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(maxRecv))

	if s.cfg.TLS != nil {
		tlsCfg := s.cfg.TLS.ServerTLSConfig()
		opts = append(opts,
			grpc.Creds(credentials.NewTLS(tlsCfg)),
			grpc.ChainUnaryInterceptor(s.pauseUnaryInterceptor, s.mTLSUnaryInterceptor),
			grpc.ChainStreamInterceptor(s.pauseStreamInterceptor, s.mTLSStreamInterceptor),
		)
	} else {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(s.pauseUnaryInterceptor),
			grpc.ChainStreamInterceptor(s.pauseStreamInterceptor),
		)
	}

	if s.cfg.ByteMetrics != nil {
		opts = append(opts, grpc.StatsHandler(newServerStatsHandler(s.cfg.ByteMetrics)))
	}
	return opts
}

func (s *Server) serveListener(ln net.Listener, srv *grpc.Server, label string) error {
	s.logger.Info("cluster gRPC server starting", "lane", label, "addr", s.listener.Addr().String())
	go func() {
		if err := srv.Serve(ln); err != nil {
			s.logger.Error("cluster gRPC server error", "lane", label, "error", err)
		}
	}()
	return nil
}

// mTLSUnaryInterceptor enforces peer authority on all unary RPCs.
func (s *Server) mTLSUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.requirePeerAuthority(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// mTLSStreamInterceptor enforces peer authority on all streaming RPCs.
func (s *Server) mTLSStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.requirePeerAuthority(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

// requirePeerAuthority decides whether a caller may make this call.
//
// A certificate signed by the cluster CA is necessary but no longer
// sufficient: it says which node the caller claims to be, and the caller has
// to be a node this cluster currently has. That is what makes removing a node
// revoke it, rather than leaving a valid certificate loose until the whole
// cluster is reissued.
//
// Two calls sit before membership and are exempt in different ways. Enrol
// takes no certificate at all, because it exists to hand one out.
// RequestMembership takes the certificate but cannot require membership —
// the caller is asking for exactly that — so it is the one call a node holds
// a certificate for while still being outside the configuration.
func (s *Server) requirePeerAuthority(ctx context.Context, method string) error {
	if strings.HasSuffix(method, "/Enroll") {
		return nil
	}
	leaf, err := verifiedPeerLeaf(ctx)
	if err != nil {
		return err
	}
	if strings.HasSuffix(method, "/RequestMembership") {
		return nil
	}
	return s.requireCurrentMember(leaf)
}

// verifiedPeerLeaf returns the certificate the peer presented, once the TLS
// stack has verified it chains to the cluster CA.
func verifiedPeerLeaf(ctx context.Context) (*x509.Certificate, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer info")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no TLS info")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return nil, status.Error(codes.Unauthenticated, "client certificate required")
	}
	return tlsInfo.State.VerifiedChains[0][0], nil
}

// requireCurrentMember checks the node a certificate names against the
// configuration this node holds.
//
// An empty configuration means this node does not know the membership rather
// than that there is none: it has just started, or it is a joiner that has
// not replicated yet and whose peers must be able to reach it to deliver the
// configuration in the first place. Refusing there would make a new node
// unreachable by the very traffic that would populate it, so an unknown
// membership admits a valid certificate. A configuration this node does hold
// and that does not list the caller is a different statement, and refused.
func (s *Server) requireCurrentMember(leaf *x509.Certificate) error {
	if s.raft == nil {
		return nil
	}
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		// Same reasoning as an empty configuration: a node that cannot read
		// its own membership does not know it, and must not refuse traffic
		// on the strength of not knowing.
		return nil //nolint:nilerr // an unreadable configuration is unknown membership, which admits
	}
	servers := future.Configuration().Servers
	configured := make([]string, 0, len(servers))
	for _, srv := range servers {
		configured = append(configured, string(srv.ID))
	}
	return checkMembership(configured, leaf)
}

// checkMembership is the decision requireCurrentMember reads the
// configuration for. See there for why an empty configuration admits.
func checkMembership(configured []string, leaf *x509.Certificate) error {
	if len(configured) == 0 {
		return nil
	}
	nodeID := tlsutil.NodeIDFromCert(leaf)
	if nodeID == "" {
		return status.Error(codes.Unauthenticated, "client certificate names no node")
	}
	if slices.Contains(configured, nodeID) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "node %s is not a member of this cluster", nodeID)
}

// Stop gracefully stops the cluster gRPC server.
//
// The drain order matters:
//
//  1. stopCancel() fires the cluster server's shutdown context. Long-
//     running stream handlers (vault replication, stream forward records,
//     forward import records) that wrap their RecvMsg via recvOrShutdown
//     observe this immediately and return errShuttingDown → no error →
//     handler goroutine exits. Without this step those handlers block
//     in stream.RecvMsg() until the peer closes the stream — which
//     never happens during a planned cluster shutdown because peers are
//     also shutting down — and GracefulStop() waits the full fallback
//     timeout.
//
//  2. Close the SNI demuxer so no new inbound cluster connections arrive.
//
//  3. tm.BeginShutdown() closes shutdownCh and per-group doneCh. This
//     unblocks Raft handlers stuck in dispatchRPC on rpcChan while
//     keeping group entries registered (Unavailable, not NotFound).
//
//  4. grpcSrv.GracefulStop() drains in-flight handlers; raft lane
//     servers stop before group map cleanup.
//
//  5. tm.Close() drops registered groups.
//
//  6. peerConns.Close() tears down outbound peer connections.
//
// A 2-second fallback timeout remains as a last-resort safety net. If
// it ever fires in production, that is a signal to investigate a
// handler that doesn't observe stopCtx cancellation — the whole point
// of this ordering is that GracefulStop completes in milliseconds.
func (s *Server) Stop() {
	if s.grpcSrv == nil {
		return
	}

	// Step 1: signal long-running stream handlers to return cleanly.
	if s.stopCancel != nil {
		s.stopCancel()
	}

	if s.sniDemux != nil {
		_ = s.sniDemux.Close()
	}

	if s.tm != nil {
		s.tm.BeginShutdown()
	}

	s.gracefulStopServer(s.grpcSrv)
	s.raftLaneMu.Lock()
	raftServers := s.raftGroupServers
	s.raftGroupServers = nil
	s.raftLaneMu.Unlock()
	for groupID, srv := range raftServers {
		s.gracefulStopServer(srv)
		if reg := s.tm.InboundLanes(); reg != nil {
			reg.Remove(groupID)
		}
	}

	if s.tm != nil {
		_ = s.tm.Close()
	}

	if s.peerConns != nil {
		_ = s.peerConns.Close()
	}
}

func (s *Server) gracefulStopServer(srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.logger.Warn("cluster gRPC graceful stop timed out, forcing — a handler is not observing stopCtx")
		srv.Stop()
	}
}

// PrepareRejoin stops the cluster gRPC server and re-binds the listen port,
// returning a fresh transport for the new Raft instance. Because the leader
// health service captures the *raft.Raft pointer at registration time and
// gRPC does not support service re-registration, we must stop and restart the
// gRPC server.
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

// RegisterPeerObserver registers a channel to receive Raft PeerObservation
// events (peer added to / removed from the cluster configuration). Used by
// the peer-state cache to evict entries for permanently removed nodes
// without waiting for TTL expiry.
func (s *Server) RegisterPeerObserver(ch chan hraft.Observation) {
	if s.raft == nil {
		return
	}
	s.raft.RegisterObserver(hraft.NewObserver(ch, true, func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.PeerObservation)
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
