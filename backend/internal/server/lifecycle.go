package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"
)

// Version is set at build time.
var Version = "dev"

// ClusterStatusProvider exposes Raft cluster topology. Implemented by
// cluster.Server; defined here at the consumer site to keep the dependency narrow.
type ClusterStatusProvider interface {
	LeaderInfo() (address string, id string)
	Servers() ([]cluster.RaftServer, error)
	LocalStats() map[string]string
	IsLeader() bool
	LeadershipTransfer() error
}

// NodeStatsProvider returns the latest stats for a given cluster node.
type NodeStatsProvider interface {
	Get(senderID string) *apiv1.NodeStats
}

// LifecycleServer implements the LifecycleService.
type LifecycleServer struct {
	orch              *orchestrator.Orchestrator
	startTime         time.Time
	shutdown          func(drain bool)
	cluster           ClusterStatusProvider
	cfgStore          system.Store
	nodeID            string
	clusterAddress    string
	peerStats         NodeStatsProvider
	localStats        func() *apiv1.NodeStats
	joinClusterFn     func(ctx context.Context, leaderAddr, joinToken string) error
	removeNodeFn      func(ctx context.Context, nodeID string, force bool) error
	setNodeSuffrageFn func(ctx context.Context, nodeID string, voter bool) error
	statsSignal       *notify.Signal         // fired by stats collector on each broadcast tick
	peerRouteStats    PeerRouteStatsProvider // for aggregating route stats across cluster
	listVaultsFn      func(ctx context.Context) []*apiv1.VaultInfo
	getStatsFn        func(ctx context.Context) *apiv1.GetStatsResponse
	logger            *slog.Logger
}

var _ gastrologv1connect.LifecycleServiceHandler = (*LifecycleServer)(nil)

// NewLifecycleServer creates a new LifecycleServer.
// The shutdown function is called when Shutdown is invoked with the drain flag.
func NewLifecycleServer(orch *orchestrator.Orchestrator, shutdown func(drain bool), cluster ClusterStatusProvider, cfgStore system.Store, nodeID string, clusterAddress string, peerStats NodeStatsProvider, localStats func() *apiv1.NodeStats, logger *slog.Logger) *LifecycleServer {
	return &LifecycleServer{
		orch:           orch,
		startTime:      time.Now(),
		shutdown:       shutdown,
		cluster:        cluster,
		cfgStore:       cfgStore,
		nodeID:         nodeID,
		clusterAddress: clusterAddress,
		peerStats:      peerStats,
		localStats:     localStats,
		logger:         compLifecycle.Apply(logging.Default(logger)),
	}
}

// SetJoinClusterFunc sets the callback for the JoinCluster RPC.
// Must be called before the server starts serving.
func (s *LifecycleServer) SetJoinClusterFunc(fn func(ctx context.Context, leaderAddr, joinToken string) error) {
	s.joinClusterFn = fn
}

// SetRemoveNodeFunc sets the callback for the RemoveNode RPC. The force
// flag bypasses the orphan-refusal gate (gastrolog-2ch9y).
func (s *LifecycleServer) SetRemoveNodeFunc(fn func(ctx context.Context, nodeID string, force bool) error) {
	s.removeNodeFn = fn
}

// SetNodeSuffrageFunc sets the callback for the SetNodeSuffrage RPC.
func (s *LifecycleServer) SetNodeSuffrageFunc(fn func(ctx context.Context, nodeID string, voter bool) error) {
	s.setNodeSuffrageFn = fn
}

// SetStatsSignal wires the notification signal for system status streaming.
func (s *LifecycleServer) SetStatsSignal(sig *notify.Signal) {
	s.statsSignal = sig
}

// SetPeerRouteStats wires the peer route stats provider for cluster aggregation.
func (s *LifecycleServer) SetPeerRouteStats(p PeerRouteStatsProvider) {
	s.peerRouteStats = p
}

// SetVaultFuncs wires vault data providers for the WatchSystemStatus stream.
func (s *LifecycleServer) SetVaultFuncs(listVaults func(ctx context.Context) []*apiv1.VaultInfo, getStats func(ctx context.Context) *apiv1.GetStatsResponse) {
	s.listVaultsFn = listVaults
	s.getStatsFn = getStats
}

// Health returns the server health status.
func (s *LifecycleServer) Health(
	ctx context.Context,
	req *connect.Request[apiv1.HealthRequest],
) (*connect.Response[apiv1.HealthResponse], error) {
	status := apiv1.Status_STATUS_HEALTHY
	if !s.orch.IsRunning() {
		status = apiv1.Status_STATUS_UNHEALTHY
	}

	return connect.NewResponse(&apiv1.HealthResponse{
		Status:              status,
		Version:             Version,
		UptimeSeconds:       int64(time.Since(s.startTime).Seconds()),
		IngestQueueDepth:    int64(s.orch.IngestQueueDepth()),
		IngestQueueCapacity: int64(s.orch.IngestQueueCapacity()),
	}), nil
}

// Shutdown initiates a graceful shutdown.
// If drain is true in the request, waits for in-flight requests to complete.
func (s *LifecycleServer) Shutdown(
	ctx context.Context,
	req *connect.Request[apiv1.ShutdownRequest],
) (*connect.Response[apiv1.ShutdownResponse], error) {
	if s.shutdown != nil {
		drain := req.Msg.Drain
		// Run shutdown in background so we can return the response
		go s.shutdown(drain)
	}
	return connect.NewResponse(&apiv1.ShutdownResponse{}), nil
}

// GetClusterStatus returns the current cluster topology and Raft state.
func (s *LifecycleServer) GetClusterStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetClusterStatusRequest],
) (*connect.Response[apiv1.GetClusterStatusResponse], error) {
	if s.cluster == nil {
		return connect.NewResponse(&apiv1.GetClusterStatusResponse{
			ClusterEnabled: false,
		}), nil
	}

	leaderAddr, leaderID := s.cluster.LeaderInfo()
	servers, err := s.cluster.Servers()
	if err != nil {
		return nil, errInternal(err)
	}

	// Build a node-config lookup keyed by ID so we can attach name +
	// lifecycle state to each ClusterNode.
	cfgByID := make(map[string]system.NodeConfig)
	if s.cfgStore != nil {
		if nodes, err := s.cfgStore.ListNodes(ctx); err == nil {
			for _, n := range nodes {
				cfgByID[n.ID.String()] = n
			}
		}
	}

	nodes := make([]*apiv1.ClusterNode, 0, len(servers))
	for _, srv := range servers {
		role := apiv1.ClusterNodeRole_CLUSTER_NODE_ROLE_FOLLOWER
		isLeader := srv.ID == leaderID
		if isLeader {
			role = apiv1.ClusterNodeRole_CLUSTER_NODE_ROLE_LEADER
		}

		var suffrage apiv1.ClusterNodeSuffrage
		switch srv.Suffrage {
		case "Voter":
			suffrage = apiv1.ClusterNodeSuffrage_CLUSTER_NODE_SUFFRAGE_VOTER
		case "Nonvoter":
			suffrage = apiv1.ClusterNodeSuffrage_CLUSTER_NODE_SUFFRAGE_NONVOTER
		case "Staging":
			suffrage = apiv1.ClusterNodeSuffrage_CLUSTER_NODE_SUFFRAGE_STAGING
		}

		cfg := cfgByID[srv.ID]
		node := &apiv1.ClusterNode{
			Id:       []byte(srv.ID),
			Name:     cfg.Name,
			Address:  srv.Address,
			Role:     role,
			Suffrage: suffrage,
			IsLeader: isLeader,
			State:    command.NodeStateToProto(cfg.EffectiveState()),
		}
		if !cfg.StateSince.IsZero() {
			node.StateSince = timestamppb.New(cfg.StateSince)
		}

		// Attach per-node stats: real-time for local, last broadcast for peers.
		if isLocal := srv.ID == s.nodeID; isLocal && s.localStats != nil {
			node.Stats = s.localStats()
		} else if s.peerStats != nil {
			node.Stats = s.peerStats.Get(srv.ID)
		}

		// Copy advertised addresses from stats onto the ClusterNode.
		if node.Stats != nil {
			node.ApiAddress = node.Stats.ApiAddress
			node.PprofAddress = node.Stats.PprofAddress
		}

		nodes = append(nodes, node)
	}

	// Use the local node's advertised Raft address (reachable by other hosts)
	// rather than the listen address (e.g. ":4566") which only works on localhost.
	clusterAddr := s.clusterAddress
	for _, srv := range servers {
		if srv.ID == s.nodeID && srv.Address != "" {
			clusterAddr = srv.Address
			break
		}
	}

	resp := &apiv1.GetClusterStatusResponse{
		ClusterEnabled: true,
		LeaderId:       []byte(leaderID),
		LeaderAddress:  leaderAddr,
		Nodes:          nodes,
		LocalStats:     buildRaftStats(s.cluster.LocalStats()),
		LocalNodeId:    []byte(s.nodeID),
		ClusterAddress: clusterAddr,
	}

	// Expose join token from the replicated config (available on all nodes).
	if sys, err := s.cfgStore.Load(ctx); err == nil && sys != nil && sys.Runtime.ClusterTLS != nil {
		resp.JoinToken = sys.Runtime.ClusterTLS.JoinToken
	}

	return connect.NewResponse(resp), nil
}

// SetNodeSuffrage promotes or demotes a node's voting status.
func (s *LifecycleServer) SetNodeSuffrage(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeSuffrageRequest],
) (*connect.Response[apiv1.SetNodeSuffrageResponse], error) {
	if s.setNodeSuffrageFn == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cluster not enabled"))
	}

	nodeID := string(req.Msg.NodeId)
	if nodeID == "" {
		return nil, errRequired("node_id")
	}

	if err := s.setNodeSuffrageFn(ctx, nodeID, req.Msg.Voter); err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.SetNodeSuffrageResponse{}), nil
}

// SetNodeState transitions a node between lifecycle states (see
// docs/node-lifecycle-design.md). Routes through cfgStore.SetNodeState
// which proposes the FSM command via Raft, replacing legality
// checks deterministically inside the apply path.
//
// Idempotent: re-applying the same state is a no-op success (per
// system.ValidateNodeStateTransition). Illegal transitions surface
// as FailedPrecondition so the CLI can distinguish operator-fixable
// errors from genuine internal failures.
func (s *LifecycleServer) SetNodeState(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeStateRequest],
) (*connect.Response[apiv1.SetNodeStateResponse], error) {
	if s.cfgStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cluster not enabled"))
	}
	rawID := req.Msg.GetNodeId()
	if len(rawID) == 0 {
		return nil, errRequired("node_id")
	}
	id, err := glid.ParseAny(string(rawID))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("parse node_id: %w", err))
	}
	state := command.NodeStateFromProto(req.Msg.GetState())
	if state == system.NodeStateUnknown {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("state is required (one of LIVE, UNREACHABLE, MAINTENANCE, DRAINING, DECOMMISSIONING)"))
	}
	if err := s.cfgStore.SetNodeState(ctx, id, state, time.Now()); err != nil {
		if strings.Contains(err.Error(), "illegal node state transition") || strings.Contains(err.Error(), "not found") {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.SetNodeStateResponse{}), nil
}

// buildRaftStats converts the raw Hashicorp Raft Stats() map into a typed proto message.
func buildRaftStats(m map[string]string) *apiv1.RaftStats {
	if m == nil {
		return nil
	}
	return &apiv1.RaftStats{
		State:             m["state"],
		Term:              parseUint64(m["term"]),
		LastLogIndex:      parseUint64(m["last_log_index"]),
		LastLogTerm:       parseUint64(m["last_log_term"]),
		CommitIndex:       parseUint64(m["commit_index"]),
		AppliedIndex:      parseUint64(m["applied_index"]),
		FsmPending:        parseUint64(m["fsm_pending"]),
		LastSnapshotIndex: parseUint64(m["last_snapshot_index"]),
		LastSnapshotTerm:  parseUint64(m["last_snapshot_term"]),
		LastContact:       m["last_contact"],
		NumPeers:          parseUint32(m["num_peers"]),
		ProtocolVersion:   parseUint32(m["protocol_version"]),
	}
}

func parseUint64(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func parseUint32(s string) uint32 {
	v, _ := strconv.ParseUint(s, 10, 32)
	return uint32(v)
}

// JoinCluster joins a running single-node server to an existing cluster.
func (s *LifecycleServer) JoinCluster(
	ctx context.Context,
	req *connect.Request[apiv1.JoinClusterRequest],
) (*connect.Response[apiv1.JoinClusterResponse], error) {
	if s.joinClusterFn == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("runtime cluster join not available"))
	}
	leaderAddr := req.Msg.LeaderAddress
	if leaderAddr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("leader_address is required"))
	}
	joinToken := req.Msg.JoinToken
	if joinToken == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("join_token is required"))
	}

	if err := s.joinClusterFn(ctx, leaderAddr, joinToken); err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.JoinClusterResponse{}), nil
}

// RemoveNode evicts a node from the cluster.
func (s *LifecycleServer) RemoveNode(
	ctx context.Context,
	req *connect.Request[apiv1.RemoveNodeRequest],
) (*connect.Response[apiv1.RemoveNodeResponse], error) {
	if s.removeNodeFn == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("node removal not available"))
	}
	nodeID := string(req.Msg.NodeId)
	if nodeID == "" {
		return nil, errRequired("node_id")
	}
	// gastrolog-24iv4: operator-driven removal must not accidentally
	// remove the node the operator is connected to (typo, wrong copy-
	// paste). preStop's `cluster demote-self` sets AllowSelf=true to
	// opt out of this guard — that path's whole purpose is self-removal.
	if nodeID == s.nodeID && !req.Msg.AllowSelf {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cannot remove self from cluster (set allow_self=true to override)"))
	}

	s.logger.Info("removing node from cluster", "node_id", nodeID, "force", req.Msg.Force)
	if err := s.removeNodeFn(ctx, nodeID, req.Msg.Force); err != nil {
		s.logger.Error("node removal failed", "node_id", nodeID, "error", err)
		// Orphan-refusal rejections are operator-correctable (drain
		// the vault, or re-run with --force). Surface as FailedPrecondition
		// so the CLI can treat them differently from genuine internal errors.
		if strings.Contains(err.Error(), "refusing to remove node") {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, errInternal(err)
	}
	s.logger.Info("node removed from cluster", "node_id", nodeID)
	return connect.NewResponse(&apiv1.RemoveNodeResponse{}), nil
}

// YieldLeadership asks this node to transfer Raft leadership to another
// voter if it currently holds leadership. Non-leaders return success
// with transferred=false. Used as a Kubernetes preStop hook so a pod
// about to terminate hands off leadership cleanly without removing
// itself from the voter set — the absence is then handled by normal
// Raft heartbeat timeout and the node rejoins as a known follower when
// it comes back up. See gastrolog-2yeie.
func (s *LifecycleServer) YieldLeadership(
	_ context.Context,
	_ *connect.Request[apiv1.YieldLeadershipRequest],
) (*connect.Response[apiv1.YieldLeadershipResponse], error) {
	if s.cluster == nil {
		// Single-node mode: nothing to yield, no error.
		return connect.NewResponse(&apiv1.YieldLeadershipResponse{Transferred: false}), nil
	}
	if !s.cluster.IsLeader() {
		s.logger.Info("YieldLeadership: not leader, no-op")
		return connect.NewResponse(&apiv1.YieldLeadershipResponse{Transferred: false}), nil
	}
	s.logger.Info("YieldLeadership: transferring leadership")
	if err := s.cluster.LeadershipTransfer(); err != nil {
		s.logger.Error("YieldLeadership: transfer failed", "error", err)
		return nil, errInternal(err)
	}
	s.logger.Info("YieldLeadership: leadership transferred")
	return connect.NewResponse(&apiv1.YieldLeadershipResponse{Transferred: true}), nil
}

// WatchSystemStatus streams combined system status whenever stats update.
// Replaces polling GetClusterStatus, Health, and GetRouteStats.
func (s *LifecycleServer) WatchSystemStatus(
	ctx context.Context,
	req *connect.Request[apiv1.WatchSystemStatusRequest],
	stream *connect.ServerStream[apiv1.WatchSystemStatusResponse],
) error {
	// Send initial snapshot immediately.
	if err := stream.Send(s.buildSystemStatus(ctx)); err != nil {
		return err
	}

	if s.statsSignal == nil {
		<-ctx.Done()
		return nil
	}

	for {
		ch := s.statsSignal.C()
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
			if err := stream.Send(s.buildSystemStatus(ctx)); err != nil {
				return err
			}
		}
	}
}

// buildSystemStatus assembles the combined system status response.
func (s *LifecycleServer) buildSystemStatus(ctx context.Context) *apiv1.WatchSystemStatusResponse {
	// Health.
	status := apiv1.Status_STATUS_HEALTHY
	if !s.orch.IsRunning() {
		status = apiv1.Status_STATUS_UNHEALTHY
	}
	health := &apiv1.HealthResponse{
		Status:              status,
		Version:             Version,
		UptimeSeconds:       int64(time.Since(s.startTime).Seconds()),
		IngestQueueDepth:    int64(s.orch.IngestQueueDepth()),
		IngestQueueCapacity: int64(s.orch.IngestQueueCapacity()),
	}

	// Cluster status — reuse the existing RPC logic.
	clusterResp, _ := s.GetClusterStatus(ctx, connect.NewRequest(&apiv1.GetClusterStatusRequest{}))
	var cluster *apiv1.GetClusterStatusResponse
	if clusterResp != nil {
		cluster = clusterResp.Msg
	}

	// Route stats.
	routeStats := s.buildRouteStats()

	var vaults []*apiv1.VaultInfo
	if s.listVaultsFn != nil {
		vaults = s.listVaultsFn(ctx)
	}
	var stats *apiv1.GetStatsResponse
	if s.getStatsFn != nil {
		stats = s.getStatsFn(ctx)
	}

	return &apiv1.WatchSystemStatusResponse{
		Cluster:       cluster,
		Health:        health,
		RouteStats:    routeStats,
		Vaults:        vaults,
		Stats:         stats,
		IngesterAlive: s.buildIngesterAlive(ctx),
	}
}

// buildIngesterAlive snapshots the FSM ingester-alive map for every configured
// ingester, intersected with the set of nodes currently in NodeStateLive.
// The inspector uses this to render per-card running/selected badges and
// per-node ingester filters without polling ListIngesters.
//
// gastrolog-2kzb4: the raw FSM alive map records "this node started this
// ingester" — a bit that stays set after a clean shutdown notification
// but lingers when a node goes Unreachable / Maintenance / Decommissioning
// or just crashes. Surfacing the raw map made the inspector report N/N
// healthy even when a node was clearly offline. The fix is to filter by
// NodeConfig.EffectiveState() == NodeStateLive at the server boundary,
// so every UI surface (this proto, downstream caches, defensive
// frontend filters) sees only nodes whose ingester is actually expected
// to be running right now.
func (s *LifecycleServer) buildIngesterAlive(ctx context.Context) []*apiv1.IngesterAlive {
	if s.cfgStore == nil {
		return nil
	}
	ingesters, err := s.cfgStore.ListIngesters(ctx)
	if err != nil {
		return nil
	}
	liveNodes := s.listLiveNodes(ctx)
	out := make([]*apiv1.IngesterAlive, 0, len(ingesters))
	for _, ing := range ingesters {
		alive, err := s.cfgStore.GetIngesterAlive(ctx, ing.ID)
		if err != nil {
			continue
		}
		filtered := make(map[string]bool, len(alive))
		for nodeID, a := range alive {
			if _, isLive := liveNodes[nodeID]; isLive {
				filtered[nodeID] = a
			}
		}
		out = append(out, &apiv1.IngesterAlive{
			Id:         ing.ID.ToProto(),
			NodeStatus: filtered,
		})
	}
	return out
}

// listLiveNodes returns the set of node IDs whose NodeConfig
// EffectiveState() is NodeStateLive. Returns an empty (non-nil)
// set on cfgStore failure so callers treat all nodes as
// non-live rather than fall through to the un-filtered map.
func (s *LifecycleServer) listLiveNodes(ctx context.Context) map[string]struct{} {
	out := make(map[string]struct{})
	if s.cfgStore == nil {
		return out
	}
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return out
	}
	for _, n := range nodes {
		if n.EffectiveState() == system.NodeStateLive {
			out[n.ID.String()] = struct{}{}
		}
	}
	return out
}

// buildRouteStats aggregates route statistics from local + peer sources.
func (s *LifecycleServer) buildRouteStats() *apiv1.GetRouteStatsResponse {
	rs := s.orch.GetRouteStats()
	totalIngested := rs.Ingested.Load()
	totalDropped := rs.Dropped.Load()
	totalRouted := rs.Routed.Load()
	filterActive := s.orch.IsFilterSetActive()

	vaultMap := make(map[string]*apiv1.VaultRouteStats)
	for vaultID, vs := range s.orch.VaultRouteStatsList() {
		vaultMap[vaultID.String()] = &apiv1.VaultRouteStats{
			VaultId:          vaultID.ToProto(),
			RecordsMatched:   vs.Matched.Load(),
			RecordsForwarded: vs.Forwarded.Load(),
		}
	}

	routeMap := make(map[string]*apiv1.PerRouteStats)
	for routeID, ps := range s.orch.PerRouteStatsList() {
		routeMap[routeID.String()] = &apiv1.PerRouteStats{
			RouteId:          routeID.ToProto(),
			RecordsMatched:   ps.Matched.Load(),
			RecordsForwarded: ps.Forwarded.Load(),
		}
	}

	if s.peerRouteStats != nil {
		pIngested, pDropped, pRouted, pFilterActive, pVaultStats, pRouteStats := s.peerRouteStats.AggregateRouteStats()
		totalIngested += pIngested
		totalDropped += pDropped
		totalRouted += pRouted
		if pFilterActive {
			filterActive = true
		}
		mergeVaultRouteStats(vaultMap, pVaultStats)
		mergePerRouteStats(routeMap, pRouteStats)
	}

	resp := &apiv1.GetRouteStatsResponse{
		TotalIngested:   totalIngested,
		TotalDropped:    totalDropped,
		TotalRouted:     totalRouted,
		FilterSetActive: filterActive,
	}
	for _, vs := range vaultMap {
		resp.VaultStats = append(resp.VaultStats, vs)
	}
	for _, rs := range routeMap {
		resp.RouteStats = append(resp.RouteStats, rs)
	}
	return resp
}
