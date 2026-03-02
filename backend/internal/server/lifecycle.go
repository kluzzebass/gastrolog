package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"
)

// Version is set at build time.
var Version = "dev"

// ClusterStatusProvider exposes Raft cluster topology. Implemented by
// cluster.Server; defined here at the consumer site to keep the dependency narrow.
type ClusterStatusProvider interface {
	LeaderInfo() (address string, id string)
	Servers() ([]cluster.RaftServer, error)
	LocalStats() map[string]string
	AddVoter(id, addr string, timeout time.Duration) error
	AddNonvoter(id, addr string, timeout time.Duration) error
	DemoteVoter(id string, timeout time.Duration) error
}

// NodeStatsProvider returns the latest stats for a given cluster node.
type NodeStatsProvider interface {
	Get(senderID string) *apiv1.NodeStats
}

// LifecycleServer implements the LifecycleService.
type LifecycleServer struct {
	orch       *orchestrator.Orchestrator
	startTime  time.Time
	shutdown   func(drain bool)
	cluster    ClusterStatusProvider
	cfgStore   config.Store
	nodeID     string
	peerStats  NodeStatsProvider
	localStats func() *apiv1.NodeStats
}

var _ gastrologv1connect.LifecycleServiceHandler = (*LifecycleServer)(nil)

// NewLifecycleServer creates a new LifecycleServer.
// The shutdown function is called when Shutdown is invoked with the drain flag.
func NewLifecycleServer(orch *orchestrator.Orchestrator, shutdown func(drain bool), cluster ClusterStatusProvider, cfgStore config.Store, nodeID string, peerStats NodeStatsProvider, localStats func() *apiv1.NodeStats) *LifecycleServer {
	return &LifecycleServer{
		orch:       orch,
		startTime:  time.Now(),
		shutdown:   shutdown,
		cluster:    cluster,
		cfgStore:   cfgStore,
		nodeID:     nodeID,
		peerStats:  peerStats,
		localStats: localStats,
	}
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
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Build a name lookup from the config store's node list.
	nameByID := make(map[string]string)
	if s.cfgStore != nil {
		if nodes, err := s.cfgStore.ListNodes(ctx); err == nil {
			for _, n := range nodes {
				nameByID[n.ID.String()] = n.Name
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

		node := &apiv1.ClusterNode{
			Id:       srv.ID,
			Name:     nameByID[srv.ID],
			Address:  srv.Address,
			Role:     role,
			Suffrage: suffrage,
			IsLeader: isLeader,
		}

		// Attach per-node stats: real-time for local, last broadcast for peers.
		if isLocal := srv.ID == s.nodeID; isLocal && s.localStats != nil {
			node.Stats = s.localStats()
		} else if s.peerStats != nil {
			node.Stats = s.peerStats.Get(srv.ID)
		}

		nodes = append(nodes, node)
	}

	return connect.NewResponse(&apiv1.GetClusterStatusResponse{
		ClusterEnabled: true,
		LeaderId:       leaderID,
		LeaderAddress:  leaderAddr,
		Nodes:          nodes,
		LocalStats:     buildRaftStats(s.cluster.LocalStats()),
		LocalNodeId:    s.nodeID,
	}), nil
}

// SetNodeSuffrage promotes or demotes a node's voting status.
func (s *LifecycleServer) SetNodeSuffrage(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeSuffrageRequest],
) (*connect.Response[apiv1.SetNodeSuffrageResponse], error) {
	if s.cluster == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cluster not enabled"))
	}

	nodeID := req.Msg.NodeId
	if nodeID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}

	// Look up the node's address from the current Raft configuration.
	servers, err := s.cluster.Servers()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	var nodeAddr string
	for _, srv := range servers {
		if srv.ID == nodeID {
			nodeAddr = srv.Address
			break
		}
	}
	if nodeAddr == "" {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("node %s not in cluster configuration", nodeID))
	}

	const timeout = 10 * time.Second
	if req.Msg.Voter {
		err = s.cluster.AddVoter(nodeID, nodeAddr, timeout)
	} else {
		err = s.cluster.DemoteVoter(nodeID, timeout)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.SetNodeSuffrageResponse{}), nil
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
