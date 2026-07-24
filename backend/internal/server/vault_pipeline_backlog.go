package server

import (
	"context"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// RemotePipelineBacklogGetter fetches on-disk segment counts from a peer node.
type RemotePipelineBacklogGetter interface {
	GetPipelineBacklogDisk(ctx context.Context, nodeID string, req *apiv1.ForwardGetPipelineBacklogRequest) (*apiv1.ForwardGetPipelineBacklogResponse, error)
}

// PeerPipelineDiskProvider aggregates pipeline disk counts from peer broadcasts.
type PeerPipelineDiskProvider interface {
	AggregatePipelineDisk() map[glid.GLID][]cluster.PeerVaultPipelineDisk
}

// GetPipelineBacklog returns vault-ctl registry/manifest state and
// cluster-wide on-disk segment counts. Prefer WatchSystemStatus for the
// inspector; this RPC remains for on-demand/CLI use.
func (s *VaultServer) GetPipelineBacklog(
	ctx context.Context,
	req *connect.Request[apiv1.GetPipelineBacklogRequest],
) (*connect.Response[apiv1.GetPipelineBacklogResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	snap, err := s.assemblePipelineBacklogRPC(ctx, vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	return connect.NewResponse(&apiv1.GetPipelineBacklogResponse{
		Backlog: pipelineBacklogToProto(snap),
	}), nil
}

func (s *VaultServer) assemblePipelineBacklogRPC(ctx context.Context, vaultID glid.GLID) (orchestrator.PipelineBacklogSnapshot, error) {
	var peerDisk []cluster.PeerVaultPipelineDisk
	if s.remotePipelineBacklog != nil {
		remoteNodes := s.remoteClusterNodes(ctx)
		results, ok, _ := peerFanOut(ctx, s.logger, "GetPipelineBacklog", remoteNodes,
			func(peerCtx context.Context, nodeID string) (*apiv1.ForwardGetPipelineBacklogResponse, error) {
				return s.remotePipelineBacklog.GetPipelineBacklogDisk(peerCtx, nodeID, &apiv1.ForwardGetPipelineBacklogRequest{
					VaultId: vaultID.ToProto(),
				})
			})
		for i, remote := range results {
			if !ok[i] || remote == nil {
				continue
			}
			disk := orchestratorDiskFromProto(remote)
			peerDisk = append(peerDisk, cluster.PeerVaultPipelineDisk{
				NodeID:                remoteNodes[i],
				Working:               disk.Working,
				CompletedStaging:      disk.CompletedStaging,
				Head:                  disk.Head,
				PreHead:               disk.PreHead,
				WorkingBytes:          disk.WorkingBytes,
				CompletedStagingBytes: disk.CompletedStagingBytes,
				HeadBytes:             disk.HeadBytes,
				PreHeadBytes:          disk.PreHeadBytes,
			})
		}
	}
	return AssemblePipelineBacklog(s.orch, vaultID, s.localNodeID, peerDisk)
}

func orchestratorDiskFromProto(p *apiv1.ForwardGetPipelineBacklogResponse) orchestrator.PipelineDiskSegmentCounts {
	if p == nil {
		return orchestrator.PipelineDiskSegmentCounts{}
	}
	return orchestrator.PipelineDiskSegmentCounts{
		Working:               int(p.GetWorkingSegments()),
		CompletedStaging:      int(p.GetCompletedStagingSegments()),
		Head:                  int(p.GetHeadSegments()),
		PreHead:               int(p.GetPreHeadSegments()),
		WorkingBytes:          int64(p.GetWorkingBytes()),          //nolint:gosec
		CompletedStagingBytes: int64(p.GetCompletedStagingBytes()), //nolint:gosec
		HeadBytes:             int64(p.GetHeadBytes()),             //nolint:gosec
		PreHeadBytes:          int64(p.GetPreHeadBytes()),          //nolint:gosec
	}
}

func pipelineBacklogToProto(snap orchestrator.PipelineBacklogSnapshot) *apiv1.VaultPipelineBacklog {
	pb := &apiv1.VaultPipelineBacklog{
		VaultId:                       snap.VaultID.ToProto(),
		RegistrySegments:              snap.RegistrySegments,
		EligibleSegments:              snap.EligibleSegments,
		RegistryRecords:               snap.RegistryRecords,
		OpenManifestRefs:              snap.OpenManifestRefs,
		OpenManifestRecords:           snap.OpenManifestRecords,
		SealedManifestPending:         snap.SealedManifestPending,
		WorkingSegments:               uint32(snap.Working),               //nolint:gosec
		CompletedStagingSegments:      uint32(snap.CompletedStaging),      //nolint:gosec
		HeadSegments:                  uint32(snap.Head),                  //nolint:gosec
		PreHeadSegments:               uint32(snap.PreHead),               //nolint:gosec
		WorkingBytes:                  uint64(snap.WorkingBytes),          //nolint:gosec
		CompletedStagingBytes:         uint64(snap.CompletedStagingBytes), //nolint:gosec
		HeadBytes:                     uint64(snap.HeadBytes),             //nolint:gosec
		PreHeadBytes:                  uint64(snap.PreHeadBytes),          //nolint:gosec
		ConnectedNodeIsVaultCtlLeader: snap.ConnectedNodeIsVaultCtlLeader,
	}
	if !snap.OpenManifestIngestEnd.IsZero() {
		pb.OpenManifestIngestEnd = timestamppb.New(snap.OpenManifestIngestEnd)
	}
	if !snap.OldestEligibleLastIngest.IsZero() {
		pb.OldestEligibleLastIngest = timestamppb.New(snap.OldestEligibleLastIngest)
	}
	if snap.VaultCtlLeaderNodeID != "" {
		if id, err := glid.ParseAny(snap.VaultCtlLeaderNodeID); err == nil {
			pb.VaultCtlLeaderNodeId = id.ToProto()
		}
	}
	for _, ns := range snap.NodeSegments {
		nodeID, err := glid.ParseAny(ns.NodeID)
		if err != nil {
			continue
		}
		pb.NodeSegments = append(pb.NodeSegments, &apiv1.PipelineNodeSegments{
			NodeId:                   nodeID.ToProto(),
			WorkingSegments:          uint32(ns.Working),               //nolint:gosec
			CompletedStagingSegments: uint32(ns.CompletedStaging),      //nolint:gosec
			HeadSegments:             uint32(ns.Head),                  //nolint:gosec
			PreHeadSegments:          uint32(ns.PreHead),               //nolint:gosec
			WorkingBytes:             uint64(ns.WorkingBytes),          //nolint:gosec
			CompletedStagingBytes:    uint64(ns.CompletedStagingBytes), //nolint:gosec
			HeadBytes:                uint64(ns.HeadBytes),             //nolint:gosec
			PreHeadBytes:             uint64(ns.PreHeadBytes),          //nolint:gosec
		})
	}
	return pb
}

// BuildAllPipelineBacklogs assembles pipeline backlog for every registered vault
// using local FSM state and peer NodeStats broadcasts.
func BuildAllPipelineBacklogs(
	orch *orchestrator.Orchestrator,
	localNodeID string,
	peerPipelineDisk PeerPipelineDiskProvider,
) ([]*apiv1.VaultPipelineBacklog, error) {
	var peerByVault map[glid.GLID][]cluster.PeerVaultPipelineDisk
	if peerPipelineDisk != nil {
		peerByVault = peerPipelineDisk.AggregatePipelineDisk()
	}

	vaultIDs := orch.ListVaults()
	sort.Slice(vaultIDs, func(i, j int) bool {
		return vaultIDs[i].String() < vaultIDs[j].String()
	})

	out := make([]*apiv1.VaultPipelineBacklog, 0, len(vaultIDs))
	for _, vaultID := range vaultIDs {
		snap, err := AssemblePipelineBacklog(orch, vaultID, localNodeID, peerByVault[vaultID])
		if err != nil {
			return nil, err
		}
		out = append(out, pipelineBacklogToProto(snap))
	}
	return out, nil
}

// remoteClusterNodes returns every configured cluster node except the local one.
func (s *VaultServer) remoteClusterNodes(ctx context.Context) []string {
	if s.cfgStore == nil {
		return nil
	}
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return nil
	}
	var remotes []string
	for _, n := range nodes {
		id := n.ID.String()
		if id != s.localNodeID {
			remotes = append(remotes, id)
		}
	}
	return remotes
}
