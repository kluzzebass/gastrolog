package server

import (
	"context"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// PeerStorageStatsProvider looks up a storage's disk-guard state from
// cluster peer broadcasts (gastrolog-3cobq4). Implemented by
// cluster.PeerState; nil in single-node mode. Only the node hosting a
// storage can statfs it, so this is the sole source of live state for
// storages this node doesn't own.
type PeerStorageStatsProvider interface {
	FindStorageState(storageID string) *apiv1.StorageState
}

// ListStorages returns every configured file storage cluster-wide, with
// live guard state (free/total, resolved thresholds, verdicts) merged in
// from the owning node's NodeStats broadcast — the entity-list analogue of
// ListVaults. NodeStorageConfigs (config, replicated to every node) is the
// identity source of truth; local storages read the orchestrator's own
// guard directly, remote storages read the peer broadcast cache.
func (s *SystemServer) ListStorages(
	ctx context.Context,
	req *connect.Request[apiv1.ListStoragesRequest],
) (*connect.Response[apiv1.ListStoragesResponse], error) {
	storages, err := s.allStorageStates(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.ListStoragesResponse{Storages: storages}), nil
}

// allStorageStates composes NodeStorageConfigs (identity) with live guard
// state from the local orchestrator or peer broadcasts. Placements are
// recomputed here from the current vault config on every call — never
// trusted from a possibly-stale cached copy embedded in a peer's last
// broadcast — so the placements list is always as fresh as config itself,
// consistent with the "config-derived server-side" requirement
// (gastrolog-3cobq4).
func (s *SystemServer) allStorageStates(ctx context.Context) ([]*apiv1.StorageState, error) {
	nscs, err := s.sysStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := s.sysStore.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodeNames := make(map[string]string, len(nodes))
	for _, n := range nodes {
		if n.Name != "" {
			nodeNames[n.ID.String()] = n.Name
		}
	}
	placements, err := s.placementsByStorage(ctx)
	if err != nil {
		return nil, err
	}

	local := make(map[string]*apiv1.StorageState)
	if s.orch != nil {
		for _, ss := range s.orch.StorageSnapshots() {
			local[ss.ID] = storageSnapshotToProto(ss, s.localNodeID)
		}
	}

	var out []*apiv1.StorageState
	for _, nsc := range nscs {
		nodeName := nodeNames[nsc.NodeID]
		if nodeName == "" {
			nodeName = nsc.NodeID
		}
		for _, fs := range nsc.FileStorages {
			sid := fs.ID.String()

			// local[sid] is built fresh by storageSnapshotToProto on every
			// call above — safe to mutate directly. A peer-provided state is
			// NOT: FindStorageState (cluster.PeerState in production) returns
			// the exact *StorageState living inside that peer's cached
			// broadcast, shared with every other concurrent WatchSystemStatus
			// subscriber / ListStorages call / GetClusterStatus read
			// marshaling the same object. Clone it before setting
			// PlacedVaultIds below — mutating the shared pointer in place
			// raced under concurrent access (gastrolog-3cobq4 review).
			state := local[sid]
			if state == nil && s.peerStorageStats != nil {
				if peer := s.peerStorageStats.FindStorageState(sid); peer != nil {
					state = proto.Clone(peer).(*apiv1.StorageState) //nolint:forcetypeassert // Clone(x) always returns the concrete type of x
				}
			}
			if state == nil {
				// No live sample yet — owning node down, or hasn't ticked
				// since this storage was added. Identity from config; live
				// fields honestly zero rather than fabricated (facts before
				// speculation, gastrolog-9akebz).
				state = &apiv1.StorageState{
					Id:             fs.ID.ToProto(),
					Name:           fs.Name,
					Path:           fs.Path,
					NodeName:       nodeName,
					NodeId:         []byte(nsc.NodeID),
					StorageClass:   fs.StorageClass,
					WarnExpr:       fs.DiskFreeWarn,
					FloorExpr:      fs.DiskFreeFloor,
					WarnInherited:  fs.DiskFreeWarn == "",
					FloorInherited: fs.DiskFreeFloor == "",
				}
			}
			state.PlacedVaultIds = glidsToProtoBytes(placements[sid])
			out = append(out, state)
		}
	}
	return out, nil
}

// placementsByStorage maps every LOCALLY-known storage ID to the file
// vault IDs with a config placement referencing it, across the whole
// cluster — the same config-derived shape refreshVaultDiskGuards computes
// per-node, recomputed here for the entity list so it never depends on a
// broadcast's staleness.
func (s *SystemServer) placementsByStorage(ctx context.Context) (map[string][]glid.GLID, error) {
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]glid.GLID)
	for _, vc := range vaults {
		if vc.Type != system.VaultTypeFile {
			continue
		}
		for _, p := range vc.Placements {
			out[p.StorageID] = append(out[p.StorageID], vc.ID)
		}
	}
	for sid := range out {
		sort.Slice(out[sid], func(i, j int) bool { return out[sid][i].String() < out[sid][j].String() })
	}
	return out, nil
}

// glidsToProtoBytes converts a vault ID slice to its wire form.
func glidsToProtoBytes(ids []glid.GLID) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.ToProto()
	}
	return out
}

// storageSnapshotToProto converts the orchestrator's local guard snapshot
// to the wire StorageState — the local-storage counterpart to what the
// stats collector builds for the NodeStats broadcast (gastrolog-3cobq4).
// nodeID is this responding node's own raw ID (every locally-hosted storage
// shares it — the guard only ever tracks THIS node's own volumes).
func storageSnapshotToProto(ss orchestrator.StorageSnapshot, nodeID string) *apiv1.StorageState {
	id, err := glid.Parse(ss.ID)
	if err != nil {
		return nil
	}
	state := &apiv1.StorageState{
		Id:             id.ToProto(),
		Name:           ss.Name,
		Path:           ss.Path,
		NodeName:       ss.Node,
		NodeId:         []byte(nodeID),
		StorageClass:   ss.StorageClass,
		WarnExpr:       ss.WarnExpr,
		FloorExpr:      ss.FloorExpr,
		WarnInherited:  ss.WarnExpr == "",
		FloorInherited: ss.FloorExpr == "",
		WarnBytes:      ss.WarnBytes,
		FloorBytes:     ss.FloorBytes,
		FreeBytes:      ss.FreeBytes,
		TotalBytes:     ss.TotalBytes,
		WarnVerdict:    ss.WarnVerdict,
		ProtectVerdict: ss.ProtectVerdict,
	}
	if !ss.SampledAt.IsZero() {
		state.SampledAt = timestamppb.New(ss.SampledAt)
	}
	return state
}
