package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// ReconcileCloudIndex rebuilds every homing node's cloud index for a vault from
// the blob store.
//
// Fans out because the cloud index is per-node: repairing only where the request
// landed would leave the other nodes' caches wrong while reporting success.
func (s *VaultServer) ReconcileCloudIndex(
	ctx context.Context,
	req *connect.Request[apiv1.ReconcileCloudIndexRequest],
) (*connect.Response[apiv1.ReconcileCloudIndexResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	resp := &apiv1.ReconcileCloudIndexResponse{}
	local, err := s.orch.ReconcileVaultCloudIndex(ctx, vaultID)
	switch {
	case err == nil:
		resp.Repairs = append(resp.Repairs, cloudIndexRepairToProto(local, s.localNodeID))
	case errors.Is(err, chunk.ErrCloudStoreNotConfigured):
		// A local-only vault, or one this node does not home. Not an error: the
		// peers may still have work, and the empty result says so.
	case errors.Is(err, orchestrator.ErrVaultNotFound):
	default:
		return nil, mapVaultError(err)
	}

	if s.remoteCloudIndexReconciler != nil {
		s.mergeRemoteReconciles(ctx, vaultID, resp)
	}
	return connect.NewResponse(resp), nil
}

// mergeRemoteReconciles asks every peer that homes the vault to repair its own
// cloud index and folds the results in, attributing each to its node.
func (s *VaultServer) mergeRemoteReconciles(ctx context.Context, vaultID glid.GLID, resp *apiv1.ReconcileCloudIndexResponse) {
	remoteNodes := s.remoteVaultNodes(ctx, vaultID)
	results, ok, report := peerFanOut(ctx, s.logger, "ReconcileCloudIndex", remoteNodes,
		func(peerCtx context.Context, nodeID string) (*apiv1.ForwardReconcileCloudIndexResponse, error) {
			return s.remoteCloudIndexReconciler.ReconcileCloudIndex(peerCtx, nodeID,
				&apiv1.ForwardReconcileCloudIndexRequest{VaultId: vaultID.ToProto()})
		})
	resp.ContributionReport = report
	for i, remote := range results {
		if !ok[i] || remote == nil || remote.GetRepair() == nil {
			continue
		}
		repair := remote.GetRepair()
		if repair.GetNodeId() == "" {
			repair.NodeId = remoteNodes[i]
		}
		resp.Repairs = append(resp.Repairs, repair)
	}
}

func cloudIndexRepairToProto(r chunk.CloudIndexRepair, nodeID string) *apiv1.CloudIndexRepair {
	return &apiv1.CloudIndexRepair{
		NodeId:         nodeID,
		RemovedEntries: int64(r.RemovedEntries),
		CorrectedSizes: int64(r.CorrectedSizes),
		IndexedBlobs:   int64(r.IndexedBlobs),
	}
}

// CloudIndexRepairToProto renders a repair result for the cluster forward path.
// Node attribution is stamped by the requester from the peer it asked.
func CloudIndexRepairToProto(r chunk.CloudIndexRepair) *apiv1.CloudIndexRepair {
	return cloudIndexRepairToProto(r, "")
}
