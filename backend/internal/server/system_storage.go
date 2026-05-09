package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// --- Cloud Services ---

// PutCloudService creates or updates a cloud service.
func (s *SystemServer) PutCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.PutCloudServiceRequest],
) (*connect.Response[apiv1.PutCloudServiceResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	id, connErr := parseProtoID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	services, err := s.sysStore.ListCloudServices(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("cloud service", id, req.Msg.Config.Name, services, func(cs system.CloudService) (glid.GLID, string) { return cs.ID, cs.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := convert.CloudServiceFromProto(req.Msg.Config)
	cfg.ID = id

	if err := s.sysStore.PutCloudService(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServicePut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutCloudServiceResponse{System: fullCfg}), nil
}

// DeleteCloudService removes a cloud service.
func (s *SystemServer) DeleteCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCloudServiceRequest],
) (*connect.Response[apiv1.DeleteCloudServiceResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	existing, err := s.sysStore.GetCloudService(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("cloud service not found"))
	}

	// Referential integrity: reject if any vault references this cloud service.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, v := range vaults {
		if v.CloudServiceID != nil && *v.CloudServiceID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("cloud service %q is referenced by vault %q", req.Msg.Id, v.ID))
		}
	}

	if err := s.sysStore.DeleteCloudService(ctx, id); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServiceDeleted, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteCloudServiceResponse{System: cfg}), nil
}

// --- Node Storage ---

// SetNodeStorageConfig creates or updates a node storage configuration.
func (s *SystemServer) SetNodeStorageConfig(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeStorageConfigRequest],
) (*connect.Response[apiv1.SetNodeStorageConfigResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.NodeId) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id required"))
	}

	cfg := convert.NodeStorageConfigFromProto(req.Msg.Config)

	// Assign UUIDs to file storages that don't have one.
	for i := range cfg.FileStorages {
		if cfg.FileStorages[i].ID == glid.Nil {
			cfg.FileStorages[i].ID = glid.New()
		}
	}

	if err := s.sysStore.SetNodeStorageConfig(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyNodeStorageConfigSet})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.SetNodeStorageConfigResponse{System: fullCfg}), nil
}

// --- Proto <-> Config conversion ---
//
// Canonical converters live in the convert package (gastrolog-2f8et).
// protoToCloudService, protoToNodeStorageConfig, protoToTierType were
// moved there as CloudServiceFromProto, NodeStorageConfigFromProto, and
// tierTypeFromProto respectively.

// validateCloudTierFields checks that a cloud tier has all required fields and
// that the referenced cloud service exists.
func (s *SystemServer) validateCloudTierFields(ctx context.Context, cfg *apiv1.TierConfig) *connect.Error {
	if len(cfg.CloudServiceId) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cloud_service_id required for cloud tiers"))
	}
	csID, connErr := parseProtoID(cfg.CloudServiceId)
	if connErr != nil {
		return connErr
	}
	cs, err := s.sysStore.GetCloudService(ctx, csID)
	if err != nil {
		return errInternal(err)
	}
	if cs == nil {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("cloud service %q not found", csID))
	}
	return nil
}

// validateReplicationFactor rejects RF higher than the number of eligible nodes.
func (s *SystemServer) validateReplicationFactor(ctx context.Context, vaultType system.VaultType, p *apiv1.TierConfig) *connect.Error {
	if p.ReplicationFactor <= 1 {
		return nil
	}
	eligible, err := s.countEligibleStorages(ctx, vaultType, p)
	if err != nil {
		return errInternal(err)
	}
	if int(p.ReplicationFactor) > eligible {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("replication factor %d exceeds eligible file storages (%d with required storage class)", p.ReplicationFactor, eligible))
	}
	return nil
}

// given type with the given storage class requirements.
// countEligibleStorages returns how many file storages can host a replica of
// this tier type. Same-node replication is valid (different file storages on the
// same node), so this counts file storages, not nodes.
func (s *SystemServer) countEligibleStorages(ctx context.Context, vaultType system.VaultType, p *apiv1.TierConfig) (int, error) {
	nscs, err := s.sysStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return 0, err
	}
	nodes, err := s.sysStore.ListNodes(ctx)
	if err != nil {
		return 0, err
	}

	switch vaultType {
	case system.VaultTypeMemory:
		return len(nodes), nil // memory tiers: one per node (no disk storage)
	case system.VaultTypeJSONL:
		return 1, nil // JSONL tiers are pinned to a single node
	case system.VaultTypeFile:
		// Single storage class for both local-only and cloud-backed
		// file tiers. See gastrolog-4k5mg.
		requiredClass := p.GetStorageClass()
		count := 0
		for _, nsc := range nscs {
			for _, fs := range nsc.FileStorages {
				if fs.StorageClass == requiredClass {
					count++
				}
			}
		}
		return count, nil
	default:
		return len(nodes), nil
	}
}

// protoToTierConfig was here — now lives in convert.TierConfigFromProto.
