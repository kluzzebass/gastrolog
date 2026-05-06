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

	// Referential integrity: reject if any tier or vault references this
	// cloud service. VaultConfig.CloudServiceID is mirrored from TierConfig
	// (gastrolog-257l7); post-tier the vault check is the only one.
	tiers, err := s.sysStore.ListTiers(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, t := range tiers {
		if t.CloudServiceID != nil && *t.CloudServiceID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("cloud service %q is referenced by tier %q", req.Msg.Id, t.ID))
		}
	}
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

// --- Tiers ---

// PutTier creates or updates a tier.
func (s *SystemServer) PutTier(
	ctx context.Context,
	req *connect.Request[apiv1.PutTierRequest],
) (*connect.Response[apiv1.PutTierResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}
	id, connErr := parseProtoID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Validate tier type.
	tierType := convert.TierTypeFromProto(req.Msg.Config.Type)
	if tierType == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("type must be memory, file, or jsonl"))
	}

	// cloud_service_id is part of the tier's *shape*, not runtime tuning.
	// Mutating it on an existing tier would orphan cloud blobs in the
	// cloud→local direction and trigger an implicit cloud upload of all
	// existing local chunks in the local→cloud direction — neither is
	// safe to do silently. Reject any change to cloud_service_id on an
	// existing tier; users wanting to migrate must create a new tier and
	// route data via retention rules. Runs before the cloud-service
	// existence check so the user gets the right diagnostic. See
	// gastrolog-4k5mg.
	existing, err := s.sysStore.GetTier(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing != nil {
		var existingCSID, incomingCSID string
		if existing.CloudServiceID != nil {
			existingCSID = existing.CloudServiceID.String()
		}
		if len(req.Msg.Config.CloudServiceId) > 0 {
			incomingCSID = glid.FromBytes(req.Msg.Config.CloudServiceId).String()
		}
		if existingCSID != incomingCSID {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("cannot change cloud_service_id on an existing tier; create a new tier and migrate data via retention rules instead"))
		}
	}

	// Cloud-backed tiers (file tier with cloud_service_id set) need extra
	// validation. The legacy TIER_TYPE_CLOUD wire value is normalized to
	// TIER_TYPE_FILE by TierTypeFromProto; cloud-ness is signaled by the
	// presence of cloud_service_id (gastrolog-4k5mg).
	if tierType == system.TierTypeFile && len(req.Msg.Config.CloudServiceId) > 0 {
		if connErr := s.validateCloudTierFields(ctx, req.Msg.Config); connErr != nil {
			return nil, connErr
		}
	}

	// Validate referenced rotation policy exists (if set).
	if len(req.Msg.Config.RotationPolicyId) != 0 {
		rpID, connErr := parseProtoID(req.Msg.Config.RotationPolicyId)
		if connErr != nil {
			return nil, connErr
		}
		rp, err := s.sysStore.GetRotationPolicy(ctx, rpID)
		if err != nil {
			return nil, errInternal(err)
		}
		if rp == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("rotation policy %q not found", rpID))
		}
	}

	if err := s.validateReplicationFactor(ctx, tierType, req.Msg.Config); err != nil {
		return nil, err
	}

	cfg, err := convert.TierConfigFromProto(req.Msg.Config)
	if err != nil {
		return nil, errInvalidArg(err)
	}
	cfg.ID = id

	// Placements are system-managed (in Runtime, not Config).
	// PutTier only stores the config portion. Placements are
	// managed separately via SetTierPlacements.

	if err := s.sysStore.PutTier(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierPut, ID: id})

	if s.placementReconcile != nil {
		s.placementReconcile(ctx)
	}

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutTierResponse{System: fullCfg}), nil
}

// DeleteTier removes a tier.
func (s *SystemServer) DeleteTier(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteTierRequest],
) (*connect.Response[apiv1.DeleteTierResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	existing, err := s.sysStore.GetTier(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("tier not found"))
	}

	drain := req.Msg.GetDrain()

	if err := s.sysStore.DeleteTier(ctx, id, drain); err != nil {
		return nil, errInternal(err)
	}

	// Tier ownership now lives on TierConfig (VaultID field), so there is no
	// vault-side tier list to clean up. The tier config itself was already
	// deleted above by cfgStore.DeleteTier.

	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: id, Drain: drain})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteTierResponse{System: cfg}), nil
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
func (s *SystemServer) validateReplicationFactor(ctx context.Context, tierType system.TierType, p *apiv1.TierConfig) *connect.Error {
	if p.ReplicationFactor <= 1 {
		return nil
	}
	eligible, err := s.countEligibleStorages(ctx, tierType, p)
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
func (s *SystemServer) countEligibleStorages(ctx context.Context, tierType system.TierType, p *apiv1.TierConfig) (int, error) {
	nscs, err := s.sysStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return 0, err
	}
	nodes, err := s.sysStore.ListNodes(ctx)
	if err != nil {
		return 0, err
	}

	switch tierType {
	case system.TierTypeMemory:
		return len(nodes), nil // memory tiers: one per node (no disk storage)
	case system.TierTypeJSONL:
		return 1, nil // JSONL tiers are pinned to a single node
	case system.TierTypeFile:
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
