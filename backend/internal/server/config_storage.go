package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
)

// --- Cloud Services ---

// PutCloudService creates or updates a cloud service.
func (s *ConfigServer) PutCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.PutCloudServiceRequest],
) (*connect.Response[apiv1.PutCloudServiceResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name required"))
	}

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	services, err := s.cfgStore.ListCloudServices(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if connErr := checkNameConflict("cloud service", id, req.Msg.Config.Name, services, func(cs config.CloudService) (uuid.UUID, string) { return cs.ID, cs.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := protoToCloudService(req.Msg.Config)
	cfg.ID = id

	if err := s.cfgStore.PutCloudService(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServicePut, ID: id})

	fullCfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.PutCloudServiceResponse{Config: fullCfg}), nil
}

// DeleteCloudService removes a cloud service.
func (s *ConfigServer) DeleteCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCloudServiceRequest],
) (*connect.Response[apiv1.DeleteCloudServiceResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	existing, err := s.cfgStore.GetCloudService(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("cloud service not found"))
	}

	// Referential integrity: reject if any tier references this cloud service.
	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, t := range tiers {
		if t.CloudServiceID != nil && *t.CloudServiceID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("cloud service %q is referenced by tier %q", req.Msg.Id, t.ID))
		}
	}

	if err := s.cfgStore.DeleteCloudService(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServiceDeleted, ID: id})

	cfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.DeleteCloudServiceResponse{Config: cfg}), nil
}

// --- Node Storage ---

// SetNodeStorageConfig creates or updates a node storage configuration.
func (s *ConfigServer) SetNodeStorageConfig(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeStorageConfigRequest],
) (*connect.Response[apiv1.SetNodeStorageConfigResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.NodeId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id required"))
	}

	cfg := protoToNodeStorageConfig(req.Msg.Config)

	// Assign UUIDs to file storages that don't have one.
	for i := range cfg.FileStorages {
		if cfg.FileStorages[i].ID == uuid.Nil {
			cfg.FileStorages[i].ID = uuid.Must(uuid.NewV7())
		}
	}

	if err := s.cfgStore.SetNodeStorageConfig(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyNodeStorageConfigSet})

	fullCfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.SetNodeStorageConfigResponse{Config: fullCfg}), nil
}

// --- Tiers ---

// PutTier creates or updates a tier.
func (s *ConfigServer) PutTier(
	ctx context.Context,
	req *connect.Request[apiv1.PutTierRequest],
) (*connect.Response[apiv1.PutTierResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Validate tier type.
	tierType := protoToTierType(req.Msg.Config.Type)
	if tierType == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("type must be memory, file, cloud, or jsonl"))
	}

	// For cloud tiers, validate required fields.
	if tierType == config.TierTypeCloud {
		if connErr := s.validateCloudTierFields(ctx, req.Msg.Config); connErr != nil {
			return nil, connErr
		}
	}

	// Validate referenced rotation policy exists (if set).
	if req.Msg.Config.RotationPolicyId != "" {
		rpID, connErr := parseUUID(req.Msg.Config.RotationPolicyId)
		if connErr != nil {
			return nil, connErr
		}
		rp, err := s.cfgStore.GetRotationPolicy(ctx, rpID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if rp == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("rotation policy %q not found", req.Msg.Config.RotationPolicyId))
		}
	}

	if err := s.validateReplicationFactor(ctx, tierType, req.Msg.Config); err != nil {
		return nil, err
	}

	cfg, err := protoToTierConfig(req.Msg.Config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cfg.ID = id

	// Placements are system-managed by the placement manager.
	// Preserve existing placements on updates; leave empty on create.
	cfg.Placements = nil
	if existing, _ := s.cfgStore.GetTier(ctx, id); existing != nil {
		cfg.Placements = existing.Placements
	}

	if err := s.cfgStore.PutTier(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierPut, ID: id})

	if s.placementReconcile != nil {
		s.placementReconcile(ctx)
	}

	fullCfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.PutTierResponse{Config: fullCfg}), nil
}

// DeleteTier removes a tier.
func (s *ConfigServer) DeleteTier(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteTierRequest],
) (*connect.Response[apiv1.DeleteTierResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	existing, err := s.cfgStore.GetTier(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("tier not found"))
	}

	drain := req.Msg.GetDrain()

	if err := s.cfgStore.DeleteTier(ctx, id, drain); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Tier ownership now lives on TierConfig (VaultID field), so there is no
	// vault-side tier list to clean up. The tier config itself was already
	// deleted above by cfgStore.DeleteTier.

	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: id, Drain: drain})

	cfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.DeleteTierResponse{Config: cfg}), nil
}

// --- Proto <-> Config conversion helpers for storage ---

func protoToCloudService(p *apiv1.CloudService) config.CloudService {
	transitions := make([]config.CloudStorageTransition, len(p.Transitions))
	for i, t := range p.Transitions {
		transitions[i] = config.CloudStorageTransition{
			After:        t.After,
			StorageClass: t.StorageClass,
		}
	}
	return config.CloudService{
		Name:              p.Name,
		Provider:          p.Provider,
		Bucket:            p.Bucket,
		Region:            p.Region,
		Endpoint:          p.Endpoint,
		AccessKey:         p.AccessKey,
		SecretKey:         p.SecretKey,
		Container:         p.Container,
		ConnectionString:  p.ConnectionString,
		CredentialsJSON:   p.CredentialsJson,
		StorageClass:      p.StorageClass,
		ArchivalMode:      p.ArchivalMode,
		Transitions:       transitions,
		RestoreTier:       p.RestoreTier,
		RestoreDays:       p.RestoreDays,
		SuspectGraceDays:  p.SuspectGraceDays,
		ReconcileSchedule: p.ReconcileSchedule,
	}
}

func protoToNodeStorageConfig(p *apiv1.NodeStorageConfig) config.NodeStorageConfig {
	cfg := config.NodeStorageConfig{
		NodeID: p.NodeId,
	}
	for _, a := range p.FileStorages {
		fs := config.FileStorage{
			StorageClass:      a.StorageClass,
			Name:              a.Name,
			Path:              a.Path,
			MemoryBudgetBytes: a.MemoryBudgetBytes,
		}
		if a.Id != "" {
			if id, err := uuid.Parse(a.Id); err == nil {
				fs.ID = id
			}
		}
		cfg.FileStorages = append(cfg.FileStorages, fs)
	}
	return cfg
}

func protoToTierType(t apiv1.TierType) config.TierType {
	switch t { //nolint:exhaustive // UNSPECIFIED handled by default
	case apiv1.TierType_TIER_TYPE_MEMORY:
		return config.TierTypeMemory
	case apiv1.TierType_TIER_TYPE_FILE:
		return config.TierTypeFile
	case apiv1.TierType_TIER_TYPE_CLOUD:
		return config.TierTypeCloud
	case apiv1.TierType_TIER_TYPE_JSONL:
		return config.TierTypeJSONL
	default:
		return ""
	}
}

// validateCloudTierFields checks that a cloud tier has all required fields and
// that the referenced cloud service exists.
func (s *ConfigServer) validateCloudTierFields(ctx context.Context, cfg *apiv1.TierConfig) *connect.Error {
	if cfg.CloudServiceId == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cloud_service_id required for cloud tiers"))
	}
	if cfg.ActiveChunkClass == 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("active_chunk_class required for cloud tiers"))
	}
	if cfg.CacheClass == 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cache_class required for cloud tiers"))
	}
	csID, connErr := parseUUID(cfg.CloudServiceId)
	if connErr != nil {
		return connErr
	}
	cs, err := s.cfgStore.GetCloudService(ctx, csID)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if cs == nil {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("cloud service %q not found", cfg.CloudServiceId))
	}
	return nil
}

// validateReplicationFactor rejects RF higher than the number of eligible nodes.
func (s *ConfigServer) validateReplicationFactor(ctx context.Context, tierType config.TierType, p *apiv1.TierConfig) *connect.Error {
	if p.ReplicationFactor <= 1 {
		return nil
	}
	eligible, err := s.countEligibleStorages(ctx, tierType, p)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
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
func (s *ConfigServer) countEligibleStorages(ctx context.Context, tierType config.TierType, p *apiv1.TierConfig) (int, error) {
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return 0, err
	}
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return 0, err
	}

	switch tierType {
	case config.TierTypeMemory:
		return len(nodes), nil // memory tiers: one per node (no disk storage)
	case config.TierTypeJSONL:
		return 1, nil // JSONL tiers are pinned to a single node
	case config.TierTypeFile:
		count := 0
		for _, nsc := range nscs {
			for _, fs := range nsc.FileStorages {
				if fs.StorageClass == p.StorageClass {
					count++
				}
			}
		}
		return count, nil
	case config.TierTypeCloud:
		count := 0
		for _, nsc := range nscs {
			for _, fs := range nsc.FileStorages {
				if fs.StorageClass == p.ActiveChunkClass {
					count++
				}
			}
		}
		return count, nil
	default:
		return len(nodes), nil
	}
}

func protoToTierConfig(p *apiv1.TierConfig) (config.TierConfig, error) {
	cfg := config.TierConfig{
		Name:              p.Name,
		Type:              protoToTierType(p.Type),
		MemoryBudgetBytes: p.MemoryBudgetBytes,
		StorageClass:      p.StorageClass,
		ActiveChunkClass:  p.ActiveChunkClass,
		CacheClass:        p.CacheClass,
		ReplicationFactor: p.ReplicationFactor,
		Path:              p.Path,
		Position:          p.Position,
		CacheEviction:     p.CacheEviction,
		CacheBudget:  p.CacheBudget,
		CacheTTL:          p.CacheTtl,
	}

	if p.VaultId != "" {
		vaultID, err := uuid.Parse(p.VaultId)
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid vault_id: %w", err)
		}
		cfg.VaultID = vaultID
	}

	if p.RotationPolicyId != "" {
		rpID, err := uuid.Parse(p.RotationPolicyId)
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid rotation_policy_id: %w", err)
		}
		cfg.RotationPolicyID = &rpID
	}

	if p.CloudServiceId != "" {
		csID, err := uuid.Parse(p.CloudServiceId)
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid cloud_service_id: %w", err)
		}
		cfg.CloudServiceID = &csID
	}

	for _, r := range p.RetentionRules {
		rpID, err := uuid.Parse(r.RetentionPolicyId)
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid retention_policy_id in rule: %w", err)
		}
		rule := config.RetentionRule{
			RetentionPolicyID: rpID,
			Action:            config.RetentionAction(r.Action),
		}
		for _, eid := range r.EjectRouteIds {
			routeID, err := uuid.Parse(eid)
			if err != nil {
				return config.TierConfig{}, fmt.Errorf("invalid eject_route_id: %w", err)
			}
			rule.EjectRouteIDs = append(rule.EjectRouteIDs, routeID)
		}
		cfg.RetentionRules = append(cfg.RetentionRules, rule)
	}

	return cfg, nil
}
