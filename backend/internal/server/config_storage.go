package server

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

	return connect.NewResponse(&apiv1.PutCloudServiceResponse{Config: s.buildFullConfig(ctx)}), nil
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

	return connect.NewResponse(&apiv1.DeleteCloudServiceResponse{Config: s.buildFullConfig(ctx)}), nil
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

	// Assign UUIDs to areas that don't have one.
	for i := range cfg.Areas {
		if cfg.Areas[i].ID == uuid.Nil {
			cfg.Areas[i].ID = uuid.Must(uuid.NewV7())
		}
	}

	if err := s.cfgStore.SetNodeStorageConfig(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyNodeStorageConfigSet})

	return connect.NewResponse(&apiv1.SetNodeStorageConfigResponse{Config: s.buildFullConfig(ctx)}), nil
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
	if req.Msg.Config.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name required"))
	}

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Validate tier type.
	tierType := protoToTierType(req.Msg.Config.Type)
	if tierType == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("type must be memory, local, or cloud"))
	}

	// Reject duplicate names.
	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if connErr := checkNameConflict("tier", id, req.Msg.Config.Name, tiers, func(t config.TierConfig) (uuid.UUID, string) { return t.ID, t.Name }); connErr != nil {
		return nil, connErr
	}

	// For cloud tiers, validate the referenced cloud service exists.
	if tierType == config.TierTypeCloud {
		if req.Msg.Config.CloudServiceId == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cloud_service_id required for cloud tiers"))
		}
		csID, connErr := parseUUID(req.Msg.Config.CloudServiceId)
		if connErr != nil {
			return nil, connErr
		}
		cs, err := s.cfgStore.GetCloudService(ctx, csID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if cs == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("cloud service %q not found", req.Msg.Config.CloudServiceId))
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

	cfg, err := protoToTierConfig(req.Msg.Config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cfg.ID = id

	if err := s.cfgStore.PutTier(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierPut, ID: id})

	return connect.NewResponse(&apiv1.PutTierResponse{Config: s.buildFullConfig(ctx)}), nil
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

	// Referential integrity: reject if any vault references this tier.
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, v := range vaults {
		if slices.Contains(v.TierIDs, id) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("tier %q is referenced by vault %q", req.Msg.Id, v.ID))
		}
	}

	if err := s.cfgStore.DeleteTier(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: id})

	return connect.NewResponse(&apiv1.DeleteTierResponse{Config: s.buildFullConfig(ctx)}), nil
}

// --- Proto <-> Config conversion helpers for storage ---

func protoToCloudService(p *apiv1.CloudService) config.CloudService {
	return config.CloudService{
		Name:             p.Name,
		Provider:         p.Provider,
		Bucket:           p.Bucket,
		Region:           p.Region,
		Endpoint:         p.Endpoint,
		AccessKey:        p.AccessKey,
		SecretKey:        p.SecretKey,
		Container:        p.Container,
		ConnectionString: p.ConnectionString,
		CredentialsJSON:  p.CredentialsJson,
		StorageClass:     p.StorageClass,
		ActiveChunkClass: p.ActiveChunkClass,
		CacheClass:       p.CacheClass,
	}
}

func protoToNodeStorageConfig(p *apiv1.NodeStorageConfig) config.NodeStorageConfig {
	cfg := config.NodeStorageConfig{
		NodeID: p.NodeId,
	}
	for _, a := range p.Areas {
		area := config.StorageArea{
			StorageClass:      a.StorageClass,
			Name:              a.Name,
			Path:              a.Path,
			MemoryBudgetBytes: a.MemoryBudgetBytes,
		}
		if a.Id != "" {
			if id, err := uuid.Parse(a.Id); err == nil {
				area.ID = id
			}
		}
		cfg.Areas = append(cfg.Areas, area)
	}
	return cfg
}

func protoToTierType(t apiv1.TierType) config.TierType {
	switch t { //nolint:exhaustive // UNSPECIFIED handled by default
	case apiv1.TierType_TIER_TYPE_MEMORY:
		return config.TierTypeMemory
	case apiv1.TierType_TIER_TYPE_LOCAL:
		return config.TierTypeLocal
	case apiv1.TierType_TIER_TYPE_CLOUD:
		return config.TierTypeCloud
	default:
		return ""
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
		NodeID:            p.NodeId,
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
