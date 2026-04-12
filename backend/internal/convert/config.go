package convert

// config.go provides canonical converters between config domain types and
// their protobuf representations for CloudService, NodeStorageConfig, and
// TierConfig. Both the server RPC handlers and the Raft FSM command
// package call these functions — there is exactly one source of truth for
// each field mapping. See gastrolog-2f8et.

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// CloudService
// ---------------------------------------------------------------------------

// CloudServiceToProto converts a config.CloudService to its proto representation.
func CloudServiceToProto(cs config.CloudService) *gastrologv1.CloudService {
	transitions := make([]*gastrologv1.CloudStorageTransition, len(cs.Transitions))
	for i, t := range cs.Transitions {
		transitions[i] = &gastrologv1.CloudStorageTransition{
			After:        t.After,
			StorageClass: t.StorageClass,
		}
	}
	return &gastrologv1.CloudService{
		Id:                cs.ID.String(),
		Name:              cs.Name,
		Provider:          cs.Provider,
		Bucket:            cs.Bucket,
		Region:            cs.Region,
		Endpoint:          cs.Endpoint,
		AccessKey:         cs.AccessKey,
		SecretKey:         cs.SecretKey,
		Container:         cs.Container,
		ConnectionString:  cs.ConnectionString,
		CredentialsJson:   cs.CredentialsJSON,
		StorageClass:      cs.StorageClass,
		ArchivalMode:      cs.ArchivalMode,
		Transitions:       transitions,
		RestoreTier:       cs.RestoreTier,
		RestoreDays:       cs.RestoreDays,
		SuspectGraceDays:  cs.SuspectGraceDays,
		ReconcileSchedule: cs.ReconcileSchedule,
	}
}

// CloudServiceFromProto converts a proto CloudService to config.CloudService.
// The ID field is best-effort parsed; an empty or invalid ID yields uuid.Nil
// (callers typically override it afterward for creation flows).
func CloudServiceFromProto(p *gastrologv1.CloudService) config.CloudService {
	if p == nil {
		return config.CloudService{}
	}
	id, _ := uuid.Parse(p.GetId())
	transitions := make([]config.CloudStorageTransition, len(p.GetTransitions()))
	for i, t := range p.GetTransitions() {
		transitions[i] = config.CloudStorageTransition{
			After:        t.GetAfter(),
			StorageClass: t.GetStorageClass(),
		}
	}
	return config.CloudService{
		ID:                id,
		Name:              p.GetName(),
		Provider:          p.GetProvider(),
		Bucket:            p.GetBucket(),
		Region:            p.GetRegion(),
		Endpoint:          p.GetEndpoint(),
		AccessKey:         p.GetAccessKey(),
		SecretKey:         p.GetSecretKey(),
		Container:         p.GetContainer(),
		ConnectionString:  p.GetConnectionString(),
		CredentialsJSON:   p.GetCredentialsJson(),
		StorageClass:      p.GetStorageClass(),
		ArchivalMode:      p.GetArchivalMode(),
		Transitions:       transitions,
		RestoreTier:       p.GetRestoreTier(),
		RestoreDays:       p.GetRestoreDays(),
		SuspectGraceDays:  p.GetSuspectGraceDays(),
		ReconcileSchedule: p.GetReconcileSchedule(),
	}
}

// ---------------------------------------------------------------------------
// NodeStorageConfig
// ---------------------------------------------------------------------------

// NodeStorageConfigToProto converts a config.NodeStorageConfig to its proto representation.
func NodeStorageConfigToProto(cfg config.NodeStorageConfig) *gastrologv1.NodeStorageConfig {
	storages := make([]*gastrologv1.FileStorage, len(cfg.FileStorages))
	for i, fs := range cfg.FileStorages {
		storages[i] = &gastrologv1.FileStorage{
			Id:                fs.ID.String(),
			StorageClass:      fs.StorageClass,
			Name:              fs.Name,
			Path:              fs.Path,
			MemoryBudgetBytes: fs.MemoryBudgetBytes,
		}
	}
	return &gastrologv1.NodeStorageConfig{
		NodeId:       cfg.NodeID,
		FileStorages: storages,
	}
}

// NodeStorageConfigFromProto converts a proto NodeStorageConfig to config.NodeStorageConfig.
func NodeStorageConfigFromProto(p *gastrologv1.NodeStorageConfig) config.NodeStorageConfig {
	if p == nil {
		return config.NodeStorageConfig{}
	}
	cfg := config.NodeStorageConfig{
		NodeID: p.GetNodeId(),
	}
	for _, a := range p.GetFileStorages() {
		fs := config.FileStorage{
			StorageClass:      a.GetStorageClass(),
			Name:              a.GetName(),
			Path:              a.GetPath(),
			MemoryBudgetBytes: a.GetMemoryBudgetBytes(),
		}
		if a.GetId() != "" {
			if id, err := uuid.Parse(a.GetId()); err == nil {
				fs.ID = id
			}
		}
		cfg.FileStorages = append(cfg.FileStorages, fs)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// TierConfig
// ---------------------------------------------------------------------------

// TierConfigToProto converts a config.TierConfig to its proto representation.
func TierConfigToProto(t config.TierConfig) *gastrologv1.TierConfig {
	placements := make([]*gastrologv1.TierPlacement, len(t.Placements))
	for i, p := range t.Placements {
		placements[i] = &gastrologv1.TierPlacement{
			StorageId: p.StorageID,
			Leader:    p.Leader,
		}
	}
	rules := make([]*gastrologv1.RetentionRule, len(t.RetentionRules))
	for i, r := range t.RetentionRules {
		ejectRouteIDs := make([]string, len(r.EjectRouteIDs))
		for j, id := range r.EjectRouteIDs {
			ejectRouteIDs[j] = id.String()
		}
		rules[i] = &gastrologv1.RetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.String(),
			Action:            string(r.Action),
			EjectRouteIds:     ejectRouteIDs,
		}
	}

	pb := &gastrologv1.TierConfig{
		Id:                t.ID.String(),
		Name:              t.Name,
		Type:              TierTypeToProto(t.Type),
		RetentionRules:    rules,
		MemoryBudgetBytes: t.MemoryBudgetBytes,
		StorageClass:      t.StorageClass,
		ActiveChunkClass:  t.ActiveChunkClass,
		CacheClass:        t.CacheClass,
		ReplicationFactor: t.ReplicationFactor,
		Path:              t.Path,
		Placements:        placements,
		VaultId:           t.VaultID.String(),
		Position:          t.Position,
		CacheEviction:     t.CacheEviction,
		CacheBudget:       t.CacheBudget,
		CacheTtl:          t.CacheTTL,
	}
	if t.RotationPolicyID != nil {
		pb.RotationPolicyId = t.RotationPolicyID.String()
	}
	if t.CloudServiceID != nil {
		pb.CloudServiceId = t.CloudServiceID.String()
	}
	return pb
}

// TierConfigFromProto converts a proto TierConfig to config.TierConfig.
func TierConfigFromProto(p *gastrologv1.TierConfig) (config.TierConfig, error) {
	if p == nil {
		return config.TierConfig{}, nil
	}
	// ID and VaultID are best-effort parsed: empty values yield uuid.Nil.
	// Callers that need a valid ID (e.g., the server handler) typically
	// override cfg.ID afterward. VaultID may be empty during tier creation
	// when the vault is assigned separately.
	id, _ := uuid.Parse(p.GetId())

	cfg := config.TierConfig{
		ID:                id,
		Name:              p.GetName(),
		Type:              TierTypeFromProto(p.GetType()),
		MemoryBudgetBytes: p.GetMemoryBudgetBytes(),
		StorageClass:      p.GetStorageClass(),
		ActiveChunkClass:  p.GetActiveChunkClass(),
		CacheClass:        p.GetCacheClass(),
		ReplicationFactor: p.GetReplicationFactor(),
		Path:              p.GetPath(),
		Position:          p.GetPosition(),
		CacheEviction:     p.GetCacheEviction(),
		CacheBudget:       p.GetCacheBudget(),
		CacheTTL:          p.GetCacheTtl(),
	}

	if p.GetVaultId() != "" {
		vaultID, err := uuid.Parse(p.GetVaultId())
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid vault_id: %w", err)
		}
		cfg.VaultID = vaultID
	}

	if p.GetRotationPolicyId() != "" {
		rpID, err := uuid.Parse(p.GetRotationPolicyId())
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid rotation_policy_id: %w", err)
		}
		cfg.RotationPolicyID = &rpID
	}

	if p.GetCloudServiceId() != "" {
		csID, err := uuid.Parse(p.GetCloudServiceId())
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid cloud_service_id: %w", err)
		}
		cfg.CloudServiceID = &csID
	}

	for _, r := range p.GetRetentionRules() {
		rpID, err := uuid.Parse(r.GetRetentionPolicyId())
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("invalid retention_policy_id in rule: %w", err)
		}
		rule := config.RetentionRule{
			RetentionPolicyID: rpID,
			Action:            config.RetentionAction(r.GetAction()),
		}
		for _, eid := range r.GetEjectRouteIds() {
			routeID, err := uuid.Parse(eid)
			if err != nil {
				return config.TierConfig{}, fmt.Errorf("invalid eject_route_id: %w", err)
			}
			rule.EjectRouteIDs = append(rule.EjectRouteIDs, routeID)
		}
		cfg.RetentionRules = append(cfg.RetentionRules, rule)
	}

	for _, p := range p.GetPlacements() {
		cfg.Placements = append(cfg.Placements, config.TierPlacement{
			StorageID: p.GetStorageId(),
			Leader:    p.GetLeader(),
		})
	}

	return cfg, nil
}

func TierTypeToProto(t config.TierType) gastrologv1.TierType {
	switch t {
	case config.TierTypeMemory:
		return gastrologv1.TierType_TIER_TYPE_MEMORY
	case config.TierTypeFile:
		return gastrologv1.TierType_TIER_TYPE_FILE
	case config.TierTypeCloud:
		return gastrologv1.TierType_TIER_TYPE_CLOUD
	case config.TierTypeJSONL:
		return gastrologv1.TierType_TIER_TYPE_JSONL
	default:
		return gastrologv1.TierType_TIER_TYPE_UNSPECIFIED
	}
}

func TierTypeFromProto(t gastrologv1.TierType) config.TierType {
	switch t {
	case gastrologv1.TierType_TIER_TYPE_MEMORY:
		return config.TierTypeMemory
	case gastrologv1.TierType_TIER_TYPE_FILE:
		return config.TierTypeFile
	case gastrologv1.TierType_TIER_TYPE_CLOUD:
		return config.TierTypeCloud
	case gastrologv1.TierType_TIER_TYPE_JSONL:
		return config.TierTypeJSONL
	case gastrologv1.TierType_TIER_TYPE_UNSPECIFIED:
		return config.TierTypeFile
	default:
		return config.TierTypeFile
	}
}
