package convert

// system.go provides canonical converters between config domain types and
// their protobuf representations for CloudService, NodeStorageConfig, and
// TierConfig. Both the server RPC handlers and the Raft FSM command
// package call these functions — there is exactly one source of truth for
// each field mapping. See gastrolog-2f8et.

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// ---------------------------------------------------------------------------
// CloudService
// ---------------------------------------------------------------------------

// CloudServiceToProto converts a system.CloudService to its proto representation.
func CloudServiceToProto(cs system.CloudService) *gastrologv1.CloudService {
	transitions := make([]*gastrologv1.CloudStorageTransition, len(cs.Transitions))
	for i, t := range cs.Transitions {
		transitions[i] = &gastrologv1.CloudStorageTransition{
			After:        t.After,
			StorageClass: t.StorageClass,
		}
	}
	return &gastrologv1.CloudService{
		Id:                cs.ID.ToProto(),
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

// CloudServiceFromProto converts a proto CloudService to system.CloudService.
func CloudServiceFromProto(p *gastrologv1.CloudService) system.CloudService {
	if p == nil {
		return system.CloudService{}
	}
	transitions := make([]system.CloudStorageTransition, len(p.GetTransitions()))
	for i, t := range p.GetTransitions() {
		transitions[i] = system.CloudStorageTransition{
			After:        t.GetAfter(),
			StorageClass: t.GetStorageClass(),
		}
	}
	return system.CloudService{
		ID:                glid.FromBytes(p.GetId()),
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

// NodeStorageConfigToProto converts a system.NodeStorageConfig to its proto representation.
func NodeStorageConfigToProto(cfg system.NodeStorageConfig) *gastrologv1.NodeStorageConfig {
	storages := make([]*gastrologv1.FileStorage, len(cfg.FileStorages))
	for i, fs := range cfg.FileStorages {
		storages[i] = &gastrologv1.FileStorage{
			Id:                fs.ID.ToProto(),
			StorageClass:      fs.StorageClass,
			Name:              fs.Name,
			Path:              fs.Path,
			MemoryBudgetBytes: fs.MemoryBudgetBytes,
		}
	}
	return &gastrologv1.NodeStorageConfig{
		NodeId:       []byte(cfg.NodeID),
		FileStorages: storages,
	}
}

// NodeStorageConfigFromProto converts a proto NodeStorageConfig to system.NodeStorageConfig.
func NodeStorageConfigFromProto(p *gastrologv1.NodeStorageConfig) system.NodeStorageConfig {
	if p == nil {
		return system.NodeStorageConfig{}
	}
	cfg := system.NodeStorageConfig{
		NodeID: string(p.GetNodeId()),
	}
	for _, a := range p.GetFileStorages() {
		fs := system.FileStorage{
			StorageClass:      a.GetStorageClass(),
			Name:              a.GetName(),
			Path:              a.GetPath(),
			MemoryBudgetBytes: a.GetMemoryBudgetBytes(),
		}
		if len(a.GetId()) > 0 {
			fs.ID = glid.FromBytes(a.GetId())
		}
		cfg.FileStorages = append(cfg.FileStorages, fs)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// TierConfig
// ---------------------------------------------------------------------------

// TierConfigToProto converts a system.TierConfig to its proto representation.
// Placements are passed separately since they live in system.Runtime, not Config.
func TierConfigToProto(t system.TierConfig, placements []system.TierPlacement) *gastrologv1.TierConfig {
	pbPlacements := make([]*gastrologv1.TierPlacement, len(placements))
	for i, p := range placements {
		pbPlacements[i] = &gastrologv1.TierPlacement{
			StorageId: []byte(p.StorageID),
			Leader:    p.Leader,
		}
	}
	rules := make([]*gastrologv1.RetentionRule, len(t.RetentionRules))
	for i, r := range t.RetentionRules {
		rules[i] = &gastrologv1.RetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.ToProto(),
			Action:            string(r.Action),
			EjectRouteIds:     glid.SliceToProto(r.EjectRouteIDs),
		}
	}

	pb := &gastrologv1.TierConfig{
		Id:                t.ID.ToProto(),
		Name:              t.Name,
		Type:              TierTypeToProto(t.Type),
		RetentionRules:    rules,
		MemoryBudgetBytes: t.MemoryBudgetBytes,
		StorageClass:      t.StorageClass,
		ReplicationFactor: t.ReplicationFactor,
		Path:              t.Path,
		Placements:        pbPlacements,
		VaultId:           t.VaultID.ToProto(),
		Position:          t.Position,
		CacheEviction:     t.CacheEviction,
		CacheBudget:       t.CacheBudget,
		CacheTtl:          t.CacheTTL,
	}
	pb.RotationPolicyId = glid.OptionalToProto(t.RotationPolicyID)
	pb.CloudServiceId = glid.OptionalToProto(t.CloudServiceID)
	return pb
}

// TierConfigFromProto converts a proto TierConfig to system.TierConfig.
func TierConfigFromProto(p *gastrologv1.TierConfig) (system.TierConfig, error) {
	if p == nil {
		return system.TierConfig{}, nil
	}
	cfg := system.TierConfig{
		ID:                glid.FromBytes(p.GetId()),
		Name:              p.GetName(),
		Type:              TierTypeFromProto(p.GetType()),
		MemoryBudgetBytes: p.GetMemoryBudgetBytes(),
		StorageClass:      p.GetStorageClass(),
		ReplicationFactor: p.GetReplicationFactor(),
		Path:              p.GetPath(),
		Position:          p.GetPosition(),
		CacheEviction:     p.GetCacheEviction(),
		CacheBudget:       p.GetCacheBudget(),
		CacheTTL:          p.GetCacheTtl(),
		VaultID:           glid.FromBytes(p.GetVaultId()),
		RotationPolicyID:  glid.OptionalFromProto(p.GetRotationPolicyId()),
		CloudServiceID:    glid.OptionalFromProto(p.GetCloudServiceId()),
	}

	for _, r := range p.GetRetentionRules() {
		rule := system.RetentionRule{
			RetentionPolicyID: glid.FromBytes(r.GetRetentionPolicyId()),
			Action:            system.RetentionAction(r.GetAction()),
			EjectRouteIDs:     glid.SliceFromProto(r.GetEjectRouteIds()),
		}
		cfg.RetentionRules = append(cfg.RetentionRules, rule)
	}

	return cfg, nil
}

// TierPlacementsFromProto extracts placements from a proto TierConfig.
func TierPlacementsFromProto(p *gastrologv1.TierConfig) []system.TierPlacement {
	if p == nil {
		return nil
	}
	var placements []system.TierPlacement
	for _, pp := range p.GetPlacements() {
		placements = append(placements, system.TierPlacement{
			StorageID: string(pp.GetStorageId()),
			Leader:    pp.GetLeader(),
		})
	}
	return placements
}

func TierTypeToProto(t system.TierType) gastrologv1.TierType {
	switch t {
	case system.TierTypeMemory:
		return gastrologv1.TierType_TIER_TYPE_MEMORY
	case system.TierTypeFile:
		return gastrologv1.TierType_TIER_TYPE_FILE
	case system.TierTypeJSONL:
		return gastrologv1.TierType_TIER_TYPE_JSONL
	default:
		return gastrologv1.TierType_TIER_TYPE_UNSPECIFIED
	}
}

func TierTypeFromProto(t gastrologv1.TierType) system.TierType {
	switch t {
	case gastrologv1.TierType_TIER_TYPE_MEMORY:
		return system.TierTypeMemory
	case gastrologv1.TierType_TIER_TYPE_FILE:
		return system.TierTypeFile
	case gastrologv1.TierType_TIER_TYPE_JSONL:
		return system.TierTypeJSONL
	case gastrologv1.TierType_TIER_TYPE_UNSPECIFIED:
		return system.TierTypeFile
	default:
		return system.TierTypeFile
	}
}

// ---------------------------------------------------------------------------
// VaultConfig (post-tier shape)
// ---------------------------------------------------------------------------
//
// Mirrors the TierConfig converters during the vault refactor
// (gastrolog-257l7). Once consumers migrate from TierConfig to VaultConfig,
// the tier converters above are deleted.

// VaultConfigToProto converts a system.VaultConfig to its proto representation.
func VaultConfigToProto(v system.VaultConfig) *gastrologv1.VaultConfig {
	pbPlacements := make([]*gastrologv1.VaultPlacement, len(v.Placements))
	for i, p := range v.Placements {
		pbPlacements[i] = &gastrologv1.VaultPlacement{
			StorageId: []byte(p.StorageID),
			Leader:    p.Leader,
		}
	}
	rules := make([]*gastrologv1.RetentionRule, len(v.RetentionRules))
	for i, r := range v.RetentionRules {
		rules[i] = &gastrologv1.RetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.ToProto(),
			Action:            string(r.Action),
			EjectRouteIds:     glid.SliceToProto(r.EjectRouteIDs),
		}
	}

	pb := &gastrologv1.VaultConfig{
		Id:                v.ID.ToProto(),
		Name:              v.Name,
		Enabled:           v.Enabled,
		Type:              VaultTypeToProto(v.Type),
		RetentionRules:    rules,
		MemoryBudgetBytes: v.MemoryBudgetBytes,
		StorageClass:      v.StorageClass,
		ReplicationFactor: v.ReplicationFactor,
		Path:              v.Path,
		Placements:        pbPlacements,
		CacheEviction:     v.CacheEviction,
		CacheBudget:       v.CacheBudget,
		CacheTtl:          v.CacheTTL,
	}
	pb.RotationPolicyId = glid.OptionalToProto(v.RotationPolicyID)
	pb.CloudServiceId = glid.OptionalToProto(v.CloudServiceID)
	return pb
}

// VaultConfigFromProto converts a proto VaultConfig to system.VaultConfig.
func VaultConfigFromProto(p *gastrologv1.VaultConfig) (system.VaultConfig, error) {
	if p == nil {
		return system.VaultConfig{}, nil
	}
	cfg := system.VaultConfig{
		ID:                glid.FromBytes(p.GetId()),
		Name:              p.GetName(),
		Enabled:           p.GetEnabled(),
		Type:              VaultTypeFromProto(p.GetType()),
		MemoryBudgetBytes: p.GetMemoryBudgetBytes(),
		StorageClass:      p.GetStorageClass(),
		ReplicationFactor: p.GetReplicationFactor(),
		Path:              p.GetPath(),
		CacheEviction:     p.GetCacheEviction(),
		CacheBudget:       p.GetCacheBudget(),
		CacheTTL:          p.GetCacheTtl(),
		RotationPolicyID:  glid.OptionalFromProto(p.GetRotationPolicyId()),
		CloudServiceID:    glid.OptionalFromProto(p.GetCloudServiceId()),
	}

	for _, r := range p.GetRetentionRules() {
		rule := system.RetentionRule{
			RetentionPolicyID: glid.FromBytes(r.GetRetentionPolicyId()),
			Action:            system.RetentionAction(r.GetAction()),
			EjectRouteIDs:     glid.SliceFromProto(r.GetEjectRouteIds()),
		}
		cfg.RetentionRules = append(cfg.RetentionRules, rule)
	}

	for _, pp := range p.GetPlacements() {
		cfg.Placements = append(cfg.Placements, system.TierPlacement{
			StorageID: string(pp.GetStorageId()),
			Leader:    pp.GetLeader(),
		})
	}

	return cfg, nil
}

// VaultTypeToProto maps the Go-side TierType (still used as the underlying
// string enum during the refactor) to the new proto VaultType.
func VaultTypeToProto(t system.TierType) gastrologv1.VaultType {
	switch t {
	case system.TierTypeMemory:
		return gastrologv1.VaultType_VAULT_TYPE_MEMORY
	case system.TierTypeFile:
		return gastrologv1.VaultType_VAULT_TYPE_FILE
	case system.TierTypeJSONL:
		return gastrologv1.VaultType_VAULT_TYPE_JSONL
	default:
		return gastrologv1.VaultType_VAULT_TYPE_UNSPECIFIED
	}
}

// VaultTypeFromProto maps proto VaultType back to the Go-side TierType.
func VaultTypeFromProto(t gastrologv1.VaultType) system.TierType {
	switch t {
	case gastrologv1.VaultType_VAULT_TYPE_MEMORY:
		return system.TierTypeMemory
	case gastrologv1.VaultType_VAULT_TYPE_FILE:
		return system.TierTypeFile
	case gastrologv1.VaultType_VAULT_TYPE_JSONL:
		return system.TierTypeJSONL
	case gastrologv1.VaultType_VAULT_TYPE_UNSPECIFIED:
		return system.TierTypeFile
	default:
		return system.TierTypeFile
	}
}
