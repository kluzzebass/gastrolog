package convert

// config.go provides canonical converters between config domain types and
// their protobuf representations. Both the server RPC handlers and the
// Raft FSM command package call these functions — there is exactly one
// source of truth for each field mapping. See gastrolog-2f8et.

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
		RestoreSpeed:       cs.RestoreSpeed,
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
		RestoreSpeed:       p.GetRestoreSpeed(),
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
// VaultConfig
// ---------------------------------------------------------------------------

// VaultConfigToProto converts a system.VaultConfig to its proto representation.
func VaultConfigToProto(v system.VaultConfig) *gastrologv1.VaultConfig {
	pbPlacements := make([]*gastrologv1.VaultPlacement, len(v.Placements))
	for i, p := range v.Placements {
		pbPlacements[i] = &gastrologv1.VaultPlacement{
			StorageId: []byte(p.StorageID),
		}
	}
	rules := make([]*gastrologv1.RetentionRule, len(v.RetentionRules))
	for i, r := range v.RetentionRules {
		rules[i] = &gastrologv1.RetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.ToProto(),
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
		CacheEviction:        v.CacheEviction,
		CacheBudget:          v.CacheBudget,
		CacheTtl:             v.CacheTTL,
		RetentionDisposition: v.RetentionDisposition,
		WOfN:                 string(v.WOfN),
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
		CacheEviction:        p.GetCacheEviction(),
		CacheBudget:          p.GetCacheBudget(),
		CacheTTL:             p.GetCacheTtl(),
		RetentionDisposition: p.GetRetentionDisposition(),
		WOfN:                 system.WOfNPolicy(p.GetWOfN()),
		RotationPolicyID:     glid.OptionalFromProto(p.GetRotationPolicyId()),
		CloudServiceID:       glid.OptionalFromProto(p.GetCloudServiceId()),
	}

	for _, r := range p.GetRetentionRules() {
		rule := system.RetentionRule{
			RetentionPolicyID: glid.FromBytes(r.GetRetentionPolicyId()),
		}
		cfg.RetentionRules = append(cfg.RetentionRules, rule)
	}

	for _, pp := range p.GetPlacements() {
		cfg.Placements = append(cfg.Placements, system.VaultPlacement{
			StorageID: string(pp.GetStorageId()),
		})
	}

	return cfg, nil
}

// VaultTypeToProto maps the Go-side VaultType to the proto VaultType.
func VaultTypeToProto(t system.VaultType) gastrologv1.VaultType {
	switch t {
	case system.VaultTypeMemory:
		return gastrologv1.VaultType_VAULT_TYPE_MEMORY
	case system.VaultTypeFile:
		return gastrologv1.VaultType_VAULT_TYPE_FILE
	case system.VaultTypeJSONL:
		return gastrologv1.VaultType_VAULT_TYPE_JSONL
	default:
		return gastrologv1.VaultType_VAULT_TYPE_UNSPECIFIED
	}
}

// ---------------------------------------------------------------------------
// Route stages (gastrolog-4kkoo Phase 5)
// ---------------------------------------------------------------------------

// RouteStagesToProto converts a slice of system.RouteStage to its proto
// representation. Today only MatchStage is set; future kinds (per
// gastrolog-5e85x Programmable Ingestion) plug into the same oneof.
func RouteStagesToProto(stages []system.RouteStage) []*gastrologv1.RouteStage {
	if len(stages) == 0 {
		return nil
	}
	out := make([]*gastrologv1.RouteStage, len(stages))
	for i, s := range stages {
		stage := &gastrologv1.RouteStage{}
		if s.Match != nil {
			stage.Stage = &gastrologv1.RouteStage_Match{
				Match: &gastrologv1.MatchStage{Expression: s.Match.Expression},
			}
		}
		out[i] = stage
	}
	return out
}

// RouteStagesFromProto converts a proto RouteStage slice back to
// system.RouteStage. Stages without a recognized variant become zero-value
// stages — semantic validation is the consumer's job (e.g. orchestrator).
func RouteStagesFromProto(stages []*gastrologv1.RouteStage) []system.RouteStage {
	if len(stages) == 0 {
		return nil
	}
	out := make([]system.RouteStage, len(stages))
	for i, s := range stages {
		if m := s.GetMatch(); m != nil {
			out[i] = system.RouteStage{
				Match: &system.MatchStage{Expression: m.GetExpression()},
			}
		}
	}
	return out
}

// VaultTypeFromProto maps proto VaultType back to the Go-side VaultType.
// Round-trips empty: VAULT_TYPE_UNSPECIFIED maps to the empty VaultType so
// "type was never set" is distinguishable from "type is file".
func VaultTypeFromProto(t gastrologv1.VaultType) system.VaultType {
	switch t {
	case gastrologv1.VaultType_VAULT_TYPE_MEMORY:
		return system.VaultTypeMemory
	case gastrologv1.VaultType_VAULT_TYPE_FILE:
		return system.VaultTypeFile
	case gastrologv1.VaultType_VAULT_TYPE_JSONL:
		return system.VaultTypeJSONL
	case gastrologv1.VaultType_VAULT_TYPE_UNSPECIFIED:
		return ""
	default:
		return ""
	}
}

// --- Log Levels (gastrolog-3flfp) ---

// LogLevelConfigToProto converts a system.LogLevelConfig to its proto form.
// The Go side stores levels as int64 (matching slog.Level); the wire form
// uses the LogLevel enum, mapping is purely cosmetic.
func LogLevelConfigToProto(cfg system.LogLevelConfig) *gastrologv1.LogLevelConfig {
	out := &gastrologv1.LogLevelConfig{
		DefaultLevel: SlogLevelToProto(cfg.Default),
	}
	if len(cfg.Rules) > 0 {
		out.Rules = make([]*gastrologv1.LogLevelRule, len(cfg.Rules))
		for i, r := range cfg.Rules {
			out.Rules[i] = &gastrologv1.LogLevelRule{
				Pattern: r.Pattern,
				Level:   SlogLevelToProto(r.Level),
			}
		}
	}
	return out
}

// LogLevelConfigFromProto converts a proto LogLevelConfig back to the Go type.
// Nil-safe — nil proto produces the zero-value system.LogLevelConfig.
func LogLevelConfigFromProto(p *gastrologv1.LogLevelConfig) system.LogLevelConfig {
	if p == nil {
		return system.LogLevelConfig{}
	}
	out := system.LogLevelConfig{
		Default: SlogLevelFromProto(p.GetDefaultLevel()),
	}
	if len(p.GetRules()) > 0 {
		out.Rules = make([]system.LogLevelRule, len(p.GetRules()))
		for i, r := range p.GetRules() {
			out.Rules[i] = system.LogLevelRule{
				Pattern: r.GetPattern(),
				Level:   SlogLevelFromProto(r.GetLevel()),
			}
		}
	}
	return out
}

// SlogLevelToProto maps a slog level (as int64) to the proto LogLevel enum.
// Values outside the recognised set fall through to UNSPECIFIED rather
// than rounding — the operator UI/CLI should not invent levels the wire
// can't carry.
func SlogLevelToProto(lvl int64) gastrologv1.LogLevel {
	switch lvl {
	case -4: // slog.LevelDebug
		return gastrologv1.LogLevel_LOG_LEVEL_DEBUG
	case 0: // slog.LevelInfo
		return gastrologv1.LogLevel_LOG_LEVEL_INFO
	case 4: // slog.LevelWarn
		return gastrologv1.LogLevel_LOG_LEVEL_WARN
	case 8: // slog.LevelError
		return gastrologv1.LogLevel_LOG_LEVEL_ERROR
	default:
		return gastrologv1.LogLevel_LOG_LEVEL_UNSPECIFIED
	}
}

// SlogLevelFromProto maps a proto LogLevel enum to the int64 form used in
// system.LogLevelConfig (matching slog.Level's underlying type).
// UNSPECIFIED falls through to 0 (INFO) — that's the documented default
// when no rule has been set, and equals slog.LevelInfo.
func SlogLevelFromProto(p gastrologv1.LogLevel) int64 {
	switch p {
	case gastrologv1.LogLevel_LOG_LEVEL_DEBUG:
		return -4
	case gastrologv1.LogLevel_LOG_LEVEL_INFO:
		return 0
	case gastrologv1.LogLevel_LOG_LEVEL_WARN:
		return 4
	case gastrologv1.LogLevel_LOG_LEVEL_ERROR:
		return 8
	case gastrologv1.LogLevel_LOG_LEVEL_UNSPECIFIED:
		return 0
	default:
		return 0
	}
}
