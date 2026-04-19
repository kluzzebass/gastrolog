package server

import (
	"gastrolog/internal/glid"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"strings"

	"github.com/itchyny/gojq"

	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"connectrpc.com/connect"
	petname "github.com/dustinkirkland/golang-petname"
	"sigs.k8s.io/yaml"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/lookup"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// PeerIngesterStatsProvider looks up ingester stats from cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerIngesterStatsProvider interface {
	FindIngesterStats(ingesterID string) *apiv1.IngesterNodeStats
	CollectIngesterAlive(ingesterID string) map[string]bool
}

// PeerRouteStatsProvider aggregates route stats from all cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerRouteStatsProvider interface {
	AggregateRouteStats() (ingested, dropped, routed int64, filterActive bool, vaultStats []*apiv1.VaultRouteStats, routeStats []*apiv1.PerRouteStats)
}

// SystemServerConfig holds all dependencies for SystemServer construction.
type SystemServerConfig struct {
	Orch               *orchestrator.Orchestrator
	CfgStore           system.Store
	Factories          orchestrator.Factories
	CertManager        CertManager
	PeerStats          PeerIngesterStatsProvider
	PeerRouteStats     PeerRouteStatsProvider
	LocalNodeID        string
	AfterConfigApply   func(raftfsm.Notification)
	ConfigSignal       *notify.Signal
	ResolveManagedFile func(ctx context.Context, fileID string) string
	OnTLSConfigChange  func()
	OnLookupConfigChange func(system.LookupConfig, system.MaxMindConfig)
	CloudTesters       map[string]CloudServiceTester
	Tokens             *auth.TokenService
	PlacementReconcile func(ctx context.Context) // synchronous placement for RPC handlers
}

// SystemServer implements the ConfigService.
type SystemServer struct {
	orch                  *orchestrator.Orchestrator
	sysStore              system.Store
	factories             orchestrator.Factories
	certManager           CertManager
	peerStats             PeerIngesterStatsProvider
	peerRouteStats        PeerRouteStatsProvider
	localNodeID           string
	onTLSConfigChange     func()
	onLookupConfigChange  func(system.LookupConfig, system.MaxMindConfig)
	afterConfigApply      func(raftfsm.Notification)
	configSignal          *notify.Signal
	resolveManagedFile    func(ctx context.Context, fileID string) string
	cloudTesters          map[string]CloudServiceTester
	tokens                *auth.TokenService
	placementReconcile    func(ctx context.Context) // synchronous placement, nil in non-cluster mode
}

var _ gastrologv1connect.SystemServiceHandler = (*SystemServer)(nil)

// NewSystemServer creates a new SystemServer.
func NewSystemServer(cfg SystemServerConfig) *SystemServer {
	return &SystemServer{
		orch:                 cfg.Orch,
		sysStore:             cfg.CfgStore,
		factories:            cfg.Factories,
		certManager:          cfg.CertManager,
		peerStats:            cfg.PeerStats,
		peerRouteStats:       cfg.PeerRouteStats,
		localNodeID:          cfg.LocalNodeID,
		afterConfigApply:     cfg.AfterConfigApply,
		configSignal:         cfg.ConfigSignal,
		resolveManagedFile:   cfg.ResolveManagedFile,
		onTLSConfigChange:    cfg.OnTLSConfigChange,
		onLookupConfigChange: cfg.OnLookupConfigChange,
		cloudTesters:         cfg.CloudTesters,
		tokens:               cfg.Tokens,
		placementReconcile:   cfg.PlacementReconcile,
	}
}

// notify fires the afterConfigApply callback if set.
func (s *SystemServer) notify(n raftfsm.Notification) {
	if s.afterConfigApply != nil {
		s.afterConfigApply(n)
	}
}

// GetConfig returns the current configuration.
func (s *SystemServer) GetSystem(
	ctx context.Context,
	req *connect.Request[apiv1.GetSystemRequest],
) (*connect.Response[apiv1.GetSystemResponse], error) {
	resp, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("load config: %w", err))
	}
	return connect.NewResponse(resp), nil
}

// buildFullSystem assembles a complete GetConfigResponse from the config store.
// Used by GetConfig and by mutation handlers to return the updated config inline.
// Returns an error if any config section fails to load — never returns partial data.
func (s *SystemServer) buildFullSystem(ctx context.Context) (*apiv1.GetSystemResponse, error) {
	resp := &apiv1.GetSystemResponse{}
	if s.sysStore != nil {
		err := errors.Join(
			s.loadSystemVaults(ctx, resp),
			s.loadSystemIngesters(ctx, resp),
			s.loadConfigFilters(ctx, resp),
			s.loadConfigRotationPolicies(ctx, resp),
			s.loadConfigRetentionPolicies(ctx, resp),
			s.loadConfigRoutes(ctx, resp),
			s.loadConfigNodeConfigs(ctx, resp),
			s.loadConfigManagedFiles(ctx, resp),
			s.loadConfigCloudServices(ctx, resp),
			s.loadSystemTiers(ctx, resp),
			s.loadConfigNodeStorageConfigs(ctx, resp),
		)
		if err != nil {
			return nil, err
		}
	}
	if s.configSignal != nil {
		resp.SystemRaftIndex = s.configSignal.Version()
	}
	return resp, nil
}

// currentSystemRaftIndex returns the committed system Raft log index exposed on GetSystem.
func (s *SystemServer) currentSystemRaftIndex() uint64 {
	if s.configSignal == nil {
		return 0
	}
	return s.configSignal.Version()
}

// buildFullSettingsResponse builds the authenticated GetSettingsResponse payload.
// includeSecrets mirrors GetSettingsRequest.include_secrets.
func (s *SystemServer) buildFullSettingsResponse(ctx context.Context, includeSecrets bool) (*apiv1.GetSettingsResponse, error) {
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}

	maxJobs := int32(ss.Scheduler.MaxConcurrentJobs) //nolint:gosec // G115: small config value, always fits in int32
	if maxJobs == 0 {
		maxJobs = int32(s.orch.MaxConcurrentJobs()) //nolint:gosec // G115: small config value, always fits in int32
	}

	mm := &apiv1.MaxMindSettings{
		AutoDownload:      ss.MaxMind.AutoDownload,
		LicenseConfigured: ss.MaxMind.AccountID != "" && ss.MaxMind.LicenseKey != "",
	}
	if !ss.MaxMind.LastUpdate.IsZero() {
		mm.LastUpdate = ss.MaxMind.LastUpdate.Format(time.RFC3339)
	}

	authSettings := &apiv1.AuthSettings{
		TokenDuration:        ss.Auth.TokenDuration,
		JwtSecretConfigured:  ss.Auth.JWTSecret != "",
		RefreshTokenDuration: ss.Auth.RefreshTokenDuration,
		PasswordPolicy: &apiv1.PasswordPolicySettings{
			MinLength:             int32(ss.Auth.PasswordPolicy.MinLength),             //nolint:gosec // G115
			RequireMixedCase:      ss.Auth.PasswordPolicy.RequireMixedCase,
			RequireDigit:          ss.Auth.PasswordPolicy.RequireDigit,
			RequireSpecial:        ss.Auth.PasswordPolicy.RequireSpecial,
			MaxConsecutiveRepeats: int32(ss.Auth.PasswordPolicy.MaxConsecutiveRepeats), //nolint:gosec // G115
			ForbidAnimalNoise:     ss.Auth.PasswordPolicy.ForbidAnimalNoise,
		},
	}

	if includeSecrets {
		mm.AccountId = []byte(ss.MaxMind.AccountID)
		mm.LicenseKey = ss.MaxMind.LicenseKey
	}

	resp := &apiv1.GetSettingsResponse{
		Auth: authSettings,
		Query: &apiv1.QuerySettings{
			Timeout:           ss.Query.Timeout,
			MaxFollowDuration: ss.Query.MaxFollowDuration,
			MaxResultCount:    int32(ss.Query.MaxResultCount), //nolint:gosec // G115
		},
		Scheduler: &apiv1.SchedulerSettings{
			MaxConcurrentJobs: maxJobs,
		},
		Tls: &apiv1.TLSSettings{
			DefaultCert:         ss.TLS.DefaultCert,
			Enabled:             ss.TLS.TLSEnabled,
			HttpToHttpsRedirect: ss.TLS.HTTPToHTTPSRedirect,
			HttpsPort:           ss.TLS.HTTPSPort,
		},
		Lookup: &apiv1.LookupSettings{
			HttpLookups:     httpLookupsToProto(ss.Lookup.HTTPLookups),
			JsonFileLookups: jsonFileLookupsToProto(ss.Lookup.JSONFileLookups),
			YamlFileLookups: yamlFileLookupsToProto(ss.Lookup.YAMLFileLookups),
			MmdbLookups:     mmdbLookupsToProto(ss.Lookup.MMDBLookups),
			CsvLookups:      csvLookupsToProto(ss.Lookup.CSVLookups),
			StaticLookups:   staticLookupsToProto(ss.Lookup.StaticLookups),
		},
		Maxmind: mm,
		Cluster: &apiv1.ClusterSettings{
			BroadcastInterval: ss.Cluster.BroadcastInterval,
		},
		SetupWizardDismissed: func() bool { v, _ := s.sysStore.GetSetupWizardDismissed(ctx); return v }(),
		NodeId:               []byte(s.localNodeID),
	}

	if nodeUUID, err := glid.ParseUUID(s.localNodeID); err == nil {
		if node, err := s.sysStore.GetNode(ctx, nodeUUID); err == nil && node != nil {
			resp.NodeName = node.Name
		}
	}

	return resp, nil
}

func (s *SystemServer) newSettingsMutationEcho(ctx context.Context) (*apiv1.SettingsMutationEcho, error) {
	settings, err := s.buildFullSettingsResponse(ctx, false)
	if err != nil {
		return nil, err
	}
	return &apiv1.SettingsMutationEcho{
		Settings:        settings,
		SystemRaftIndex: s.currentSystemRaftIndex(),
	}, nil
}

func (s *SystemServer) loadSystemVaults(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	cfgStores, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return fmt.Errorf("list vaults: %w", err)
	}
	for _, vaultCfg := range cfgStores {
		resp.Vaults = append(resp.Vaults, vaultConfigToProto(vaultCfg))
	}
	return nil
}

func vaultConfigToProto(vaultCfg system.VaultConfig) *apiv1.VaultConfig {
	return &apiv1.VaultConfig{
		Id:      vaultCfg.ID.ToProto(),
		Name:    vaultCfg.Name,
		Enabled: vaultCfg.Enabled,
	}
}

func (s *SystemServer) loadSystemIngesters(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	ingesters, err := s.sysStore.ListIngesters(ctx)
	if err != nil {
		return fmt.Errorf("list ingesters: %w", err)
	}
	for _, ing := range ingesters {
		resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
			Id:        ing.ID.ToProto(),
			Name:      ing.Name,
			Type:      ing.Type,
			Params:    ing.Params,
			Enabled:   ing.Enabled,
			NodeIds:   stringsToBytes(ing.NodeIDs),
			Singleton: ing.Singleton,
		})
	}
	return nil
}

func (s *SystemServer) loadConfigFilters(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	filters, err := s.sysStore.ListFilters(ctx)
	if err != nil {
		return fmt.Errorf("list filters: %w", err)
	}
	for _, fc := range filters {
		resp.Filters = append(resp.Filters, &apiv1.FilterConfig{
			Id:         fc.ID.ToProto(),
			Name:       fc.Name,
			Expression: fc.Expression,
		})
	}
	return nil
}

func (s *SystemServer) loadConfigRotationPolicies(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	policies, err := s.sysStore.ListRotationPolicies(ctx)
	if err != nil {
		return fmt.Errorf("list rotation policies: %w", err)
	}
	for _, pol := range policies {
		p := rotationPolicyToProto(pol)
		p.Id = pol.ID.ToProto()
		p.Name = pol.Name
		resp.RotationPolicies = append(resp.RotationPolicies, p)
	}
	return nil
}

func (s *SystemServer) loadConfigRetentionPolicies(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	retPolicies, err := s.sysStore.ListRetentionPolicies(ctx)
	if err != nil {
		return fmt.Errorf("list retention policies: %w", err)
	}
	for _, pol := range retPolicies {
		p := retentionPolicyToProto(pol)
		p.Id = pol.ID.ToProto()
		p.Name = pol.Name
		resp.RetentionPolicies = append(resp.RetentionPolicies, p)
	}
	return nil
}

func (s *SystemServer) loadConfigRoutes(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	routes, err := s.sysStore.ListRoutes(ctx)
	if err != nil {
		return fmt.Errorf("list routes: %w", err)
	}
	for _, rt := range routes {
		prt := &apiv1.RouteConfig{
			Id:           rt.ID.ToProto(),
			Name:         rt.Name,
			Distribution: string(rt.Distribution),
			Enabled:      rt.Enabled,
			EjectOnly:    rt.EjectOnly,
		}
		if rt.FilterID != nil {
			prt.FilterId = rt.FilterID.ToProto()
		}
		for _, destID := range rt.Destinations {
			prt.Destinations = append(prt.Destinations, &apiv1.RouteDestination{
				VaultId: destID.ToProto(),
			})
		}
		resp.Routes = append(resp.Routes, prt)
	}
	return nil
}

func (s *SystemServer) loadConfigNodeConfigs(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	nodes, err := s.sysStore.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	for _, n := range nodes {
		resp.NodeConfigs = append(resp.NodeConfigs, &apiv1.NodeConfig{
			Id:   n.ID.ToProto(),
			Name: n.Name,
		})
	}
	return nil
}

func (s *SystemServer) loadConfigCloudServices(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	services, err := s.sysStore.ListCloudServices(ctx)
	if err != nil {
		return fmt.Errorf("list cloud services: %w", err)
	}
	for _, cs := range services {
		transitions := make([]*apiv1.CloudStorageTransition, len(cs.Transitions))
		for i, t := range cs.Transitions {
			transitions[i] = &apiv1.CloudStorageTransition{
				After:        t.After,
				StorageClass: t.StorageClass,
			}
		}
		resp.CloudServices = append(resp.CloudServices, &apiv1.CloudService{
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
		})
	}
	return nil
}

func (s *SystemServer) loadSystemTiers(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	tiers, err := s.sysStore.ListTiers(ctx)
	if err != nil {
		return fmt.Errorf("list tiers: %w", err)
	}
	for _, tier := range tiers {
		tierPlacements, _ := s.sysStore.GetTierPlacements(ctx, tier.ID)
		var placements []*apiv1.TierPlacement
		for _, p := range tierPlacements {
			placements = append(placements, &apiv1.TierPlacement{
				StorageId: []byte(p.StorageID),
				Leader:    p.Leader,
			})
		}
		tc := &apiv1.TierConfig{
			Id:                tier.ID.ToProto(),
			Name:              tier.Name,
			Type:              tierTypeToProto(tier.Type),
			MemoryBudgetBytes: tier.MemoryBudgetBytes,
			StorageClass:      tier.StorageClass,
			ActiveChunkClass:  tier.ActiveChunkClass,
			CacheClass:        tier.CacheClass,
			ReplicationFactor: tier.ReplicationFactor,
			Path:              tier.Path,
			Placements:        placements,
			VaultId:           tier.VaultID.ToProto(),
			Position:          tier.Position,
			CacheEviction:     tier.CacheEviction,
			CacheBudget:  tier.CacheBudget,
			CacheTtl:          tier.CacheTTL,
		}
		if tier.RotationPolicyID != nil {
			tc.RotationPolicyId = tier.RotationPolicyID.ToProto()
		}
		if tier.CloudServiceID != nil {
			tc.CloudServiceId = tier.CloudServiceID.ToProto()
		}
		for _, r := range tier.RetentionRules {
			pb := &apiv1.RetentionRule{
				RetentionPolicyId: r.RetentionPolicyID.ToProto(),
				Action:            string(r.Action),
			}
			for _, eid := range r.EjectRouteIDs {
				pb.EjectRouteIds = append(pb.EjectRouteIds, eid.ToProto())
			}
			tc.RetentionRules = append(tc.RetentionRules, pb)
		}
		resp.Tiers = append(resp.Tiers, tc)
	}
	return nil
}

func tierTypeToProto(t system.TierType) apiv1.TierType {
	switch t {
	case system.TierTypeMemory:
		return apiv1.TierType_TIER_TYPE_MEMORY
	case system.TierTypeFile:
		return apiv1.TierType_TIER_TYPE_FILE
	case system.TierTypeCloud:
		return apiv1.TierType_TIER_TYPE_CLOUD
	case system.TierTypeJSONL:
		return apiv1.TierType_TIER_TYPE_JSONL
	default:
		return apiv1.TierType_TIER_TYPE_UNSPECIFIED
	}
}

func (s *SystemServer) loadConfigNodeStorageConfigs(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	configs, err := s.sysStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return fmt.Errorf("list node storage configs: %w", err)
	}
	for _, nsc := range configs {
		storages := make([]*apiv1.FileStorage, len(nsc.FileStorages))
		for i, a := range nsc.FileStorages {
			storages[i] = &apiv1.FileStorage{
				Id:                a.ID.ToProto(),
				StorageClass:      a.StorageClass,
				Name:              a.Name,
				Path:              a.Path,
				MemoryBudgetBytes: a.MemoryBudgetBytes,
			}
		}
		resp.NodeStorageConfigs = append(resp.NodeStorageConfigs, &apiv1.NodeStorageConfig{
			NodeId: []byte(nsc.NodeID),
			FileStorages:  storages,
		})
	}
	return nil
}

// GetSettings returns the server-level configuration.
// Unauthenticated callers (e.g. the registration page) only receive the
// password policy — everything else is stripped to prevent information leakage.
func (s *SystemServer) GetSettings(
	ctx context.Context,
	req *connect.Request[apiv1.GetSettingsRequest],
) (*connect.Response[apiv1.GetSettingsResponse], error) {
	ss, err := s.sysStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, errInternal(err)
	}

	// Unauthenticated: return only the password policy.
	if auth.ClaimsFromContext(ctx) == nil {
		pp := ss.Auth.PasswordPolicy
		return connect.NewResponse(&apiv1.GetSettingsResponse{
			Auth: &apiv1.AuthSettings{
				PasswordPolicy: &apiv1.PasswordPolicySettings{
					MinLength:             int32(pp.MinLength),             //nolint:gosec // G115
					RequireMixedCase:      pp.RequireMixedCase,
					RequireDigit:          pp.RequireDigit,
					RequireSpecial:        pp.RequireSpecial,
					MaxConsecutiveRepeats: int32(pp.MaxConsecutiveRepeats), //nolint:gosec // G115
					ForbidAnimalNoise:     pp.ForbidAnimalNoise,
				},
			},
		}), nil
	}

	resp, err := s.buildFullSettingsResponse(ctx, req.Msg.IncludeSecrets)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// PutServiceSettings updates auth, query, scheduler, TLS, and cluster settings.
func (s *SystemServer) PutServiceSettings(
	ctx context.Context,
	req *connect.Request[apiv1.PutServiceSettingsRequest],
) (*connect.Response[apiv1.PutServiceSettingsResponse], error) {
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}
	if connErr := mergeServiceSettingsFields(req.Msg, &ss); connErr != nil {
		return nil, connErr
	}
	if connErr := validateTokenDurations(ss.Auth); connErr != nil {
		return nil, connErr
	}
	saveCtx := system.WithSaveServerSettingsNotifyKey(ctx, system.NotifyKeyServiceSettings)
	if err := s.sysStore.SaveServerSettings(saveCtx, ss); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyServiceSettings})
	if s.onTLSConfigChange != nil && req.Msg.Tls != nil {
		s.onTLSConfigChange()
	}
	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PutServiceSettingsResponse{Echo: echo}), nil
}

// PutLookupSettings replaces lookup table configuration.
func (s *SystemServer) PutLookupSettings(
	ctx context.Context,
	req *connect.Request[apiv1.PutLookupSettingsRequest],
) (*connect.Response[apiv1.PutLookupSettingsResponse], error) {
	if req.Msg.Lookup == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("lookup is required"))
	}
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}
	mergeLookup(req.Msg.Lookup, &ss.Lookup)
	if connErr := validateLookupNames(ss.Lookup); connErr != nil {
		return nil, connErr
	}
	saveCtx := system.WithSaveServerSettingsNotifyKey(ctx, system.NotifyKeyLookupSettings)
	if err := s.sysStore.SaveServerSettings(saveCtx, ss); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyLookupSettings})
	if s.onLookupConfigChange != nil {
		s.onLookupConfigChange(ss.Lookup, ss.MaxMind)
	}
	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PutLookupSettingsResponse{Echo: echo}), nil
}

// PutMaxMindSettings updates MaxMind auto-download configuration.
func (s *SystemServer) PutMaxMindSettings(
	ctx context.Context,
	req *connect.Request[apiv1.PutMaxMindSettingsRequest],
) (*connect.Response[apiv1.PutMaxMindSettingsResponse], error) {
	if req.Msg.Maxmind == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("maxmind is required"))
	}
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}
	mergeMaxMind(req.Msg.Maxmind, &ss.MaxMind)
	saveCtx := system.WithSaveServerSettingsNotifyKey(ctx, system.NotifyKeyMaxMindSettings)
	if err := s.sysStore.SaveServerSettings(saveCtx, ss); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyMaxMindSettings})
	if s.onLookupConfigChange != nil {
		s.onLookupConfigChange(ss.Lookup, ss.MaxMind)
	}
	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PutMaxMindSettingsResponse{Echo: echo}), nil
}

// PutSetupSettings updates setup wizard dismissal state.
func (s *SystemServer) PutSetupSettings(
	ctx context.Context,
	req *connect.Request[apiv1.PutSetupSettingsRequest],
) (*connect.Response[apiv1.PutSetupSettingsResponse], error) {
	if req.Msg.SetupWizardDismissed == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("setup_wizard_dismissed is required"))
	}
	if err := s.sysStore.SetSetupWizardDismissed(ctx, *req.Msg.SetupWizardDismissed); err != nil {
		return nil, errInternal(err)
	}
	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PutSetupSettingsResponse{Echo: echo}), nil
}

// DeleteLookup removes a lookup table by name from whichever type list it
// belongs to (HTTP, JSON file, MMDB, CSV, or static).
func (s *SystemServer) DeleteLookup(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteLookupRequest],
) (*connect.Response[apiv1.DeleteLookupResponse], error) {
	name := req.Msg.Name
	if name == "" {
		return nil, errRequired("name")
	}

	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}

	found := false
	ss.Lookup.HTTPLookups = slicesDeleteFunc(ss.Lookup.HTTPLookups, func(l system.HTTPLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})
	ss.Lookup.JSONFileLookups = slicesDeleteFunc(ss.Lookup.JSONFileLookups, func(l system.JSONFileLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})
	ss.Lookup.YAMLFileLookups = slicesDeleteFunc(ss.Lookup.YAMLFileLookups, func(l system.YAMLFileLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})
	ss.Lookup.MMDBLookups = slicesDeleteFunc(ss.Lookup.MMDBLookups, func(l system.MMDBLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})
	ss.Lookup.CSVLookups = slicesDeleteFunc(ss.Lookup.CSVLookups, func(l system.CSVLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})
	ss.Lookup.StaticLookups = slicesDeleteFunc(ss.Lookup.StaticLookups, func(l system.StaticLookupConfig) bool {
		if l.Name == name { found = true; return true }; return false
	})

	if !found {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("lookup %q not found", name))
	}

	saveCtx := system.WithSaveServerSettingsNotifyKey(ctx, system.NotifyKeyLookupSettings)
	if err := s.sysStore.SaveServerSettings(saveCtx, ss); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyLookupSettings})

	if s.onLookupConfigChange != nil {
		s.onLookupConfigChange(ss.Lookup, ss.MaxMind)
	}

	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteLookupResponse{Echo: echo}), nil
}

// slicesDeleteFunc returns a new slice with elements matching f removed.
func slicesDeleteFunc[T any](s []T, f func(T) bool) []T {
	out := s[:0:0]
	for _, v := range s {
		if !f(v) {
			out = append(out, v)
		}
	}
	return out
}

// RegenerateJwtSecret generates a new random JWT signing secret, replacing the
// existing one. All active sessions are immediately invalidated because the old
// secret can no longer verify existing tokens.
func (s *SystemServer) RegenerateJwtSecret(
	ctx context.Context,
	_ *connect.Request[apiv1.RegenerateJwtSecretRequest],
) (*connect.Response[apiv1.RegenerateJwtSecretResponse], error) {
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("generate secret: %w", err))
	}
	ss.Auth.JWTSecret = base64.StdEncoding.EncodeToString(secret)

	saveCtx := system.WithSaveServerSettingsNotifyKey(ctx, system.NotifyKeyServiceSettings)
	if err := s.sysStore.SaveServerSettings(saveCtx, ss); err != nil {
		return nil, errInternal(err)
	}

	// Swap the live signing secret so existing tokens fail verification immediately.
	if s.tokens != nil {
		s.tokens.SetSecret(secret)
	}

	// Invalidate all refresh tokens by setting TokenInvalidatedAt on every user.
	now := time.Now().UTC()
	users, err := s.sysStore.ListUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list users: %w", err))
	}
	for _, u := range users {
		if err := s.sysStore.InvalidateTokens(ctx, u.ID, now); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens for %s: %w", u.Username, err))
		}
	}

	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyServiceSettings})

	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RegenerateJwtSecretResponse{Echo: echo}), nil
}

// PutNodeConfig creates or updates a node configuration.
func (s *SystemServer) PutNodeConfig(
	ctx context.Context,
	req *connect.Request[apiv1.PutNodeConfigRequest],
) (*connect.Response[apiv1.PutNodeConfigResponse], error) {
	cfg := req.Msg.GetConfig()
	if cfg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config must not be nil"))
	}
	name := cfg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name must not be empty"))
	}

	// Use the ID from the request if provided, otherwise fall back to the local node.
	idBytes := cfg.GetId()
	if len(idBytes) == 0 {
		idBytes = []byte(s.localNodeID)
	}
	nodeUUID := glid.FromBytes(idBytes)

	// Reject duplicate names.
	nodes, err := s.sysStore.ListNodes(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("node", nodeUUID, name, nodes, func(n system.NodeConfig) (glid.GLID, string) { return n.ID, n.Name }); connErr != nil {
		return nil, connErr
	}

	if err := s.sysStore.PutNode(ctx, system.NodeConfig{ID: nodeUUID, Name: name}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("put node config: %w", err))
	}

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutNodeConfigResponse{System: fullCfg}), nil
}

// GenerateName returns a random petname for use as a default entity name.
func (s *SystemServer) GenerateName(
	_ context.Context,
	_ *connect.Request[apiv1.GenerateNameRequest],
) (*connect.Response[apiv1.GenerateNameResponse], error) {
	return connect.NewResponse(&apiv1.GenerateNameResponse{
		Name: petname.Generate(2, "-"),
	}), nil
}

// WatchConfig streams a notification whenever configuration changes.
func (s *SystemServer) WatchSystem(
	ctx context.Context,
	req *connect.Request[apiv1.WatchSystemRequest],
	stream *connect.ServerStream[apiv1.WatchSystemResponse],
) error {
	// Send one initial message so the client knows the stream is alive.
	// Include the current config version so the client can seed its cache.
	initialVersion := uint64(0)
	if s.configSignal != nil {
		initialVersion = s.configSignal.Version()
	}
	if err := stream.Send(&apiv1.WatchSystemResponse{SystemRaftIndex: initialVersion}); err != nil {
		return err
	}
	if s.configSignal == nil {
		// No signal wired (e.g. tests) — block until context cancelled.
		<-ctx.Done()
		return nil
	}
	for {
		ch := s.configSignal.C()
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
			if err := stream.Send(&apiv1.WatchSystemResponse{
				SystemRaftIndex: s.configSignal.Version(),
			}); err != nil {
				return err
			}
		}
	}
}

func (s *SystemServer) loadServerSettings(ctx context.Context) (system.ServerSettings, error) {
	ss, err := s.sysStore.LoadServerSettings(ctx)
	if err != nil {
		return ss, errInternal(err)
	}
	if ss.Auth.PasswordPolicy.MinLength == 0 {
		ss.Auth.PasswordPolicy.MinLength = 8
	}
	if ss.Scheduler.MaxConcurrentJobs == 0 {
		ss.Scheduler.MaxConcurrentJobs = 4
	}
	return ss, nil
}

func mergeServiceSettingsFields(msg *apiv1.PutServiceSettingsRequest, ss *system.ServerSettings) *connect.Error {
	if msg.Auth != nil {
		mergeAuth(msg.Auth, &ss.Auth)
	}
	if msg.Query != nil {
		if err := mergeQuery(msg.Query, &ss.Query); err != nil {
			return err
		}
	}
	if msg.Scheduler != nil {
		if err := mergeScheduler(msg.Scheduler, &ss.Scheduler); err != nil {
			return err
		}
	}
	if msg.Tls != nil {
		mergeTLS(msg.Tls, &ss.TLS)
	}
	if msg.Cluster != nil {
		if err := mergeCluster(msg.Cluster, &ss.Cluster); err != nil {
			return err
		}
	}
	return nil
}

func mergeAuth(a *apiv1.PutAuthSettings, auth *system.AuthConfig) {
	if a.TokenDuration != nil {
		auth.TokenDuration = *a.TokenDuration
	}
	if a.RefreshTokenDuration != nil {
		auth.RefreshTokenDuration = *a.RefreshTokenDuration
	}
	if a.PasswordPolicy != nil {
		mergePasswordPolicy(a.PasswordPolicy, &auth.PasswordPolicy)
	}
}

func mergePasswordPolicy(pp *apiv1.PutPasswordPolicySettings, pol *system.PasswordPolicy) {
	if pp.MinLength != nil {
		pol.MinLength = int(*pp.MinLength)
	}
	if pp.RequireMixedCase != nil {
		pol.RequireMixedCase = *pp.RequireMixedCase
	}
	if pp.RequireDigit != nil {
		pol.RequireDigit = *pp.RequireDigit
	}
	if pp.RequireSpecial != nil {
		pol.RequireSpecial = *pp.RequireSpecial
	}
	if pp.MaxConsecutiveRepeats != nil {
		pol.MaxConsecutiveRepeats = int(*pp.MaxConsecutiveRepeats)
	}
	if pp.ForbidAnimalNoise != nil {
		pol.ForbidAnimalNoise = *pp.ForbidAnimalNoise
	}
}

func mergeQuery(q *apiv1.PutQuerySettings, query *system.QueryConfig) *connect.Error {
	if q.Timeout != nil {
		query.Timeout = *q.Timeout
	}
	if q.MaxFollowDuration != nil {
		query.MaxFollowDuration = *q.MaxFollowDuration
	}
	if q.MaxResultCount != nil {
		if *q.MaxResultCount < 0 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_result_count must be non-negative, got %d", *q.MaxResultCount))
		}
		query.MaxResultCount = int(*q.MaxResultCount)
	}
	return nil
}

func mergeScheduler(sc *apiv1.PutSchedulerSettings, sched *system.SchedulerConfig) *connect.Error {
	if sc.MaxConcurrentJobs != nil {
		if *sc.MaxConcurrentJobs < 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_concurrent_jobs must be at least 1, got %d", *sc.MaxConcurrentJobs))
		}
		sched.MaxConcurrentJobs = int(*sc.MaxConcurrentJobs)
	}
	return nil
}

func mergeTLS(t *apiv1.PutTLSSettings, tlsCfg *system.TLSConfig) {
	if t.DefaultCert != nil {
		tlsCfg.DefaultCert = *t.DefaultCert
	}
	if t.Enabled != nil {
		tlsCfg.TLSEnabled = *t.Enabled && tlsCfg.DefaultCert != ""
	}
	if t.HttpToHttpsRedirect != nil {
		tlsCfg.HTTPToHTTPSRedirect = *t.HttpToHttpsRedirect && tlsCfg.DefaultCert != ""
	}
	if t.HttpsPort != nil {
		tlsCfg.HTTPSPort = *t.HttpsPort
	}
}

func mergeLookup(l *apiv1.PutLookupSettings, lookup *system.LookupConfig) {
	if l.HttpLookups != nil {
		lookup.HTTPLookups = httpLookupsFromProto(l.HttpLookups)
	}
	if l.JsonFileLookups != nil {
		lookup.JSONFileLookups = jsonFileLookupsFromProto(l.JsonFileLookups)
	}
	if l.YamlFileLookups != nil {
		lookup.YAMLFileLookups = yamlFileLookupsFromProto(l.YamlFileLookups)
	}
	if l.MmdbLookups != nil {
		lookup.MMDBLookups = mmdbLookupsFromProto(l.MmdbLookups)
	}
	if l.CsvLookups != nil {
		lookup.CSVLookups = csvLookupsFromProto(l.CsvLookups)
	}
	if l.StaticLookups != nil {
		lookup.StaticLookups = staticLookupsFromProto(l.StaticLookups)
	}
}

func mergeMaxMind(mm *apiv1.PutMaxMindSettings, cfg *system.MaxMindConfig) {
	if mm.AutoDownload != nil {
		cfg.AutoDownload = *mm.AutoDownload
	}
	if mm.AccountId != nil {
		cfg.AccountID = string(mm.AccountId)
	}
	if mm.LicenseKey != nil {
		cfg.LicenseKey = *mm.LicenseKey
	}
}

func httpLookupsToProto(lookups []system.HTTPLookupConfig) []*apiv1.HTTPLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.HTTPLookupEntry, len(lookups))
	for i, l := range lookups {
		params := make([]*apiv1.HTTPLookupParam, len(l.Parameters))
		for j, p := range l.Parameters {
			params[j] = &apiv1.HTTPLookupParam{Name: p.Name, Description: p.Description}
		}
		out[i] = &apiv1.HTTPLookupEntry{
			Name:          l.Name,
			UrlTemplate:   l.URLTemplate,
			Headers:       l.Headers,
			ResponsePaths: l.ResponsePaths,
			Parameters:    params,
			Timeout:       l.Timeout,
			CacheTtl:      l.CacheTTL,
			CacheSize:     int32(l.CacheSize), //nolint:gosec // reasonable config value
		}
	}
	return out
}

func httpLookupsFromProto(entries []*apiv1.HTTPLookupEntry) []system.HTTPLookupConfig {
	out := make([]system.HTTPLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.UrlTemplate == "" {
			continue
		}
		params := make([]system.HTTPLookupParam, len(e.Parameters))
		for j, p := range e.Parameters {
			params[j] = system.HTTPLookupParam{Name: p.Name, Description: p.Description}
		}
		out = append(out, system.HTTPLookupConfig{
			Name:          e.Name,
			URLTemplate:   e.UrlTemplate,
			Headers:       e.Headers,
			ResponsePaths: e.ResponsePaths,
			Parameters:    params,
			Timeout:       e.Timeout,
			CacheTTL:      e.CacheTtl,
			CacheSize:     int(e.CacheSize),
		})
	}
	return out
}

func jsonFileLookupsToProto(lookups []system.JSONFileLookupConfig) []*apiv1.JSONFileLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.JSONFileLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.JSONFileLookupEntry{
			Name:         l.Name,
			FileId:       glid.MustParse(l.FileID).ToProto(),
			Query:        l.Query,
			KeyColumn:    l.KeyColumn,
			ValueColumns: l.ValueColumns,
		}
	}
	return out
}

func jsonFileLookupsFromProto(entries []*apiv1.JSONFileLookupEntry) []system.JSONFileLookupConfig {
	out := make([]system.JSONFileLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || len(e.FileId) == 0 {
			continue
		}
		out = append(out, system.JSONFileLookupConfig{
			Name:         e.Name,
			FileID:       parseLookupFileID(e.FileId),
			Query:        e.Query,
			KeyColumn:    e.KeyColumn,
			ValueColumns: e.ValueColumns,
		})
	}
	return out
}

func yamlFileLookupsToProto(lookups []system.YAMLFileLookupConfig) []*apiv1.YAMLFileLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.YAMLFileLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.YAMLFileLookupEntry{
			Name:         l.Name,
			FileId:       glid.MustParse(l.FileID).ToProto(),
			Query:        l.Query,
			KeyColumn:    l.KeyColumn,
			ValueColumns: l.ValueColumns,
		}
	}
	return out
}

func yamlFileLookupsFromProto(entries []*apiv1.YAMLFileLookupEntry) []system.YAMLFileLookupConfig {
	out := make([]system.YAMLFileLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || len(e.FileId) == 0 {
			continue
		}
		out = append(out, system.YAMLFileLookupConfig{
			Name:         e.Name,
			FileID:       parseLookupFileID(e.FileId),
			Query:        e.Query,
			KeyColumn:    e.KeyColumn,
			ValueColumns: e.ValueColumns,
		})
	}
	return out
}

func mmdbLookupsToProto(lookups []system.MMDBLookupConfig) []*apiv1.MMDBLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.MMDBLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.MMDBLookupEntry{
			Name:   l.Name,
			DbType: l.DBType,
			FileId: glid.MustParse(l.FileID).ToProto(),
		}
	}
	return out
}

func mmdbLookupsFromProto(entries []*apiv1.MMDBLookupEntry) []system.MMDBLookupConfig {
	out := make([]system.MMDBLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" {
			continue
		}
		out = append(out, system.MMDBLookupConfig{
			Name:   e.Name,
			DBType: e.DbType,
			FileID: parseLookupFileID(e.FileId),
		})
	}
	return out
}

func csvLookupsToProto(lookups []system.CSVLookupConfig) []*apiv1.CSVLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.CSVLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.CSVLookupEntry{
			Name:         l.Name,
			FileId:       glid.MustParse(l.FileID).ToProto(),
			KeyColumn:    l.KeyColumn,
			ValueColumns: l.ValueColumns,
		}
	}
	return out
}

func csvLookupsFromProto(entries []*apiv1.CSVLookupEntry) []system.CSVLookupConfig {
	out := make([]system.CSVLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || len(e.FileId) == 0 {
			continue
		}
		out = append(out, system.CSVLookupConfig{
			Name:         e.Name,
			FileID:       parseLookupFileID(e.FileId),
			KeyColumn:    e.KeyColumn,
			ValueColumns: e.ValueColumns,
		})
	}
	return out
}

func staticLookupsToProto(lookups []system.StaticLookupConfig) []*apiv1.StaticLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.StaticLookupEntry, len(lookups))
	for i, l := range lookups {
		rows := make([]*apiv1.StaticLookupRow, len(l.Rows))
		for j, r := range l.Rows {
			rows[j] = &apiv1.StaticLookupRow{Values: r.Values}
		}
		out[i] = &apiv1.StaticLookupEntry{
			Name:         l.Name,
			KeyColumn:    l.KeyColumn,
			ValueColumns: l.ValueColumns,
			Rows:         rows,
		}
	}
	return out
}

func staticLookupsFromProto(entries []*apiv1.StaticLookupEntry) []system.StaticLookupConfig {
	out := make([]system.StaticLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" {
			continue
		}
		rows := make([]system.StaticLookupRow, len(e.Rows))
		for j, r := range e.Rows {
			rows[j] = system.StaticLookupRow{Values: r.Values}
		}
		out = append(out, system.StaticLookupConfig{
			Name:         e.Name,
			KeyColumn:    e.KeyColumn,
			ValueColumns: e.ValueColumns,
			Rows:         rows,
		})
	}
	return out
}

// parseLookupFileID converts proto bytes to a GLID string, handling both
// new format (16 raw bytes) and legacy format (26 UTF-8 bytes of base32hex).
func parseLookupFileID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b) == glid.Size {
		return glid.FromBytes(b).String()
	}
	// Legacy: stored as []byte(base32hexString) — try parsing the UTF-8 as a GLID.
	if g, err := glid.Parse(string(b)); err == nil {
		return g.String()
	}
	return string(b)
}

func mergeCluster(c *apiv1.PutClusterSettings, cluster *system.ClusterConfig) *connect.Error {
	if c.BroadcastInterval != nil {
		if _, err := time.ParseDuration(*c.BroadcastInterval); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid broadcast_interval: %w", err))
		}
		cluster.BroadcastInterval = *c.BroadcastInterval
	}
	return nil
}

func validateTokenDurations(auth system.AuthConfig) *connect.Error {
	var tokenDur, refreshDur time.Duration
	if auth.TokenDuration != "" {
		d, err := time.ParseDuration(auth.TokenDuration)
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid token duration %q: %w", auth.TokenDuration, err))
		}
		if d < time.Minute {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("token duration must be at least 1 minute, got %s", auth.TokenDuration))
		}
		tokenDur = d
	}
	if auth.RefreshTokenDuration != "" {
		d, err := time.ParseDuration(auth.RefreshTokenDuration)
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid refresh token duration %q: %w", auth.RefreshTokenDuration, err))
		}
		if d < time.Hour {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("refresh token duration must be at least 1 hour, got %s", auth.RefreshTokenDuration))
		}
		refreshDur = d
	}
	if tokenDur > 0 && refreshDur > 0 && refreshDur <= tokenDur {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("refresh token duration (%s) must be longer than token duration (%s)", auth.RefreshTokenDuration, auth.TokenDuration))
	}
	return nil
}

// validateLookupNames checks that no two lookup tables (across all types)
// share the same name. Duplicate names would shadow each other in the
// pipeline registry.
func validateLookupNames(lc system.LookupConfig) *connect.Error {
	seen := make(map[string]string) // name → type
	for _, l := range lc.HTTPLookups {
		if prev, ok := seen[l.Name]; ok {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("lookup name %q is used by both %s and http lookup", l.Name, prev))
		}
		seen[l.Name] = "http"
	}
	for _, l := range lc.JSONFileLookups {
		if prev, ok := seen[l.Name]; ok {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("lookup name %q is used by both %s and json file lookup", l.Name, prev))
		}
		seen[l.Name] = "json file"
	}
	for _, l := range lc.YAMLFileLookups {
		if prev, ok := seen[l.Name]; ok {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("lookup name %q is used by both %s and yaml file lookup", l.Name, prev))
		}
		seen[l.Name] = "yaml file"
	}
	for _, l := range lc.MMDBLookups {
		if prev, ok := seen[l.Name]; ok {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("lookup name %q is used by both %s and mmdb lookup", l.Name, prev))
		}
		seen[l.Name] = "mmdb"
	}
	for _, l := range lc.CSVLookups {
		if prev, ok := seen[l.Name]; ok {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("lookup name %q is used by both %s and csv lookup", l.Name, prev))
		}
		seen[l.Name] = "csv"
	}
	return nil
}

// TestHTTPLookup tests an HTTP lookup configuration.
// Test values are URL template variables: each key corresponds to a {key} placeholder
// in the URL template. All variables are substituted, then a single request is made.
func (s *SystemServer) TestHTTPLookup(
	ctx context.Context,
	req *connect.Request[apiv1.TestHTTPLookupRequest],
) (*connect.Response[apiv1.TestHTTPLookupResponse], error) {
	cfg := req.Msg.GetConfig()
	if cfg == nil || cfg.UrlTemplate == "" {
		return connect.NewResponse(&apiv1.TestHTTPLookupResponse{
			Error: "URL template is required",
		}), nil
	}

	lcfg := lookup.HTTPConfig{
		URLTemplate:   cfg.UrlTemplate,
		Headers:       cfg.Headers,
		ResponsePaths: cfg.ResponsePaths,
		CacheSize:     int(cfg.CacheSize),
	}
	if cfg.Timeout != "" {
		d, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return connect.NewResponse(&apiv1.TestHTTPLookupResponse{
				Error: fmt.Sprintf("invalid timeout %q: %v", cfg.Timeout, err),
			}), nil
		}
		lcfg.Timeout = d
	}

	h := lookup.NewHTTP(lcfg)
	result := h.TestFetch(ctx, req.Msg.Values)

	return connect.NewResponse(&apiv1.TestHTTPLookupResponse{
		Success: true,
		Results: []*apiv1.TestHTTPLookupResult{{
			Fields: result,
		}},
	}), nil
}

// PreviewCSVLookup reads a managed CSV file and returns column headers,
// sample rows, and total row count for the settings UI preview.
func (s *SystemServer) PreviewCSVLookup(
	ctx context.Context,
	req *connect.Request[apiv1.PreviewCSVLookupRequest],
) (*connect.Response[apiv1.PreviewCSVLookupResponse], error) {
	fileID := glid.FromBytes(req.Msg.GetFileId()).String()
	if fileID == "" {
		return connect.NewResponse(&apiv1.PreviewCSVLookupResponse{
			Error: "file_id is required",
		}), nil
	}

	filePath := s.resolveManagedFile(ctx, fileID)
	if filePath == "" {
		return connect.NewResponse(&apiv1.PreviewCSVLookupResponse{
			Error: "file not found",
		}), nil
	}

	f, err := os.Open(filePath) //nolint:gosec // path from validated managed file
	if err != nil {
		return connect.NewResponse(&apiv1.PreviewCSVLookupResponse{
			Error: fmt.Sprintf("open file: %v", err),
		}), nil
	}
	defer func() { _ = f.Close() }()

	// Strip UTF-8 BOM if present — Excel and many tools emit it.
	var bom [3]byte
	if n, _ := f.Read(bom[:]); n == 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		// BOM consumed, reader starts after it.
	} else {
		_, _ = f.Seek(0, 0) // not a BOM, rewind
	}

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = false

	header, err := reader.Read()
	if err != nil {
		return connect.NewResponse(&apiv1.PreviewCSVLookupResponse{
			Error: fmt.Sprintf("read header: %v", err),
		}), nil
	}

	// Resolve key column.
	keyCol := req.Msg.GetKeyColumn()
	if keyCol == "" && len(header) > 0 {
		keyCol = header[0]
	}

	maxRows := int(req.Msg.GetMaxRows())
	if maxRows <= 0 {
		maxRows = 10
	}

	var rows []*apiv1.CSVPreviewRow
	totalRows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		totalRows++
		if len(rows) < maxRows {
			rows = append(rows, &apiv1.CSVPreviewRow{Values: record})
		}
	}

	return connect.NewResponse(&apiv1.PreviewCSVLookupResponse{
		Columns:   header,
		KeyColumn: keyCol,
		Rows:      rows,
		TotalRows: int32(totalRows),
	}), nil
}

// PreviewJSONLookup reads a managed JSON file and returns pretty-printed
// content for structure inspection in the settings UI.
func (s *SystemServer) PreviewJSONLookup(
	ctx context.Context,
	req *connect.Request[apiv1.PreviewJSONLookupRequest],
) (*connect.Response[apiv1.PreviewJSONLookupResponse], error) {
	prev := s.previewStructuredLookup(ctx, req.Msg.GetFileId(), int(req.Msg.GetMaxBytes()),
		req.Msg.GetQuery(), req.Msg.GetParameters(), "json")
	return connect.NewResponse(&apiv1.PreviewJSONLookupResponse{
		Content:     prev.content,
		TotalSize:   prev.totalSize,
		Truncated:   prev.truncated,
		Error:       prev.errMsg,
		QueryResult: prev.queryResult,
		QueryError:  prev.queryError,
	}), nil
}

func (s *SystemServer) PreviewYAMLLookup(
	ctx context.Context,
	req *connect.Request[apiv1.PreviewYAMLLookupRequest],
) (*connect.Response[apiv1.PreviewYAMLLookupResponse], error) {
	prev := s.previewStructuredLookup(ctx, req.Msg.GetFileId(), int(req.Msg.GetMaxBytes()),
		req.Msg.GetQuery(), req.Msg.GetParameters(), "yaml")
	return connect.NewResponse(&apiv1.PreviewYAMLLookupResponse{
		Content:     prev.content,
		TotalSize:   prev.totalSize,
		Truncated:   prev.truncated,
		Error:       prev.errMsg,
		QueryResult: prev.queryResult,
		QueryError:  prev.queryError,
	}), nil
}

type structuredPreview struct {
	content     string
	totalSize   int64
	truncated   bool
	errMsg      string
	queryResult string
	queryError  string
}

// previewStructuredLookup reads a managed structured file (JSON or YAML),
// returns a truncated display body plus optional jq query result. Format
// only affects (a) how bytes parse into the generic jq tree and (b) how
// the display body is pretty-printed when parsing succeeds.
func (s *SystemServer) previewStructuredLookup(
	ctx context.Context,
	fileIDBytes []byte,
	maxBytes int,
	query string,
	params map[string]string,
	format string,
) structuredPreview {
	fileID := glid.FromBytes(fileIDBytes).String()
	if fileID == "" {
		return structuredPreview{errMsg: "file_id is required"}
	}

	filePath := s.resolveManagedFile(ctx, fileID)
	if filePath == "" {
		return structuredPreview{errMsg: "file not found"}
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return structuredPreview{errMsg: fmt.Sprintf("stat file: %v", err)}
	}
	totalSize := info.Size()

	if maxBytes <= 0 {
		maxBytes = 4096
	}

	f, err := os.Open(filePath) //nolint:gosec // path from validated managed file
	if err != nil {
		return structuredPreview{errMsg: fmt.Sprintf("open file: %v", err), totalSize: totalSize}
	}
	defer func() { _ = f.Close() }()

	// Read full file for parsing (needed for query evaluation). Cap at 10MB.
	readLimit := min(totalSize, 10<<20)
	fullBuf := make([]byte, readLimit)
	n, _ := f.Read(fullBuf)
	fullData := fullBuf[:n]

	// Parse into the generic tree jq operates on.
	var parsed any
	if format == "yaml" {
		_ = yaml.Unmarshal(fullData, &parsed)
	} else {
		_ = json.Unmarshal(fullData, &parsed)
	}

	displayData, truncated := previewDisplayBody(fullData, parsed, format, maxBytes, totalSize)

	out := structuredPreview{
		content:   string(displayData),
		totalSize: totalSize,
		truncated: truncated,
	}
	if query != "" && parsed != nil {
		out.queryResult, out.queryError = evalJQ(query, params, parsed)
	}
	return out
}

// previewDisplayBody chooses the bytes shown in the preview pane and reports
// whether they were cut short. JSON is re-encoded with indentation for
// readability; YAML is shown raw so the user sees their own file back.
func previewDisplayBody(raw []byte, parsed any, format string, maxBytes int, totalSize int64) ([]byte, bool) {
	// Start with raw bytes clipped to maxBytes.
	body := raw
	truncated := false
	if len(body) > maxBytes {
		body = body[:maxBytes]
		truncated = true
	}

	if format == "yaml" {
		return body, int64(len(body)) < totalSize
	}
	// JSON: re-encode with indentation when parsing succeeded.
	if parsed == nil {
		return body, truncated
	}
	pretty, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return body, truncated
	}
	if len(pretty) > maxBytes {
		return pretty[:maxBytes], true
	}
	return pretty, int64(len(pretty)) < totalSize
}

// evalJQ evaluates a jq query with parameter substitution.
func evalJQ(query string, params map[string]string, data any) (result, errMsg string) {
	for k, v := range params {
		query = strings.ReplaceAll(query, "{"+k+"}", v)
	}
	parsed, err := gojq.Parse(query)
	if err != nil {
		return "", fmt.Sprintf("parse error: %v", err)
	}
	code, err := gojq.Compile(parsed)
	if err != nil {
		return "", fmt.Sprintf("compile error: %v", err)
	}
	var results []any
	var lastErr string
	iter := code.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if e, isErr := v.(error); isErr {
			lastErr = fmt.Sprintf("eval error: %v", e)
			break
		}
		results = append(results, v)
		if len(results) >= 10 {
			break
		}
	}
	if len(results) == 0 {
		return "", lastErr
	}
	pretty, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", fmt.Sprintf("marshal result: %v", err)
	}
	return string(pretty), lastErr
}

// checkNameConflict returns an AlreadyExists error if another entity of the
// same type already has the given name. Empty names are allowed to coexist.
func checkNameConflict[S ~[]E, E any](entityType string, id glid.GLID, name string, existing S, identify func(E) (glid.GLID, string)) *connect.Error {
	for _, e := range existing {
		eid, ename := identify(e)
		if eid != id && ename == name {
			return connect.NewError(connect.CodeAlreadyExists,
				fmt.Errorf("%s name %q is already in use", entityType, name))
		}
	}
	return nil
}

