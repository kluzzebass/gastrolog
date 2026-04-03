package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"connectrpc.com/connect"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/lookup"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// PeerIngesterStatsProvider looks up ingester stats from cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerIngesterStatsProvider interface {
	FindIngesterStats(ingesterID string) *apiv1.IngesterNodeStats
}

// PeerRouteStatsProvider aggregates route stats from all cluster peer broadcasts.
// Implemented by cluster.PeerState; nil in single-node mode.
type PeerRouteStatsProvider interface {
	AggregateRouteStats() (ingested, dropped, routed int64, filterActive bool, vaultStats []*apiv1.VaultRouteStats, routeStats []*apiv1.PerRouteStats)
}

// ConfigServerConfig holds all dependencies for ConfigServer construction.
type ConfigServerConfig struct {
	Orch               *orchestrator.Orchestrator
	CfgStore           config.Store
	Factories          orchestrator.Factories
	CertManager        CertManager
	PeerStats          PeerIngesterStatsProvider
	PeerRouteStats     PeerRouteStatsProvider
	LocalNodeID        string
	AfterConfigApply   func(raftfsm.Notification)
	ConfigSignal       *notify.Signal
	ResolveManagedFile func(ctx context.Context, fileID string) string
	OnTLSConfigChange  func()
	OnLookupConfigChange func(config.LookupConfig, config.MaxMindConfig)
	CloudTesters       map[string]CloudServiceTester
	Tokens             *auth.TokenService
	PlacementReconcile func(ctx context.Context) // synchronous placement for RPC handlers
}

// ConfigServer implements the ConfigService.
type ConfigServer struct {
	orch                  *orchestrator.Orchestrator
	cfgStore              config.Store
	factories             orchestrator.Factories
	certManager           CertManager
	peerStats             PeerIngesterStatsProvider
	peerRouteStats        PeerRouteStatsProvider
	localNodeID           string
	onTLSConfigChange     func()
	onLookupConfigChange  func(config.LookupConfig, config.MaxMindConfig)
	afterConfigApply      func(raftfsm.Notification)
	configSignal          *notify.Signal
	resolveManagedFile    func(ctx context.Context, fileID string) string
	cloudTesters          map[string]CloudServiceTester
	tokens                *auth.TokenService
	placementReconcile    func(ctx context.Context) // synchronous placement, nil in non-cluster mode
}

var _ gastrologv1connect.ConfigServiceHandler = (*ConfigServer)(nil)

// NewConfigServer creates a new ConfigServer.
func NewConfigServer(cfg ConfigServerConfig) *ConfigServer {
	return &ConfigServer{
		orch:                 cfg.Orch,
		cfgStore:             cfg.CfgStore,
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
func (s *ConfigServer) notify(n raftfsm.Notification) {
	if s.afterConfigApply != nil {
		s.afterConfigApply(n)
	}
}

// GetConfig returns the current configuration.
func (s *ConfigServer) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	return connect.NewResponse(s.buildFullConfig(ctx)), nil
}

// buildFullConfig assembles a complete GetConfigResponse from the config store.
// Used by GetConfig and by mutation handlers to return the updated config inline.
// Includes the current config version from the signal so the frontend can track
// cache freshness without timers.
func (s *ConfigServer) buildFullConfig(ctx context.Context) *apiv1.GetConfigResponse {
	resp := &apiv1.GetConfigResponse{}
	if s.cfgStore != nil {
		s.loadConfigVaults(ctx, resp)
		s.loadConfigIngesters(ctx, resp)
		s.loadConfigFilters(ctx, resp)
		s.loadConfigRotationPolicies(ctx, resp)
		s.loadConfigRetentionPolicies(ctx, resp)
		s.loadConfigRoutes(ctx, resp)
		s.loadConfigNodeConfigs(ctx, resp)
		s.loadConfigManagedFiles(ctx, resp)
		s.loadConfigCloudServices(ctx, resp)
		s.loadConfigTiers(ctx, resp)
		s.loadConfigNodeStorageConfigs(ctx, resp)
	}
	if s.configSignal != nil {
		resp.ConfigVersion = s.configSignal.Version()
	}
	return resp
}

func (s *ConfigServer) loadConfigVaults(ctx context.Context, resp *apiv1.GetConfigResponse) {
	cfgStores, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return
	}
	for _, vaultCfg := range cfgStores {
		resp.Vaults = append(resp.Vaults, vaultConfigToProto(vaultCfg))
	}
}

func vaultConfigToProto(vaultCfg config.VaultConfig) *apiv1.VaultConfig {
	vc := &apiv1.VaultConfig{
		Id:      vaultCfg.ID.String(),
		Name:    vaultCfg.Name,
		Enabled: vaultCfg.Enabled,
	}
	for _, tid := range vaultCfg.TierIDs {
		vc.TierIds = append(vc.TierIds, tid.String())
	}
	return vc
}

func (s *ConfigServer) loadConfigIngesters(ctx context.Context, resp *apiv1.GetConfigResponse) {
	ingesters, err := s.cfgStore.ListIngesters(ctx)
	if err != nil {
		return
	}
	for _, ing := range ingesters {
		resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
			Id:      ing.ID.String(),
			Name:    ing.Name,
			Type:    ing.Type,
			Params:  ing.Params,
			Enabled: ing.Enabled,
			NodeId:  ing.NodeID,
		})
	}
}

func (s *ConfigServer) loadConfigFilters(ctx context.Context, resp *apiv1.GetConfigResponse) {
	filters, err := s.cfgStore.ListFilters(ctx)
	if err != nil {
		return
	}
	for _, fc := range filters {
		resp.Filters = append(resp.Filters, &apiv1.FilterConfig{
			Id:         fc.ID.String(),
			Name:       fc.Name,
			Expression: fc.Expression,
		})
	}
}

func (s *ConfigServer) loadConfigRotationPolicies(ctx context.Context, resp *apiv1.GetConfigResponse) {
	policies, err := s.cfgStore.ListRotationPolicies(ctx)
	if err != nil {
		return
	}
	for _, pol := range policies {
		p := rotationPolicyToProto(pol)
		p.Id = pol.ID.String()
		p.Name = pol.Name
		resp.RotationPolicies = append(resp.RotationPolicies, p)
	}
}

func (s *ConfigServer) loadConfigRetentionPolicies(ctx context.Context, resp *apiv1.GetConfigResponse) {
	retPolicies, err := s.cfgStore.ListRetentionPolicies(ctx)
	if err != nil {
		return
	}
	for _, pol := range retPolicies {
		p := retentionPolicyToProto(pol)
		p.Id = pol.ID.String()
		p.Name = pol.Name
		resp.RetentionPolicies = append(resp.RetentionPolicies, p)
	}
}

func (s *ConfigServer) loadConfigRoutes(ctx context.Context, resp *apiv1.GetConfigResponse) {
	routes, err := s.cfgStore.ListRoutes(ctx)
	if err != nil {
		return
	}
	for _, rt := range routes {
		prt := &apiv1.RouteConfig{
			Id:           rt.ID.String(),
			Name:         rt.Name,
			Distribution: rt.Distribution,
			Enabled:      rt.Enabled,
			EjectOnly:    rt.EjectOnly,
		}
		if rt.FilterID != nil {
			prt.FilterId = rt.FilterID.String()
		}
		for _, destID := range rt.Destinations {
			prt.Destinations = append(prt.Destinations, &apiv1.RouteDestination{
				VaultId: destID.String(),
			})
		}
		resp.Routes = append(resp.Routes, prt)
	}
}

func (s *ConfigServer) loadConfigNodeConfigs(ctx context.Context, resp *apiv1.GetConfigResponse) {
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return
	}
	for _, n := range nodes {
		resp.NodeConfigs = append(resp.NodeConfigs, &apiv1.NodeConfig{
			Id:   n.ID.String(),
			Name: n.Name,
		})
	}
}

func (s *ConfigServer) loadConfigCloudServices(ctx context.Context, resp *apiv1.GetConfigResponse) {
	services, err := s.cfgStore.ListCloudServices(ctx)
	if err != nil {
		return
	}
	for _, cs := range services {
		transitions := make([]*apiv1.CloudStorageTransition, len(cs.Transitions))
		for i, t := range cs.Transitions {
			transitions[i] = &apiv1.CloudStorageTransition{
				AfterDays:    t.AfterDays,
				StorageClass: t.StorageClass,
			}
		}
		resp.CloudServices = append(resp.CloudServices, &apiv1.CloudService{
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
		})
	}
}

func (s *ConfigServer) loadConfigTiers(ctx context.Context, resp *apiv1.GetConfigResponse) {
	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return
	}
	for _, tier := range tiers {
		var placements []*apiv1.TierPlacement
		for _, p := range tier.Placements {
			placements = append(placements, &apiv1.TierPlacement{
				StorageId:  p.StorageID,
				Leader: p.Leader,
			})
		}
		tc := &apiv1.TierConfig{
			Id:                tier.ID.String(),
			Name:              tier.Name,
			Type:              tierTypeToProto(tier.Type),
			MemoryBudgetBytes: tier.MemoryBudgetBytes,
			StorageClass:      tier.StorageClass,
			ActiveChunkClass:  tier.ActiveChunkClass,
			CacheClass:        tier.CacheClass,
			ReplicationFactor: tier.ReplicationFactor,
			Path:              tier.Path,
			Placements:        placements,
		}
		if tier.RotationPolicyID != nil {
			tc.RotationPolicyId = tier.RotationPolicyID.String()
		}
		if tier.CloudServiceID != nil {
			tc.CloudServiceId = tier.CloudServiceID.String()
		}
		for _, r := range tier.RetentionRules {
			pb := &apiv1.RetentionRule{
				RetentionPolicyId: r.RetentionPolicyID.String(),
				Action:            string(r.Action),
			}
			for _, eid := range r.EjectRouteIDs {
				pb.EjectRouteIds = append(pb.EjectRouteIds, eid.String())
			}
			tc.RetentionRules = append(tc.RetentionRules, pb)
		}
		resp.Tiers = append(resp.Tiers, tc)
	}
}

func tierTypeToProto(t config.TierType) apiv1.TierType {
	switch t {
	case config.TierTypeMemory:
		return apiv1.TierType_TIER_TYPE_MEMORY
	case config.TierTypeFile:
		return apiv1.TierType_TIER_TYPE_FILE
	case config.TierTypeCloud:
		return apiv1.TierType_TIER_TYPE_CLOUD
	case config.TierTypeJSONL:
		return apiv1.TierType_TIER_TYPE_JSONL
	default:
		return apiv1.TierType_TIER_TYPE_UNSPECIFIED
	}
}

func (s *ConfigServer) loadConfigNodeStorageConfigs(ctx context.Context, resp *apiv1.GetConfigResponse) {
	configs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return
	}
	for _, nsc := range configs {
		storages := make([]*apiv1.FileStorage, len(nsc.FileStorages))
		for i, a := range nsc.FileStorages {
			storages[i] = &apiv1.FileStorage{
				Id:                a.ID.String(),
				StorageClass:      a.StorageClass,
				Name:              a.Name,
				Path:              a.Path,
				MemoryBudgetBytes: a.MemoryBudgetBytes,
			}
		}
		resp.NodeStorageConfigs = append(resp.NodeStorageConfigs, &apiv1.NodeStorageConfig{
			NodeId: nsc.NodeID,
			FileStorages:  storages,
		})
	}
}

// GetSettings returns the server-level configuration.
// Unauthenticated callers (e.g. the registration page) only receive the
// password policy — everything else is stripped to prevent information leakage.
func (s *ConfigServer) GetSettings(
	ctx context.Context,
	req *connect.Request[apiv1.GetSettingsRequest],
) (*connect.Response[apiv1.GetSettingsResponse], error) {
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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

	if req.Msg.IncludeSecrets {
		mm.AccountId = ss.MaxMind.AccountID
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
			MmdbLookups:     mmdbLookupsToProto(ss.Lookup.MMDBLookups),
			CsvLookups:      csvLookupsToProto(ss.Lookup.CSVLookups),
		},
		Maxmind: mm,
		Cluster: &apiv1.ClusterSettings{
			BroadcastInterval: ss.Cluster.BroadcastInterval,
		},
		SetupWizardDismissed: ss.SetupWizardDismissed,
		NodeId:               s.localNodeID,
	}

	if nodeUUID, err := uuid.Parse(s.localNodeID); err == nil {
		if node, err := s.cfgStore.GetNode(ctx, nodeUUID); err == nil && node != nil {
			resp.NodeName = node.Name
		}
	}

	return connect.NewResponse(resp), nil
}

// PutSettings updates the server-level configuration. Merges with existing; only
// fields explicitly set in the request are updated.
func (s *ConfigServer) PutSettings(
	ctx context.Context,
	req *connect.Request[apiv1.PutSettingsRequest],
) (*connect.Response[apiv1.PutSettingsResponse], error) {
	ss, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}

	if connErr := mergeSettingsFields(req.Msg, &ss); connErr != nil {
		return nil, connErr
	}

	if connErr := validateTokenDurations(ss.Auth); connErr != nil {
		return nil, connErr
	}

	if connErr := validateLookupNames(ss.Lookup); connErr != nil {
		return nil, connErr
	}

	if err := s.cfgStore.SaveServerSettings(ctx, ss); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}

	lookupChanged := req.Msg.Lookup != nil || req.Msg.Maxmind != nil
	if s.onLookupConfigChange != nil && lookupChanged {
		s.onLookupConfigChange(ss.Lookup, ss.MaxMind)
	}

	return connect.NewResponse(&apiv1.PutSettingsResponse{}), nil
}

// RegenerateJwtSecret generates a new random JWT signing secret, replacing the
// existing one. All active sessions are immediately invalidated because the old
// secret can no longer verify existing tokens.
func (s *ConfigServer) RegenerateJwtSecret(
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

	if err := s.cfgStore.SaveServerSettings(ctx, ss); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Swap the live signing secret so existing tokens fail verification immediately.
	if s.tokens != nil {
		s.tokens.SetSecret(secret)
	}

	// Invalidate all refresh tokens by setting TokenInvalidatedAt on every user.
	now := time.Now().UTC()
	users, err := s.cfgStore.ListUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list users: %w", err))
	}
	for _, u := range users {
		if err := s.cfgStore.InvalidateTokens(ctx, u.ID, now); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens for %s: %w", u.Username, err))
		}
	}

	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

	return connect.NewResponse(&apiv1.RegenerateJwtSecretResponse{}), nil
}

// PutNodeConfig creates or updates a node configuration.
func (s *ConfigServer) PutNodeConfig(
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
	idStr := cfg.GetId()
	if idStr == "" {
		idStr = s.localNodeID
	}
	nodeUUID, err := uuid.Parse(idStr)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("parse node ID: %w", err))
	}

	// Reject duplicate names.
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if connErr := checkNameConflict("node", nodeUUID, name, nodes, func(n config.NodeConfig) (uuid.UUID, string) { return n.ID, n.Name }); connErr != nil {
		return nil, connErr
	}

	if err := s.cfgStore.PutNode(ctx, config.NodeConfig{ID: nodeUUID, Name: name}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("put node config: %w", err))
	}

	return connect.NewResponse(&apiv1.PutNodeConfigResponse{Config: s.buildFullConfig(ctx)}), nil
}

// GenerateName returns a random petname for use as a default entity name.
func (s *ConfigServer) GenerateName(
	_ context.Context,
	_ *connect.Request[apiv1.GenerateNameRequest],
) (*connect.Response[apiv1.GenerateNameResponse], error) {
	return connect.NewResponse(&apiv1.GenerateNameResponse{
		Name: petname.Generate(2, "-"),
	}), nil
}

// WatchConfig streams a notification whenever configuration changes.
func (s *ConfigServer) WatchConfig(
	ctx context.Context,
	req *connect.Request[apiv1.WatchConfigRequest],
	stream *connect.ServerStream[apiv1.WatchConfigResponse],
) error {
	// Send one initial message so the client knows the stream is alive.
	// Include the current config version so the client can seed its cache.
	initialVersion := uint64(0)
	if s.configSignal != nil {
		initialVersion = s.configSignal.Version()
	}
	if err := stream.Send(&apiv1.WatchConfigResponse{ConfigVersion: initialVersion}); err != nil {
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
			if err := stream.Send(&apiv1.WatchConfigResponse{
				ConfigVersion: s.configSignal.Version(),
			}); err != nil {
				return err
			}
		}
	}
}

func (s *ConfigServer) loadServerSettings(ctx context.Context) (config.ServerSettings, error) {
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return ss, connect.NewError(connect.CodeInternal, err)
	}
	if ss.Auth.PasswordPolicy.MinLength == 0 {
		ss.Auth.PasswordPolicy.MinLength = 8
	}
	if ss.Scheduler.MaxConcurrentJobs == 0 {
		ss.Scheduler.MaxConcurrentJobs = 4
	}
	return ss, nil
}

func mergeSettingsFields(msg *apiv1.PutSettingsRequest, ss *config.ServerSettings) *connect.Error {
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
	if msg.Lookup != nil {
		mergeLookup(msg.Lookup, &ss.Lookup)
	}
	if msg.Maxmind != nil {
		mergeMaxMind(msg.Maxmind, &ss.MaxMind)
	}
	if msg.Cluster != nil {
		if err := mergeCluster(msg.Cluster, &ss.Cluster); err != nil {
			return err
		}
	}
	if msg.SetupWizardDismissed != nil {
		ss.SetupWizardDismissed = *msg.SetupWizardDismissed
	}
	return nil
}

func mergeAuth(a *apiv1.PutAuthSettings, auth *config.AuthConfig) {
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

func mergePasswordPolicy(pp *apiv1.PutPasswordPolicySettings, pol *config.PasswordPolicy) {
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

func mergeQuery(q *apiv1.PutQuerySettings, query *config.QueryConfig) *connect.Error {
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

func mergeScheduler(sc *apiv1.PutSchedulerSettings, sched *config.SchedulerConfig) *connect.Error {
	if sc.MaxConcurrentJobs != nil {
		if *sc.MaxConcurrentJobs < 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_concurrent_jobs must be at least 1, got %d", *sc.MaxConcurrentJobs))
		}
		sched.MaxConcurrentJobs = int(*sc.MaxConcurrentJobs)
	}
	return nil
}

func mergeTLS(t *apiv1.PutTLSSettings, tlsCfg *config.TLSConfig) {
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

func mergeLookup(l *apiv1.PutLookupSettings, lookup *config.LookupConfig) {
	if l.HttpLookups != nil {
		lookup.HTTPLookups = httpLookupsFromProto(l.HttpLookups)
	}
	if l.JsonFileLookups != nil {
		lookup.JSONFileLookups = jsonFileLookupsFromProto(l.JsonFileLookups)
	}
	if l.MmdbLookups != nil {
		lookup.MMDBLookups = mmdbLookupsFromProto(l.MmdbLookups)
	}
	if l.CsvLookups != nil {
		lookup.CSVLookups = csvLookupsFromProto(l.CsvLookups)
	}
}

func mergeMaxMind(mm *apiv1.PutMaxMindSettings, cfg *config.MaxMindConfig) {
	if mm.AutoDownload != nil {
		cfg.AutoDownload = *mm.AutoDownload
	}
	if mm.AccountId != nil {
		cfg.AccountID = *mm.AccountId
	}
	if mm.LicenseKey != nil {
		cfg.LicenseKey = *mm.LicenseKey
	}
}

func httpLookupsToProto(lookups []config.HTTPLookupConfig) []*apiv1.HTTPLookupEntry {
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

func httpLookupsFromProto(entries []*apiv1.HTTPLookupEntry) []config.HTTPLookupConfig {
	out := make([]config.HTTPLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.UrlTemplate == "" {
			continue
		}
		params := make([]config.HTTPLookupParam, len(e.Parameters))
		for j, p := range e.Parameters {
			params[j] = config.HTTPLookupParam{Name: p.Name, Description: p.Description}
		}
		out = append(out, config.HTTPLookupConfig{
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

func jsonFileLookupsToProto(lookups []config.JSONFileLookupConfig) []*apiv1.JSONFileLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.JSONFileLookupEntry, len(lookups))
	for i, l := range lookups {
		params := make([]*apiv1.HTTPLookupParam, len(l.Parameters))
		for j, p := range l.Parameters {
			params[j] = &apiv1.HTTPLookupParam{Name: p.Name, Description: p.Description}
		}
		out[i] = &apiv1.JSONFileLookupEntry{
			Name:          l.Name,
			FileId:        l.FileID,
			Query:         l.Query,
			ResponsePaths: l.ResponsePaths,
			Parameters:    params,
		}
	}
	return out
}

func jsonFileLookupsFromProto(entries []*apiv1.JSONFileLookupEntry) []config.JSONFileLookupConfig {
	out := make([]config.JSONFileLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.FileId == "" {
			continue
		}
		params := make([]config.HTTPLookupParam, len(e.Parameters))
		for j, p := range e.Parameters {
			params[j] = config.HTTPLookupParam{Name: p.Name, Description: p.Description}
		}
		out = append(out, config.JSONFileLookupConfig{
			Name:          e.Name,
			FileID:        e.FileId,
			Query:         e.Query,
			ResponsePaths: e.ResponsePaths,
			Parameters:    params,
		})
	}
	return out
}

func mmdbLookupsToProto(lookups []config.MMDBLookupConfig) []*apiv1.MMDBLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.MMDBLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.MMDBLookupEntry{
			Name:   l.Name,
			DbType: l.DBType,
			FileId: l.FileID,
		}
	}
	return out
}

func mmdbLookupsFromProto(entries []*apiv1.MMDBLookupEntry) []config.MMDBLookupConfig {
	out := make([]config.MMDBLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" {
			continue
		}
		out = append(out, config.MMDBLookupConfig{
			Name:   e.Name,
			DBType: e.DbType,
			FileID: e.FileId,
		})
	}
	return out
}

func csvLookupsToProto(lookups []config.CSVLookupConfig) []*apiv1.CSVLookupEntry {
	if len(lookups) == 0 {
		return nil
	}
	out := make([]*apiv1.CSVLookupEntry, len(lookups))
	for i, l := range lookups {
		out[i] = &apiv1.CSVLookupEntry{
			Name:         l.Name,
			FileId:       l.FileID,
			KeyColumn:    l.KeyColumn,
			ValueColumns: l.ValueColumns,
		}
	}
	return out
}

func csvLookupsFromProto(entries []*apiv1.CSVLookupEntry) []config.CSVLookupConfig {
	out := make([]config.CSVLookupConfig, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.FileId == "" {
			continue
		}
		out = append(out, config.CSVLookupConfig{
			Name:         e.Name,
			FileID:       e.FileId,
			KeyColumn:    e.KeyColumn,
			ValueColumns: e.ValueColumns,
		})
	}
	return out
}

func mergeCluster(c *apiv1.PutClusterSettings, cluster *config.ClusterConfig) *connect.Error {
	if c.BroadcastInterval != nil {
		if _, err := time.ParseDuration(*c.BroadcastInterval); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid broadcast_interval: %w", err))
		}
		cluster.BroadcastInterval = *c.BroadcastInterval
	}
	return nil
}

func validateTokenDurations(auth config.AuthConfig) *connect.Error {
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
func validateLookupNames(lc config.LookupConfig) *connect.Error {
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
func (s *ConfigServer) TestHTTPLookup(
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
func (s *ConfigServer) PreviewCSVLookup(
	ctx context.Context,
	req *connect.Request[apiv1.PreviewCSVLookupRequest],
) (*connect.Response[apiv1.PreviewCSVLookupResponse], error) {
	fileID := req.Msg.GetFileId()
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

// checkNameConflict returns an AlreadyExists error if another entity of the
// same type already has the given name. Empty names are allowed to coexist.
func checkNameConflict[S ~[]E, E any](entityType string, id uuid.UUID, name string, existing S, identify func(E) (uuid.UUID, string)) *connect.Error {
	for _, e := range existing {
		eid, ename := identify(e)
		if eid != id && ename == name {
			return connect.NewError(connect.CodeAlreadyExists,
				fmt.Errorf("%s name %q is already in use", entityType, name))
		}
	}
	return nil
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(b uint64) string {
	switch {
	case b >= 1024*1024*1024 && b%(1024*1024*1024) == 0:
		return fmt.Sprintf("%dGB", b/(1024*1024*1024))
	case b >= 1024*1024 && b%(1024*1024) == 0:
		return fmt.Sprintf("%dMB", b/(1024*1024))
	case b >= 1024 && b%1024 == 0:
		return fmt.Sprintf("%dKB", b/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}
