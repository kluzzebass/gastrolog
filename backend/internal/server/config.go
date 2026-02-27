package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/lookup"
	"gastrolog/internal/orchestrator"
)

// ConfigServer implements the ConfigService.
type ConfigServer struct {
	orch                  *orchestrator.Orchestrator
	cfgStore              config.Store
	factories             orchestrator.Factories
	certManager           CertManager
	localNodeID           string
	onTLSConfigChange     func()
	onLookupConfigChange  func(config.LookupConfig)
	afterConfigApply      func(raftfsm.Notification)
}

var _ gastrologv1connect.ConfigServiceHandler = (*ConfigServer)(nil)

// NewConfigServer creates a new ConfigServer.
func NewConfigServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, certManager CertManager, localNodeID string, afterConfigApply func(raftfsm.Notification)) *ConfigServer {
	return &ConfigServer{
		orch:             orch,
		cfgStore:         cfgStore,
		factories:        factories,
		certManager:      certManager,
		localNodeID:      localNodeID,
		afterConfigApply: afterConfigApply,
	}
}

// notify fires the afterConfigApply callback if set.
func (s *ConfigServer) notify(n raftfsm.Notification) {
	if s.afterConfigApply != nil {
		s.afterConfigApply(n)
	}
}

// SetOnTLSConfigChange sets a callback invoked when TLS config changes (for dynamic listener reconfig).
func (s *ConfigServer) SetOnTLSConfigChange(fn func()) {
	s.onTLSConfigChange = fn
}

// SetOnLookupConfigChange sets a callback invoked when lookup config changes (e.g. GeoIP DB path).
func (s *ConfigServer) SetOnLookupConfigChange(fn func(config.LookupConfig)) {
	s.onLookupConfigChange = fn
}

// GetConfig returns the current configuration.
func (s *ConfigServer) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	resp := &apiv1.GetConfigResponse{}
	if s.cfgStore != nil {
		s.loadConfigVaults(ctx, resp)
		s.loadConfigIngesters(ctx, resp)
		s.loadConfigFilters(ctx, resp)
		s.loadConfigRotationPolicies(ctx, resp)
		s.loadConfigRetentionPolicies(ctx, resp)
		s.loadConfigNodeConfigs(ctx, resp)
	}
	return connect.NewResponse(resp), nil
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
		Type:    vaultCfg.Type,
		Params:  vaultCfg.Params,
		Enabled: vaultCfg.Enabled,
		NodeId:  vaultCfg.NodeID,
	}
	if vaultCfg.Filter != nil {
		vc.Filter = vaultCfg.Filter.String()
	}
	if vaultCfg.Policy != nil {
		vc.Policy = vaultCfg.Policy.String()
	}
	for _, b := range vaultCfg.RetentionRules {
		pb := &apiv1.RetentionRule{
			RetentionPolicyId: b.RetentionPolicyID.String(),
			Action:            string(b.Action),
		}
		if b.Destination != nil {
			pb.DestinationId = b.Destination.String()
		}
		vc.RetentionRules = append(vc.RetentionRules, pb)
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

// GetSettings returns the server-level configuration.
func (s *ConfigServer) GetSettings(
	ctx context.Context,
	req *connect.Request[apiv1.GetSettingsRequest],
) (*connect.Response[apiv1.GetSettingsResponse], error) {
	authCfg, queryCfg, schedCfg, tlsCfg, lookupCfg, setupDismissed, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	maxJobs := int32(schedCfg.MaxConcurrentJobs) //nolint:gosec // G115: small config value, always fits in int32
	if maxJobs == 0 {
		maxJobs = int32(s.orch.MaxConcurrentJobs()) //nolint:gosec // G115: small config value, always fits in int32
	}

	mm := &apiv1.MaxMindSettings{
		AutoDownload:     lookupCfg.MaxMind.AutoDownload,
		LicenseConfigured: lookupCfg.MaxMind.AccountID != "" && lookupCfg.MaxMind.LicenseKey != "",
	}
	if !lookupCfg.MaxMind.LastUpdate.IsZero() {
		mm.LastUpdate = lookupCfg.MaxMind.LastUpdate.Format(time.RFC3339)
	}

	auth := &apiv1.AuthSettings{
		TokenDuration:        authCfg.TokenDuration,
		JwtSecretConfigured:  authCfg.JWTSecret != "",
		RefreshTokenDuration: authCfg.RefreshTokenDuration,
		PasswordPolicy: &apiv1.PasswordPolicySettings{
			MinLength:             int32(authCfg.PasswordPolicy.MinLength),             //nolint:gosec // G115
			RequireMixedCase:      authCfg.PasswordPolicy.RequireMixedCase,
			RequireDigit:          authCfg.PasswordPolicy.RequireDigit,
			RequireSpecial:        authCfg.PasswordPolicy.RequireSpecial,
			MaxConsecutiveRepeats: int32(authCfg.PasswordPolicy.MaxConsecutiveRepeats), //nolint:gosec // G115
			ForbidAnimalNoise:     authCfg.PasswordPolicy.ForbidAnimalNoise,
		},
	}

	if req.Msg.IncludeSecrets {
		auth.JwtSecret = authCfg.JWTSecret
		mm.AccountId = lookupCfg.MaxMind.AccountID
		mm.LicenseKey = lookupCfg.MaxMind.LicenseKey
	}

	resp := &apiv1.GetSettingsResponse{
		Auth: auth,
		Query: &apiv1.QuerySettings{
			Timeout:           queryCfg.Timeout,
			MaxFollowDuration: queryCfg.MaxFollowDuration,
			MaxResultCount:    int32(queryCfg.MaxResultCount), //nolint:gosec // G115
		},
		Scheduler: &apiv1.SchedulerSettings{
			MaxConcurrentJobs: maxJobs,
		},
		Tls: &apiv1.TLSSettings{
			DefaultCert:         tlsCfg.DefaultCert,
			Enabled:             tlsCfg.TLSEnabled,
			HttpToHttpsRedirect: tlsCfg.HTTPToHTTPSRedirect,
			HttpsPort:           tlsCfg.HTTPSPort,
		},
		Lookup: &apiv1.LookupSettings{
			GeoipDbPath: lookupCfg.GeoIPDBPath,
			AsnDbPath:   lookupCfg.ASNDBPath,
			Maxmind:     mm,
		},
		SetupWizardDismissed: setupDismissed,
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
	authCfg, queryCfg, schedCfg, tlsCfg, lookupCfg, setupDismissed, err := s.loadServerSettings(ctx)
	if err != nil {
		return nil, err
	}

	if connErr := mergeSettingsFields(req.Msg, &authCfg, &queryCfg, &schedCfg, &tlsCfg, &lookupCfg, &setupDismissed); connErr != nil {
		return nil, connErr
	}

	if connErr := validateTokenDurations(authCfg); connErr != nil {
		return nil, connErr
	}

	if err := s.cfgStore.SaveServerSettings(ctx, authCfg, queryCfg, schedCfg, tlsCfg, lookupCfg, setupDismissed); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}

	lookupChanged := req.Msg.Lookup != nil
	if s.onLookupConfigChange != nil && lookupChanged {
		s.onLookupConfigChange(lookupCfg)
	}

	resp := &apiv1.PutSettingsResponse{}

	// Validate MMDB paths that were explicitly set in this request.
	if l := req.Msg.Lookup; l != nil {
		if l.GeoipDbPath != nil && *l.GeoipDbPath != "" {
			resp.GeoipValidation = validateMMDBPath(*l.GeoipDbPath)
		}
		if l.AsnDbPath != nil && *l.AsnDbPath != "" {
			resp.AsnValidation = validateMMDBPath(*l.AsnDbPath)
		}
	}

	return connect.NewResponse(resp), nil
}

func validateMMDBPath(path string) *apiv1.MmdbValidation {
	info, err := lookup.ValidateMMDB(path)
	if err != nil {
		return &apiv1.MmdbValidation{Error: err.Error()}
	}
	return &apiv1.MmdbValidation{
		Valid:        true,
		DatabaseType: info.DatabaseType,
		BuildTime:    info.BuildTime.Format(time.RFC3339),
		NodeCount:    uint32(info.NodeCount), //nolint:gosec // NodeCount fits in uint32 for all real MMDB files
	}
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

	if err := s.cfgStore.PutNode(ctx, config.NodeConfig{ID: nodeUUID, Name: name}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("put node config: %w", err))
	}

	return connect.NewResponse(&apiv1.PutNodeConfigResponse{}), nil
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

func (s *ConfigServer) loadServerSettings(ctx context.Context) (config.AuthConfig, config.QueryConfig, config.SchedulerConfig, config.TLSConfig, config.LookupConfig, bool, error) {
	auth, query, sched, tls, lookup, dismissed, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return auth, query, sched, tls, lookup, dismissed, connect.NewError(connect.CodeInternal, err)
	}
	if auth.PasswordPolicy.MinLength == 0 {
		auth.PasswordPolicy.MinLength = 8
	}
	if sched.MaxConcurrentJobs == 0 {
		sched.MaxConcurrentJobs = 4
	}
	return auth, query, sched, tls, lookup, dismissed, nil
}

func mergeSettingsFields(msg *apiv1.PutSettingsRequest, auth *config.AuthConfig, query *config.QueryConfig, sched *config.SchedulerConfig, tlsCfg *config.TLSConfig, lookup *config.LookupConfig, setupDismissed *bool) *connect.Error {
	if msg.Auth != nil {
		mergeAuth(msg.Auth, auth)
	}
	if msg.Query != nil {
		if err := mergeQuery(msg.Query, query); err != nil {
			return err
		}
	}
	if msg.Scheduler != nil {
		if err := mergeScheduler(msg.Scheduler, sched); err != nil {
			return err
		}
	}
	if msg.Tls != nil {
		mergeTLS(msg.Tls, tlsCfg)
	}
	if msg.Lookup != nil {
		mergeLookup(msg.Lookup, lookup)
	}
	if msg.SetupWizardDismissed != nil {
		*setupDismissed = *msg.SetupWizardDismissed
	}
	return nil
}

func mergeAuth(a *apiv1.PutAuthSettings, auth *config.AuthConfig) {
	if a.TokenDuration != nil {
		auth.TokenDuration = *a.TokenDuration
	}
	if a.JwtSecret != nil {
		auth.JWTSecret = *a.JwtSecret
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
	if l.GeoipDbPath != nil {
		lookup.GeoIPDBPath = *l.GeoipDbPath
	}
	if l.AsnDbPath != nil {
		lookup.ASNDBPath = *l.AsnDbPath
	}
	if mm := l.Maxmind; mm != nil {
		if mm.AutoDownload != nil {
			lookup.MaxMind.AutoDownload = *mm.AutoDownload
		}
		if mm.AccountId != nil {
			lookup.MaxMind.AccountID = *mm.AccountId
		}
		if mm.LicenseKey != nil {
			lookup.MaxMind.LicenseKey = *mm.LicenseKey
		}
	}
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
