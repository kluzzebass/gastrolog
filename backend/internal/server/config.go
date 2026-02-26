package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
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

// GetServerConfig returns the server-level configuration.
func (s *ConfigServer) GetServerConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetServerConfigRequest],
) (*connect.Response[apiv1.GetServerConfigResponse], error) {
	resp := &apiv1.GetServerConfigResponse{}

	raw, err := s.cfgStore.GetSetting(ctx, "server")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if raw != nil {
		var sc config.ServerConfig
		if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse server config: %w", err))
		}
		resp.TokenDuration = sc.Auth.TokenDuration
		resp.JwtSecretConfigured = sc.Auth.JWTSecret != ""
		resp.MinPasswordLength = int32(sc.Auth.MinPasswordLength)       //nolint:gosec // G115: small config value, always fits in int32
		resp.MaxConcurrentJobs = int32(sc.Scheduler.MaxConcurrentJobs) //nolint:gosec // G115: small config value, always fits in int32
		resp.TlsDefaultCert = sc.TLS.DefaultCert
		resp.TlsEnabled = sc.TLS.TLSEnabled
		resp.HttpToHttpsRedirect = sc.TLS.HTTPToHTTPSRedirect
		resp.HttpsPort = sc.TLS.HTTPSPort
		resp.RequireMixedCase = sc.Auth.RequireMixedCase
		resp.RequireDigit = sc.Auth.RequireDigit
		resp.RequireSpecial = sc.Auth.RequireSpecial
		resp.MaxConsecutiveRepeats = int32(sc.Auth.MaxConsecutiveRepeats) //nolint:gosec // G115: small config value, always fits in int32
		resp.ForbidAnimalNoise = sc.Auth.ForbidAnimalNoise
		resp.MaxFollowDuration = sc.Query.MaxFollowDuration
		resp.QueryTimeout = sc.Query.Timeout
		resp.RefreshTokenDuration = sc.Auth.RefreshTokenDuration
		resp.MaxResultCount = int32(sc.Query.MaxResultCount) //nolint:gosec // G115: small config value, always fits in int32
		resp.SetupWizardDismissed = sc.SetupWizardDismissed
		resp.GeoipDbPath = sc.Lookup.GeoIPDBPath
		resp.AsnDbPath = sc.Lookup.ASNDBPath
		resp.MaxmindAutoDownload = sc.Lookup.MaxMindAutoDownload
		resp.MaxmindLicenseConfigured = sc.Lookup.MaxMindAccountID != "" && sc.Lookup.MaxMindLicenseKey != ""
		if !sc.Lookup.MaxMindLastUpdate.IsZero() {
			resp.MaxmindLastUpdate = sc.Lookup.MaxMindLastUpdate.Format(time.RFC3339)
		}
	}

	// If no persisted value, report the live default from the orchestrator.
	if resp.MaxConcurrentJobs == 0 {
		resp.MaxConcurrentJobs = int32(s.orch.MaxConcurrentJobs()) //nolint:gosec // G115: small config value, always fits in int32
	}

	// Populate node identity.
	resp.NodeId = s.localNodeID
	if nodeUUID, err := uuid.Parse(s.localNodeID); err == nil {
		if node, err := s.cfgStore.GetNode(ctx, nodeUUID); err == nil && node != nil {
			resp.NodeName = node.Name
		}
	}

	return connect.NewResponse(resp), nil
}

// PutServerConfig updates the server-level configuration. Merges with existing; only
// fields explicitly set in the request are updated.
func (s *ConfigServer) PutServerConfig(
	ctx context.Context,
	req *connect.Request[apiv1.PutServerConfigRequest],
) (*connect.Response[apiv1.PutServerConfigResponse], error) {
	sc, err := s.loadServerConfig(ctx)
	if err != nil {
		return nil, err
	}

	if err := mergeServerConfigFields(req.Msg, &sc); err != nil {
		return nil, err
	}

	if err := validateTokenDurations(sc.Auth); err != nil {
		return nil, err
	}

	data, err := json.Marshal(sc)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.cfgStore.PutSetting(ctx, "server", string(data)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}

	lookupChanged := req.Msg.GeoipDbPath != nil || req.Msg.AsnDbPath != nil ||
		req.Msg.MaxmindAutoDownload != nil || req.Msg.MaxmindAccountId != nil || req.Msg.MaxmindLicenseKey != nil
	if s.onLookupConfigChange != nil && lookupChanged {
		s.onLookupConfigChange(sc.Lookup)
	}

	resp := &apiv1.PutServerConfigResponse{}

	// Validate MMDB paths that were explicitly set in this request.
	if req.Msg.GeoipDbPath != nil && *req.Msg.GeoipDbPath != "" {
		resp.GeoipValidation = validateMMDBPath(*req.Msg.GeoipDbPath)
	}
	if req.Msg.AsnDbPath != nil && *req.Msg.AsnDbPath != "" {
		resp.AsnValidation = validateMMDBPath(*req.Msg.AsnDbPath)
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

// PutNodeName updates the human-readable name for the current node.
func (s *ConfigServer) PutNodeName(
	ctx context.Context,
	req *connect.Request[apiv1.PutNodeNameRequest],
) (*connect.Response[apiv1.PutNodeNameResponse], error) {
	name := req.Msg.GetNodeName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_name must not be empty"))
	}

	nodeUUID, err := uuid.Parse(s.localNodeID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse local node ID: %w", err))
	}

	if err := s.cfgStore.PutNode(ctx, config.NodeInfo{ID: nodeUUID, Name: name}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("put node: %w", err))
	}

	return connect.NewResponse(&apiv1.PutNodeNameResponse{}), nil
}

func (s *ConfigServer) loadServerConfig(ctx context.Context) (config.ServerConfig, error) {
	raw, err := s.cfgStore.GetSetting(ctx, "server")
	if err != nil {
		return config.ServerConfig{}, connect.NewError(connect.CodeInternal, err)
	}
	var sc config.ServerConfig
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
			return config.ServerConfig{}, connect.NewError(connect.CodeInternal, fmt.Errorf("parse server config: %w", err))
		}
	}
	if sc.Auth.MinPasswordLength == 0 {
		sc.Auth.MinPasswordLength = 8
	}
	if sc.Scheduler.MaxConcurrentJobs == 0 {
		sc.Scheduler.MaxConcurrentJobs = 4
	}
	return sc, nil
}

func mergeServerConfigFields(msg *apiv1.PutServerConfigRequest, sc *config.ServerConfig) *connect.Error {
	if msg.TokenDuration != nil {
		sc.Auth.TokenDuration = *msg.TokenDuration
	}
	if msg.JwtSecret != nil {
		sc.Auth.JWTSecret = *msg.JwtSecret
	}
	if msg.MinPasswordLength != nil {
		sc.Auth.MinPasswordLength = int(*msg.MinPasswordLength)
	}
	if msg.MaxConcurrentJobs != nil {
		if *msg.MaxConcurrentJobs < 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_concurrent_jobs must be at least 1, got %d", *msg.MaxConcurrentJobs))
		}
		sc.Scheduler.MaxConcurrentJobs = int(*msg.MaxConcurrentJobs)
	}
	if msg.TlsDefaultCert != nil {
		sc.TLS.DefaultCert = *msg.TlsDefaultCert
	}
	if msg.TlsEnabled != nil {
		sc.TLS.TLSEnabled = *msg.TlsEnabled && sc.TLS.DefaultCert != ""
	}
	if msg.HttpToHttpsRedirect != nil {
		sc.TLS.HTTPToHTTPSRedirect = *msg.HttpToHttpsRedirect && sc.TLS.DefaultCert != ""
	}
	if msg.HttpsPort != nil {
		sc.TLS.HTTPSPort = *msg.HttpsPort
	}
	if msg.RequireMixedCase != nil {
		sc.Auth.RequireMixedCase = *msg.RequireMixedCase
	}
	if msg.RequireDigit != nil {
		sc.Auth.RequireDigit = *msg.RequireDigit
	}
	if msg.RequireSpecial != nil {
		sc.Auth.RequireSpecial = *msg.RequireSpecial
	}
	if msg.MaxConsecutiveRepeats != nil {
		sc.Auth.MaxConsecutiveRepeats = int(*msg.MaxConsecutiveRepeats)
	}
	if msg.ForbidAnimalNoise != nil {
		sc.Auth.ForbidAnimalNoise = *msg.ForbidAnimalNoise
	}
	if msg.MaxFollowDuration != nil {
		sc.Query.MaxFollowDuration = *msg.MaxFollowDuration
	}
	if msg.QueryTimeout != nil {
		sc.Query.Timeout = *msg.QueryTimeout
	}
	if msg.RefreshTokenDuration != nil {
		sc.Auth.RefreshTokenDuration = *msg.RefreshTokenDuration
	}
	if msg.MaxResultCount != nil {
		if *msg.MaxResultCount < 0 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_result_count must be non-negative, got %d", *msg.MaxResultCount))
		}
		sc.Query.MaxResultCount = int(*msg.MaxResultCount)
	}
	if msg.SetupWizardDismissed != nil {
		sc.SetupWizardDismissed = *msg.SetupWizardDismissed
	}
	if msg.GeoipDbPath != nil {
		sc.Lookup.GeoIPDBPath = *msg.GeoipDbPath
	}
	if msg.AsnDbPath != nil {
		sc.Lookup.ASNDBPath = *msg.AsnDbPath
	}
	if msg.MaxmindAutoDownload != nil {
		sc.Lookup.MaxMindAutoDownload = *msg.MaxmindAutoDownload
	}
	if msg.MaxmindAccountId != nil {
		sc.Lookup.MaxMindAccountID = *msg.MaxmindAccountId
	}
	if msg.MaxmindLicenseKey != nil {
		sc.Lookup.MaxMindLicenseKey = *msg.MaxmindLicenseKey
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
