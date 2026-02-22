package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"
)

// ConfigServer implements the ConfigService.
type ConfigServer struct {
	orch              *orchestrator.Orchestrator
	cfgStore          config.Store
	factories         orchestrator.Factories
	certManager       CertManager
	onTLSConfigChange func()
}

var _ gastrologv1connect.ConfigServiceHandler = (*ConfigServer)(nil)

// NewConfigServer creates a new ConfigServer.
func NewConfigServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, certManager CertManager) *ConfigServer {
	return &ConfigServer{
		orch:        orch,
		cfgStore:    cfgStore,
		factories:   factories,
		certManager: certManager,
	}
}

// SetOnTLSConfigChange sets a callback invoked when TLS config changes (for dynamic listener reconfig).
func (s *ConfigServer) SetOnTLSConfigChange(fn func()) {
	s.onTLSConfigChange = fn
}

// GetConfig returns the current configuration.
func (s *ConfigServer) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	resp := &apiv1.GetConfigResponse{}

	if s.cfgStore != nil {
		// Get store configs from config store.
		cfgStores, err := s.cfgStore.ListStores(ctx)
		if err == nil {
			for _, storeCfg := range cfgStores {
				sc := &apiv1.StoreConfig{
					Id:      storeCfg.ID.String(),
					Name:    storeCfg.Name,
					Type:    storeCfg.Type,
					Params:  storeCfg.Params,
					Enabled: storeCfg.Enabled,
				}
				if storeCfg.Filter != nil {
					sc.Filter = storeCfg.Filter.String()
				}
				if storeCfg.Policy != nil {
					sc.Policy = storeCfg.Policy.String()
				}
				for _, b := range storeCfg.RetentionRules {
					pb := &apiv1.RetentionRule{
						RetentionPolicyId: b.RetentionPolicyID.String(),
						Action:            string(b.Action),
					}
					if b.Destination != nil {
						pb.DestinationId = b.Destination.String()
					}
					sc.RetentionRules = append(sc.RetentionRules, pb)
				}
				resp.Stores = append(resp.Stores, sc)
			}
		}

		// Get ingester configs from config store.
		ingesters, err := s.cfgStore.ListIngesters(ctx)
		if err == nil {
			for _, ing := range ingesters {
				resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
					Id:      ing.ID.String(),
					Name:    ing.Name,
					Type:    ing.Type,
					Params:  ing.Params,
					Enabled: ing.Enabled,
				})
			}
		}

		// Get filters from config store.
		filters, err := s.cfgStore.ListFilters(ctx)
		if err == nil {
			for _, fc := range filters {
				resp.Filters = append(resp.Filters, &apiv1.FilterConfig{
					Id:         fc.ID.String(),
					Name:       fc.Name,
					Expression: fc.Expression,
				})
			}
		}

		// Get rotation policies from config store.
		policies, err := s.cfgStore.ListRotationPolicies(ctx)
		if err == nil {
			for _, pol := range policies {
				p := rotationPolicyToProto(pol)
				p.Id = pol.ID.String()
				p.Name = pol.Name
				resp.RotationPolicies = append(resp.RotationPolicies, p)
			}
		}

		// Get retention policies from config store.
		retPolicies, err := s.cfgStore.ListRetentionPolicies(ctx)
		if err == nil {
			for _, pol := range retPolicies {
				p := retentionPolicyToProto(pol)
				p.Id = pol.ID.String()
				p.Name = pol.Name
				resp.RetentionPolicies = append(resp.RetentionPolicies, p)
			}
		}
	}

	return connect.NewResponse(resp), nil
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
		resp.MinPasswordLength = int32(sc.Auth.MinPasswordLength)
		resp.MaxConcurrentJobs = int32(sc.Scheduler.MaxConcurrentJobs)
		resp.TlsDefaultCert = sc.TLS.DefaultCert
		resp.TlsEnabled = sc.TLS.TLSEnabled
		resp.HttpToHttpsRedirect = sc.TLS.HTTPToHTTPSRedirect
		resp.HttpsPort = sc.TLS.HTTPSPort
		resp.RequireMixedCase = sc.Auth.RequireMixedCase
		resp.RequireDigit = sc.Auth.RequireDigit
		resp.RequireSpecial = sc.Auth.RequireSpecial
		resp.MaxConsecutiveRepeats = int32(sc.Auth.MaxConsecutiveRepeats)
		resp.ForbidAnimalNoise = sc.Auth.ForbidAnimalNoise
		resp.MaxFollowDuration = sc.Query.MaxFollowDuration
		resp.QueryTimeout = sc.Query.Timeout
		resp.RefreshTokenDuration = sc.Auth.RefreshTokenDuration
		resp.MaxResultCount = int32(sc.Query.MaxResultCount)
		resp.SetupWizardDismissed = sc.SetupWizardDismissed
	}

	// If no persisted value, report the live default from the orchestrator.
	if resp.MaxConcurrentJobs == 0 {
		resp.MaxConcurrentJobs = int32(s.orch.MaxConcurrentJobs())
	}

	return connect.NewResponse(resp), nil
}

// PutServerConfig updates the server-level configuration. Merges with existing; only
// fields explicitly set in the request are updated.
func (s *ConfigServer) PutServerConfig(
	ctx context.Context,
	req *connect.Request[apiv1.PutServerConfigRequest],
) (*connect.Response[apiv1.PutServerConfigResponse], error) {
	// Load existing config and merge
	raw, err := s.cfgStore.GetSetting(ctx, "server")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	var sc config.ServerConfig
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse server config: %w", err))
		}
	}
	if sc.Auth.MinPasswordLength == 0 {
		sc.Auth.MinPasswordLength = 8
	}
	if sc.Scheduler.MaxConcurrentJobs == 0 {
		sc.Scheduler.MaxConcurrentJobs = 4
	}

	// Merge only explicitly set fields
	if req.Msg.TokenDuration != nil {
		sc.Auth.TokenDuration = *req.Msg.TokenDuration
	}
	if req.Msg.JwtSecret != nil {
		sc.Auth.JWTSecret = *req.Msg.JwtSecret
	}
	if req.Msg.MinPasswordLength != nil {
		sc.Auth.MinPasswordLength = int(*req.Msg.MinPasswordLength)
	}
	if req.Msg.MaxConcurrentJobs != nil {
		if *req.Msg.MaxConcurrentJobs < 1 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_concurrent_jobs must be at least 1, got %d", *req.Msg.MaxConcurrentJobs))
		}
		sc.Scheduler.MaxConcurrentJobs = int(*req.Msg.MaxConcurrentJobs)
	}
	if req.Msg.TlsDefaultCert != nil {
		sc.TLS.DefaultCert = *req.Msg.TlsDefaultCert
	}
	if req.Msg.TlsEnabled != nil {
		sc.TLS.TLSEnabled = *req.Msg.TlsEnabled && sc.TLS.DefaultCert != ""
	}
	if req.Msg.HttpToHttpsRedirect != nil {
		sc.TLS.HTTPToHTTPSRedirect = *req.Msg.HttpToHttpsRedirect && sc.TLS.DefaultCert != ""
	}
	if req.Msg.HttpsPort != nil {
		sc.TLS.HTTPSPort = *req.Msg.HttpsPort
	}
	if req.Msg.RequireMixedCase != nil {
		sc.Auth.RequireMixedCase = *req.Msg.RequireMixedCase
	}
	if req.Msg.RequireDigit != nil {
		sc.Auth.RequireDigit = *req.Msg.RequireDigit
	}
	if req.Msg.RequireSpecial != nil {
		sc.Auth.RequireSpecial = *req.Msg.RequireSpecial
	}
	if req.Msg.MaxConsecutiveRepeats != nil {
		sc.Auth.MaxConsecutiveRepeats = int(*req.Msg.MaxConsecutiveRepeats)
	}
	if req.Msg.ForbidAnimalNoise != nil {
		sc.Auth.ForbidAnimalNoise = *req.Msg.ForbidAnimalNoise
	}
	if req.Msg.MaxFollowDuration != nil {
		sc.Query.MaxFollowDuration = *req.Msg.MaxFollowDuration
	}
	if req.Msg.QueryTimeout != nil {
		sc.Query.Timeout = *req.Msg.QueryTimeout
	}
	if req.Msg.RefreshTokenDuration != nil {
		sc.Auth.RefreshTokenDuration = *req.Msg.RefreshTokenDuration
	}
	if req.Msg.MaxResultCount != nil {
		if *req.Msg.MaxResultCount < 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_result_count must be non-negative, got %d", *req.Msg.MaxResultCount))
		}
		sc.Query.MaxResultCount = int(*req.Msg.MaxResultCount)
	}
	if req.Msg.SetupWizardDismissed != nil {
		sc.SetupWizardDismissed = *req.Msg.SetupWizardDismissed
	}

	// Validate token durations.
	var tokenDur, refreshDur time.Duration
	if sc.Auth.TokenDuration != "" {
		d, err := time.ParseDuration(sc.Auth.TokenDuration)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid token duration %q: %w", sc.Auth.TokenDuration, err))
		}
		if d < time.Minute {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("token duration must be at least 1 minute, got %s", sc.Auth.TokenDuration))
		}
		tokenDur = d
	}
	if sc.Auth.RefreshTokenDuration != "" {
		d, err := time.ParseDuration(sc.Auth.RefreshTokenDuration)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid refresh token duration %q: %w", sc.Auth.RefreshTokenDuration, err))
		}
		if d < time.Hour {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("refresh token duration must be at least 1 hour, got %s", sc.Auth.RefreshTokenDuration))
		}
		refreshDur = d
	}
	if tokenDur > 0 && refreshDur > 0 && refreshDur <= tokenDur {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("refresh token duration (%s) must be longer than token duration (%s)", sc.Auth.RefreshTokenDuration, sc.Auth.TokenDuration))
	}

	data, err := json.Marshal(sc)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.cfgStore.PutSetting(ctx, "server", string(data)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Hot-reload the scheduler concurrency limit.
	if sc.Scheduler.MaxConcurrentJobs > 0 {
		if err := s.orch.UpdateMaxConcurrentJobs(sc.Scheduler.MaxConcurrentJobs); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update scheduler: %w", err))
		}
	}

	// TLS settings changed; notify server for dynamic listener reconfig.
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}

	return connect.NewResponse(&apiv1.PutServerConfigResponse{}), nil
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
