package server

import (
	"context"
	"errors"
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
	orch      *orchestrator.Orchestrator
	cfgStore  config.Store
	factories orchestrator.Factories
}

var _ gastrologv1connect.ConfigServiceHandler = (*ConfigServer)(nil)

// NewConfigServer creates a new ConfigServer.
func NewConfigServer(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories) *ConfigServer {
	return &ConfigServer{
		orch:      orch,
		cfgStore:  cfgStore,
		factories: factories,
	}
}

// GetConfig returns the current configuration.
func (s *ConfigServer) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	resp := &apiv1.GetConfigResponse{
		Stores:           make([]*apiv1.StoreConfig, 0),
		Ingesters:        make([]*apiv1.IngesterConfig, 0),
		RotationPolicies: make(map[string]*apiv1.RotationPolicyConfig),
	}

	if s.cfgStore != nil {
		// Get store configs from config store (has full type/params),
		// then merge live filter from orchestrator.
		cfgStores, err := s.cfgStore.ListStores(ctx)
		if err == nil {
			for _, storeCfg := range cfgStores {
				sc := &apiv1.StoreConfig{
					Id:     storeCfg.ID,
					Type:   storeCfg.Type,
					Params: storeCfg.Params,
				}
				if storeCfg.Filter != nil {
					sc.Filter = *storeCfg.Filter
				}
				if storeCfg.Policy != nil {
					sc.Policy = *storeCfg.Policy
				}
				// Override filter with live value from orchestrator if available.
				if liveCfg, err := s.orch.StoreConfig(storeCfg.ID); err == nil && liveCfg.Filter != nil {
					sc.Filter = *liveCfg.Filter
				}
				resp.Stores = append(resp.Stores, sc)
			}
		}

		// Get ingester configs from config store for full type/params info.
		ingesters, err := s.cfgStore.ListIngesters(ctx)
		if err == nil {
			for _, ing := range ingesters {
				resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
					Id:     ing.ID,
					Type:   ing.Type,
					Params: ing.Params,
				})
			}
		}

		// Get rotation policies from config store.
		policies, err := s.cfgStore.ListRotationPolicies(ctx)
		if err == nil {
			for id, pol := range policies {
				resp.RotationPolicies[id] = rotationPolicyToProto(pol)
			}
		}
	}

	return connect.NewResponse(resp), nil
}

// UpdateStoreFilter updates a store's filter expression.
func (s *ConfigServer) UpdateStoreFilter(
	ctx context.Context,
	req *connect.Request[apiv1.UpdateStoreFilterRequest],
) (*connect.Response[apiv1.UpdateStoreFilterResponse], error) {
	if req.Msg.StoreId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store_id required"))
	}

	if err := s.orch.UpdateStoreFilter(req.Msg.StoreId, req.Msg.Filter); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	return connect.NewResponse(&apiv1.UpdateStoreFilterResponse{}), nil
}

// ListIngesters returns all registered ingesters.
func (s *ConfigServer) ListIngesters(
	ctx context.Context,
	req *connect.Request[apiv1.ListIngestersRequest],
) (*connect.Response[apiv1.ListIngestersResponse], error) {
	ids := s.orch.ListIngesters()

	resp := &apiv1.ListIngestersResponse{
		Ingesters: make([]*apiv1.IngesterInfo, 0, len(ids)),
	}

	for _, id := range ids {
		resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterInfo{
			Id:      id,
			Running: s.orch.IsRunning(),
		})
	}

	return connect.NewResponse(resp), nil
}

// GetIngesterStatus returns status for a specific ingester.
func (s *ConfigServer) GetIngesterStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetIngesterStatusRequest],
) (*connect.Response[apiv1.GetIngesterStatusResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	found := false
	for _, id := range s.orch.ListIngesters() {
		if id == req.Msg.Id {
			found = true
			break
		}
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("ingester not found"))
	}

	return connect.NewResponse(&apiv1.GetIngesterStatusResponse{
		Id:      req.Msg.Id,
		Running: s.orch.IsRunning(),
	}), nil
}

// PutRotationPolicy creates or updates a rotation policy.
func (s *ConfigServer) PutRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRotationPolicyRequest],
) (*connect.Response[apiv1.PutRotationPolicyResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}

	cfg := protoToRotationPolicy(req.Msg.Config)

	// Validate by trying to convert.
	if _, err := cfg.ToRotationPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid rotation policy: %w", err))
	}

	if err := s.cfgStore.PutRotationPolicy(ctx, req.Msg.Id, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutRotationPolicyResponse{}), nil
}

// DeleteRotationPolicy removes a rotation policy.
func (s *ConfigServer) DeleteRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRotationPolicyRequest],
) (*connect.Response[apiv1.DeleteRotationPolicyResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	if err := s.cfgStore.DeleteRotationPolicy(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteRotationPolicyResponse{}), nil
}

// PutStore creates or updates a store.
func (s *ConfigServer) PutStore(
	ctx context.Context,
	req *connect.Request[apiv1.PutStoreRequest],
) (*connect.Response[apiv1.PutStoreResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store id required"))
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store type required"))
	}

	storeCfg := protoToStoreConfig(req.Msg.Config)

	// Persist to config store.
	if err := s.cfgStore.PutStore(ctx, storeCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Apply to runtime: check if store already exists.
	existing := false
	for _, id := range s.orch.ListStores() {
		if id == storeCfg.ID {
			existing = true
			break
		}
	}

	if existing {
		// Update filter on existing store.
		filter := ""
		if storeCfg.Filter != nil {
			filter = *storeCfg.Filter
		}
		if err := s.orch.UpdateStoreFilter(storeCfg.ID, filter); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update store filter: %w", err))
		}
	} else {
		// Add new store to orchestrator.
		if err := s.orch.AddStore(storeCfg, s.factories); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add store: %w", err))
		}
	}

	return connect.NewResponse(&apiv1.PutStoreResponse{}), nil
}

// DeleteStore removes a store (must be empty).
func (s *ConfigServer) DeleteStore(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteStoreRequest],
) (*connect.Response[apiv1.DeleteStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Remove from runtime first (validates emptiness).
	if err := s.orch.RemoveStore(req.Msg.Id); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		if errors.Is(err, orchestrator.ErrStoreNotEmpty) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Remove from config store.
	if err := s.cfgStore.DeleteStore(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteStoreResponse{}), nil
}

// PutIngester creates or updates an ingester.
func (s *ConfigServer) PutIngester(
	ctx context.Context,
	req *connect.Request[apiv1.PutIngesterRequest],
) (*connect.Response[apiv1.PutIngesterResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ingester id required"))
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ingester type required"))
	}

	ingCfg := config.IngesterConfig{
		ID:     req.Msg.Config.Id,
		Type:   req.Msg.Config.Type,
		Params: req.Msg.Config.Params,
	}

	// Check if ingester already exists — if so, remove it first so we can re-create.
	existing := false
	for _, id := range s.orch.ListIngesters() {
		if id == ingCfg.ID {
			existing = true
			break
		}
	}

	if existing {
		if err := s.orch.RemoveIngester(ingCfg.ID); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("remove existing ingester: %w", err))
		}
	}

	// Look up factory and create the ingester.
	factory, ok := s.factories.Ingesters[ingCfg.Type]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown ingester type: %s", ingCfg.Type))
	}

	ingester, err := factory(ingCfg.ID, ingCfg.Params, s.factories.Logger)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("create ingester: %w", err))
	}

	if err := s.orch.AddIngester(ingCfg.ID, ingester); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add ingester: %w", err))
	}

	// Persist to config store.
	if err := s.cfgStore.PutIngester(ctx, ingCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutIngesterResponse{}), nil
}

// DeleteIngester removes an ingester.
func (s *ConfigServer) DeleteIngester(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteIngesterRequest],
) (*connect.Response[apiv1.DeleteIngesterResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Remove from runtime.
	if err := s.orch.RemoveIngester(req.Msg.Id); err != nil {
		if errors.Is(err, orchestrator.ErrIngesterNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Remove from config store.
	if err := s.cfgStore.DeleteIngester(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteIngesterResponse{}), nil
}

// --- Proto <-> Config conversion helpers ---

// protoToRotationPolicy converts a proto RotationPolicyConfig to a config.RotationPolicyConfig.
func protoToRotationPolicy(p *apiv1.RotationPolicyConfig) config.RotationPolicyConfig {
	var cfg config.RotationPolicyConfig

	if p.MaxBytes > 0 {
		s := formatBytes(uint64(p.MaxBytes))
		cfg.MaxBytes = &s
	}
	if p.MaxAgeSeconds > 0 {
		s := (time.Duration(p.MaxAgeSeconds) * time.Second).String()
		cfg.MaxAge = &s
	}
	if p.MaxRecords > 0 {
		cfg.MaxRecords = config.Int64Ptr(p.MaxRecords)
	}

	return cfg
}

// rotationPolicyToProto converts a config.RotationPolicyConfig to a proto RotationPolicyConfig.
func rotationPolicyToProto(cfg config.RotationPolicyConfig) *apiv1.RotationPolicyConfig {
	p := &apiv1.RotationPolicyConfig{}

	if cfg.MaxBytes != nil {
		// Parse the human-readable byte string back to raw bytes.
		if bytes, err := config.ParseBytes(*cfg.MaxBytes); err == nil {
			p.MaxBytes = int64(bytes)
		}
	}
	if cfg.MaxAge != nil {
		if d, err := time.ParseDuration(*cfg.MaxAge); err == nil {
			p.MaxAgeSeconds = int64(d.Seconds())
		}
	}
	if cfg.MaxRecords != nil {
		p.MaxRecords = *cfg.MaxRecords
	}

	return p
}

// protoToStoreConfig converts a proto StoreConfig to a config.StoreConfig.
func protoToStoreConfig(p *apiv1.StoreConfig) config.StoreConfig {
	cfg := config.StoreConfig{
		ID:     p.Id,
		Type:   p.Type,
		Params: p.Params,
	}
	if p.Filter != "" {
		cfg.Filter = config.StringPtr(p.Filter)
	}
	if p.Policy != "" {
		cfg.Policy = config.StringPtr(p.Policy)
	}
	return cfg
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
