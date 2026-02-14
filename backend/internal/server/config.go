package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/config"
	"gastrolog/internal/ingester/docker"
	"gastrolog/internal/orchestrator"
)

// ConfigServer implements the ConfigService.
type ConfigServer struct {
	orch             *orchestrator.Orchestrator
	cfgStore            config.Store
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
					Id:      storeCfg.ID,
					Name:    storeCfg.Name,
					Type:    storeCfg.Type,
					Params:  storeCfg.Params,
					Enabled: storeCfg.Enabled,
				}
				if storeCfg.Filter != nil {
					sc.Filter = *storeCfg.Filter
				}
				if storeCfg.Policy != nil {
					sc.Policy = *storeCfg.Policy
				}
				if storeCfg.Retention != nil {
					sc.Retention = *storeCfg.Retention
				}
				resp.Stores = append(resp.Stores, sc)
			}
		}

		// Get ingester configs from config store.
		ingesters, err := s.cfgStore.ListIngesters(ctx)
		if err == nil {
			for _, ing := range ingesters {
				resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
					Id:      ing.ID,
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
					Id:         fc.ID,
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
				p.Id = pol.ID
				p.Name = pol.Name
				resp.RotationPolicies = append(resp.RotationPolicies, p)
			}
		}

		// Get retention policies from config store.
		retPolicies, err := s.cfgStore.ListRetentionPolicies(ctx)
		if err == nil {
			for _, pol := range retPolicies {
				p := retentionPolicyToProto(pol)
				p.Id = pol.ID
				p.Name = pol.Name
				resp.RetentionPolicies = append(resp.RetentionPolicies, p)
			}
		}
	}

	return connect.NewResponse(resp), nil
}

// PutFilter creates or updates a filter.
func (s *ConfigServer) PutFilter(
	ctx context.Context,
	req *connect.Request[apiv1.PutFilterRequest],
) (*connect.Response[apiv1.PutFilterResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}

	// Validate expression by trying to compile it.
	if _, err := orchestrator.CompileFilter("_validate", req.Msg.Config.Expression); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid filter expression: %w", err))
	}

	cfg := config.FilterConfig{ID: req.Msg.Config.Id, Name: req.Msg.Config.Name, Expression: req.Msg.Config.Expression}
	if err := s.cfgStore.PutFilter(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Reload filters in orchestrator.
	fullCfg, err := s.cfgStore.Load(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload config: %w", err))
	}
	if err := s.orch.UpdateFilters(fullCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update filters: %w", err))
	}

	return connect.NewResponse(&apiv1.PutFilterResponse{}), nil
}

// DeleteFilter removes a filter.
func (s *ConfigServer) DeleteFilter(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteFilterRequest],
) (*connect.Response[apiv1.DeleteFilterResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Check referential integrity: reject if any store references this filter.
	stores, err := s.cfgStore.ListStores(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		if st.Filter != nil && *st.Filter == req.Msg.Id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("filter %q is referenced by store %q", req.Msg.Id, st.ID))
		}
	}

	if err := s.cfgStore.DeleteFilter(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteFilterResponse{}), nil
}

// ListIngesters returns all registered ingesters.
func (s *ConfigServer) ListIngesters(
	ctx context.Context,
	req *connect.Request[apiv1.ListIngestersRequest],
) (*connect.Response[apiv1.ListIngestersResponse], error) {
	ids := s.orch.ListIngesters()

	// Build type and name lookup from config.
	type ingMeta struct {
		typ  string
		name string
	}
	metaMap := make(map[string]ingMeta)
	if s.cfgStore != nil {
		ingesters, err := s.cfgStore.ListIngesters(ctx)
		if err == nil {
			for _, ing := range ingesters {
				metaMap[ing.ID] = ingMeta{typ: ing.Type, name: ing.Name}
			}
		}
	}

	resp := &apiv1.ListIngestersResponse{
		Ingesters: make([]*apiv1.IngesterInfo, 0, len(ids)),
	}

	for _, id := range ids {
		m := metaMap[id]
		resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterInfo{
			Id:      id,
			Name:    m.name,
			Type:    m.typ,
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

	resp := &apiv1.GetIngesterStatusResponse{
		Id:      req.Msg.Id,
		Running: s.orch.IsRunning(),
	}

	// Look up ingester type from config.
	if s.cfgStore != nil {
		ingesters, err := s.cfgStore.ListIngesters(ctx)
		if err == nil {
			for _, ing := range ingesters {
				if ing.ID == req.Msg.Id {
					resp.Type = ing.Type
					break
				}
			}
		}
	}

	// Populate stats from orchestrator.
	if stats := s.orch.GetIngesterStats(req.Msg.Id); stats != nil {
		resp.MessagesIngested = stats.MessagesIngested.Load()
		resp.Errors = stats.Errors.Load()
		resp.BytesIngested = stats.BytesIngested.Load()
	}

	return connect.NewResponse(resp), nil
}

// PutRotationPolicy creates or updates a rotation policy.
func (s *ConfigServer) PutRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRotationPolicyRequest],
) (*connect.Response[apiv1.PutRotationPolicyResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}

	cfg := protoToRotationPolicy(req.Msg.Config)

	// Validate by trying to convert.
	if _, err := cfg.ToRotationPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid rotation policy: %w", err))
	}
	if err := cfg.ValidateCron(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := s.cfgStore.PutRotationPolicy(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Hot-reload rotation policies for running stores.
	fullCfg, err := s.cfgStore.Load(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload config: %w", err))
	}
	if err := s.orch.UpdateRotationPolicies(fullCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update rotation policies: %w", err))
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

	// Clear policy reference on any stores that use it.
	stores, err := s.cfgStore.ListStores(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		if st.Policy != nil && *st.Policy == req.Msg.Id {
			st.Policy = nil
			if err := s.cfgStore.PutStore(ctx, st); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}

	if err := s.cfgStore.DeleteRotationPolicy(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteRotationPolicyResponse{}), nil
}

// PutRetentionPolicy creates or updates a retention policy.
func (s *ConfigServer) PutRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRetentionPolicyRequest],
) (*connect.Response[apiv1.PutRetentionPolicyResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}

	cfg := protoToRetentionPolicy(req.Msg.Config)

	// Validate by trying to convert.
	if _, err := cfg.ToRetentionPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid retention policy: %w", err))
	}

	if err := s.cfgStore.PutRetentionPolicy(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Hot-reload retention policies for running stores.
	fullCfg, err := s.cfgStore.Load(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload config: %w", err))
	}
	if err := s.orch.UpdateRetentionPolicies(fullCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update retention policies: %w", err))
	}

	return connect.NewResponse(&apiv1.PutRetentionPolicyResponse{}), nil
}

// DeleteRetentionPolicy removes a retention policy.
func (s *ConfigServer) DeleteRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRetentionPolicyRequest],
) (*connect.Response[apiv1.DeleteRetentionPolicyResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Clear retention reference on any stores that use it.
	stores, err := s.cfgStore.ListStores(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		if st.Retention != nil && *st.Retention == req.Msg.Id {
			st.Retention = nil
			if err := s.cfgStore.PutStore(ctx, st); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}

	if err := s.cfgStore.DeleteRetentionPolicy(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteRetentionPolicyResponse{}), nil
}

// validateStoreDir checks that a file store's directory does not overlap (nest
// inside or contain) any other file store's directory. Returns an error
// describing the conflict, or nil if the directory is safe.
func (s *ConfigServer) validateStoreDir(ctx context.Context, storeID string, dir string) error {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	// Normalize: ensure trailing separator for prefix comparison.
	normDir := filepath.Clean(absDir) + string(filepath.Separator)

	existing, err := s.cfgStore.ListStores(ctx)
	if err != nil {
		return fmt.Errorf("list stores: %w", err)
	}

	for _, st := range existing {
		if st.ID == storeID {
			continue // Updating self is OK.
		}
		if st.Type != "file" {
			continue // Only check file stores.
		}
		otherDir := st.Params["dir"]
		if otherDir == "" {
			continue
		}
		absOther, err := filepath.Abs(otherDir)
		if err != nil {
			continue // Can't resolve — skip.
		}
		normOther := filepath.Clean(absOther) + string(filepath.Separator)

		// Check for exact match or nesting in either direction.
		if normDir == normOther {
			return fmt.Errorf("directory %q is already used by store %q", dir, st.ID)
		}
		if strings.HasPrefix(normDir, normOther) {
			return fmt.Errorf("directory %q is nested inside store %q directory %q", dir, st.ID, otherDir)
		}
		if strings.HasPrefix(normOther, normDir) {
			return fmt.Errorf("directory %q contains store %q directory %q", dir, st.ID, otherDir)
		}
	}

	return nil
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
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store type required"))
	}

	storeCfg := protoToStoreConfig(req.Msg.Config)

	// Validate file store directory against nesting.
	if storeCfg.Type == "file" {
		if dir := storeCfg.Params["dir"]; dir != "" {
			if err := s.validateStoreDir(ctx, storeCfg.ID, dir); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		}
	}

	// Persist to config store.
	if err := s.cfgStore.PutStore(ctx, storeCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Reload full config for filter resolution.
	fullCfg, err := s.cfgStore.Load(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload config: %w", err))
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
		// Reload filters, rotation policies, and retention policies (references may have changed).
		if err := s.orch.UpdateFilters(fullCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update filters: %w", err))
		}
		if err := s.orch.UpdateRotationPolicies(fullCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update rotation policies: %w", err))
		}
		if err := s.orch.UpdateRetentionPolicies(fullCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update retention policies: %w", err))
		}
		// Apply enabled state.
		if !storeCfg.Enabled {
			_ = s.orch.DisableStore(storeCfg.ID)
		} else {
			_ = s.orch.EnableStore(storeCfg.ID)
		}
	} else {
		// Add new store to orchestrator.
		if err := s.orch.AddStore(storeCfg, fullCfg, s.factories); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add store: %w", err))
		}
	}

	return connect.NewResponse(&apiv1.PutStoreResponse{}), nil
}

// DeleteStore removes a store. If force is false, the store must be empty.
// If force is true, the store is removed regardless of content: active chunks are sealed,
// all indexes and chunks are deleted, and for file stores the data directory is removed.
func (s *ConfigServer) DeleteStore(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteStoreRequest],
) (*connect.Response[apiv1.DeleteStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Read store config before removing from runtime (we need it for directory cleanup).
	var storeCfg *config.StoreConfig
	if req.Msg.Force {
		cfg, err := s.cfgStore.GetStore(ctx, req.Msg.Id)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read store config: %w", err))
		}
		storeCfg = cfg
	}

	// Remove from runtime.
	if req.Msg.Force {
		if err := s.orch.ForceRemoveStore(req.Msg.Id); err != nil {
			// If the store doesn't exist in the orchestrator, that's fine for force-delete —
			// we still clean up config and disk.
			if !errors.Is(err, orchestrator.ErrStoreNotFound) {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}

		// For file stores, remove the data directory.
		if storeCfg != nil && storeCfg.Type == "file" {
			if dir := storeCfg.Params["dir"]; dir != "" {
				if err := os.RemoveAll(dir); err != nil {
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("remove store directory: %w", err))
				}
			}
		}
	} else {
		if err := s.orch.RemoveStore(req.Msg.Id); err != nil {
			if errors.Is(err, orchestrator.ErrStoreNotFound) {
				return nil, connect.NewError(connect.CodeNotFound, err)
			}
			if errors.Is(err, orchestrator.ErrStoreNotEmpty) {
				return nil, connect.NewError(connect.CodeFailedPrecondition, err)
			}
			return nil, connect.NewError(connect.CodeInternal, err)
		}
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
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ingester type required"))
	}

	ingCfg := config.IngesterConfig{
		ID:      req.Msg.Config.Id,
		Name:    req.Msg.Config.Name,
		Type:    req.Msg.Config.Type,
		Enabled: req.Msg.Config.Enabled,
		Params:  req.Msg.Config.Params,
	}

	// Check if ingester already exists in runtime — if so, remove it first.
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

	// Only create and register the ingester if enabled.
	if ingCfg.Enabled {
		// Look up factory and create the ingester.
		factory, ok := s.factories.Ingesters[ingCfg.Type]
		if !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown ingester type: %s", ingCfg.Type))
		}

		// Inject _state_dir so ingesters can persist state.
		params := ingCfg.Params
		if s.factories.DataDir != "" {
			params = make(map[string]string, len(ingCfg.Params)+1)
			for k, v := range ingCfg.Params {
				params[k] = v
			}
			params["_state_dir"] = s.factories.DataDir
		}

		ingester, err := factory(ingCfg.ID, params, s.factories.Logger)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("create ingester: %w", err))
		}

		if err := s.orch.AddIngester(ingCfg.ID, ingester); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add ingester: %w", err))
		}
	}

	// Persist to config store (always, even when disabled).
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

// PauseStore disables ingestion for a store.
func (s *ConfigServer) PauseStore(
	ctx context.Context,
	req *connect.Request[apiv1.PauseStoreRequest],
) (*connect.Response[apiv1.PauseStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Update runtime state.
	if err := s.orch.DisableStore(req.Msg.Id); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Persist to config.
	storeCfg, err := s.cfgStore.GetStore(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if storeCfg != nil {
		storeCfg.Enabled = false
		if err := s.cfgStore.PutStore(ctx, *storeCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.PauseStoreResponse{}), nil
}

// ResumeStore enables ingestion for a store.
func (s *ConfigServer) ResumeStore(
	ctx context.Context,
	req *connect.Request[apiv1.ResumeStoreRequest],
) (*connect.Response[apiv1.ResumeStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Update runtime state.
	if err := s.orch.EnableStore(req.Msg.Id); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Persist to config.
	storeCfg, err := s.cfgStore.GetStore(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if storeCfg != nil {
		storeCfg.Enabled = true
		if err := s.cfgStore.PutStore(ctx, *storeCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.ResumeStoreResponse{}), nil
}

// DecommissionStore disables a store and force-deletes it.
func (s *ConfigServer) DecommissionStore(
	ctx context.Context,
	req *connect.Request[apiv1.DecommissionStoreRequest],
) (*connect.Response[apiv1.DecommissionStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Count chunks before removal for the response.
	var chunksRemoved int64
	if cm := s.orch.ChunkManager(req.Msg.Id); cm != nil {
		if metas, err := cm.List(); err == nil {
			chunksRemoved = int64(len(metas))
		}
	}

	// Disable ingestion first.
	_ = s.orch.DisableStore(req.Msg.Id)

	// Read store config for directory cleanup.
	var storeCfg *config.StoreConfig
	if cfg, err := s.cfgStore.GetStore(ctx, req.Msg.Id); err == nil {
		storeCfg = cfg
	}

	// Force-remove from orchestrator.
	if err := s.orch.ForceRemoveStore(req.Msg.Id); err != nil {
		if !errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	// Clean up data directory for file stores.
	if storeCfg != nil && storeCfg.Type == "file" {
		if dir := storeCfg.Params["dir"]; dir != "" {
			if err := os.RemoveAll(dir); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("remove store directory: %w", err))
			}
		}
	}

	// Remove from config store.
	if err := s.cfgStore.DeleteStore(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DecommissionStoreResponse{
		ChunksRemoved: chunksRemoved,
	}), nil
}

// TestIngester tests connectivity for an ingester configuration without saving it.
func (s *ConfigServer) TestIngester(
	ctx context.Context,
	req *connect.Request[apiv1.TestIngesterRequest],
) (*connect.Response[apiv1.TestIngesterResponse], error) {
	switch req.Msg.Type {
	case "docker":
		msg, err := docker.TestConnection(ctx, req.Msg.Params, s.cfgStore)
		if err != nil {
			return connect.NewResponse(&apiv1.TestIngesterResponse{
				Success: false,
				Message: err.Error(),
			}), nil
		}
		return connect.NewResponse(&apiv1.TestIngesterResponse{
			Success: true,
			Message: msg,
		}), nil
	default:
		return connect.NewResponse(&apiv1.TestIngesterResponse{
			Success: false,
			Message: fmt.Sprintf("connection test not supported for ingester type %q", req.Msg.Type),
		}), nil
	}
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
	if p.Cron != "" {
		cfg.Cron = config.StringPtr(p.Cron)
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
	if cfg.Cron != nil {
		p.Cron = *cfg.Cron
	}

	return p
}

// protoToRetentionPolicy converts a proto RetentionPolicyConfig to a config.RetentionPolicyConfig.
func protoToRetentionPolicy(p *apiv1.RetentionPolicyConfig) config.RetentionPolicyConfig {
	var cfg config.RetentionPolicyConfig

	if p.MaxAgeSeconds > 0 {
		s := (time.Duration(p.MaxAgeSeconds) * time.Second).String()
		cfg.MaxAge = &s
	}
	if p.MaxBytes > 0 {
		s := formatBytes(uint64(p.MaxBytes))
		cfg.MaxBytes = &s
	}
	if p.MaxChunks > 0 {
		cfg.MaxChunks = config.Int64Ptr(p.MaxChunks)
	}

	return cfg
}

// retentionPolicyToProto converts a config.RetentionPolicyConfig to a proto RetentionPolicyConfig.
func retentionPolicyToProto(cfg config.RetentionPolicyConfig) *apiv1.RetentionPolicyConfig {
	p := &apiv1.RetentionPolicyConfig{}

	if cfg.MaxAge != nil {
		if d, err := time.ParseDuration(*cfg.MaxAge); err == nil {
			p.MaxAgeSeconds = int64(d.Seconds())
		}
	}
	if cfg.MaxBytes != nil {
		if bytes, err := config.ParseBytes(*cfg.MaxBytes); err == nil {
			p.MaxBytes = int64(bytes)
		}
	}
	if cfg.MaxChunks != nil {
		p.MaxChunks = *cfg.MaxChunks
	}

	return p
}

// protoToStoreConfig converts a proto StoreConfig to a config.StoreConfig.
func protoToStoreConfig(p *apiv1.StoreConfig) config.StoreConfig {
	cfg := config.StoreConfig{
		ID:      p.Id,
		Name:    p.Name,
		Type:    p.Type,
		Params:  p.Params,
		Enabled: p.Enabled,
	}
	if p.Filter != "" {
		cfg.Filter = config.StringPtr(p.Filter)
	}
	if p.Policy != "" {
		cfg.Policy = config.StringPtr(p.Policy)
	}
	if p.Retention != "" {
		cfg.Retention = config.StringPtr(p.Retention)
	}
	return cfg
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

// userPreferences is the JSON structure stored per user.
type userPreferences struct {
	Theme string `json:"theme,omitempty"`
}

// GetPreferences returns the current user's preferences.
func (s *ConfigServer) GetPreferences(
	ctx context.Context,
	req *connect.Request[apiv1.GetPreferencesRequest],
) (*connect.Response[apiv1.GetPreferencesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	key := "user:" + claims.UserID + ":prefs"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetPreferencesResponse{}
	if raw != nil {
		var prefs userPreferences
		if err := json.Unmarshal([]byte(*raw), &prefs); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse preferences: %w", err))
		}
		resp.Theme = prefs.Theme
	}

	return connect.NewResponse(resp), nil
}

// PutPreferences updates the current user's preferences.
func (s *ConfigServer) PutPreferences(
	ctx context.Context,
	req *connect.Request[apiv1.PutPreferencesRequest],
) (*connect.Response[apiv1.PutPreferencesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	prefs := userPreferences{
		Theme: req.Msg.Theme,
	}
	data, err := json.Marshal(prefs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	key := "user:" + claims.UserID + ":prefs"
	if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutPreferencesResponse{}), nil
}

// savedQuery is the JSON structure for a single saved query.
type savedQuery struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// GetSavedQueries returns the current user's saved queries.
func (s *ConfigServer) GetSavedQueries(
	ctx context.Context,
	req *connect.Request[apiv1.GetSavedQueriesRequest],
) (*connect.Response[apiv1.GetSavedQueriesResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetSavedQueriesResponse{}
	if raw != nil {
		var queries []savedQuery
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
		for _, q := range queries {
			resp.Queries = append(resp.Queries, &apiv1.SavedQuery{
				Name:  q.Name,
				Query: q.Query,
			})
		}
	}

	return connect.NewResponse(resp), nil
}

// PutSavedQuery creates or updates a saved query by name.
func (s *ConfigServer) PutSavedQuery(
	ctx context.Context,
	req *connect.Request[apiv1.PutSavedQueryRequest],
) (*connect.Response[apiv1.PutSavedQueryResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	if req.Msg.Query == nil || req.Msg.Query.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query name required"))
	}

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var queries []savedQuery
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
	}

	// Upsert: replace if name exists, append otherwise.
	found := false
	for i, q := range queries {
		if q.Name == req.Msg.Query.Name {
			queries[i].Query = req.Msg.Query.Query
			found = true
			break
		}
	}
	if !found {
		queries = append(queries, savedQuery{
			Name:  req.Msg.Query.Name,
			Query: req.Msg.Query.Query,
		})
	}

	data, err := json.Marshal(queries)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.PutSavedQueryResponse{}), nil
}

// DeleteSavedQuery removes a saved query by name.
func (s *ConfigServer) DeleteSavedQuery(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteSavedQueryRequest],
) (*connect.Response[apiv1.DeleteSavedQueryResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query name required"))
	}

	key := "user:" + claims.UserID + ":saved_queries"
	raw, err := s.cfgStore.GetSetting(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var queries []savedQuery
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &queries); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse saved queries: %w", err))
		}
	}

	filtered := queries[:0]
	for _, q := range queries {
		if q.Name != req.Msg.Name {
			filtered = append(filtered, q)
		}
	}

	if len(filtered) == 0 {
		if err := s.cfgStore.DeleteSetting(ctx, key); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	} else {
		data, err := json.Marshal(filtered)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if err := s.cfgStore.PutSetting(ctx, key, string(data)); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.DeleteSavedQueryResponse{}), nil
}

// reloadCertManager lists all certs from the store and loads them into the cert manager.
func (s *ConfigServer) reloadCertManager(ctx context.Context) error {
	if s.certManager == nil {
		return nil
	}
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		return fmt.Errorf("load server config: %w", err)
	}
	certList, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]cert.CertSource, len(certList))
	for _, c := range certList {
		certs[c.Name] = cert.CertSource{CertPEM: c.CertPEM, KeyPEM: c.KeyPEM, CertFile: c.CertFile, KeyFile: c.KeyFile}
	}
	return s.certManager.LoadFromConfig(sc.TLS.DefaultCert, certs)
}

// ListCertificates returns all certificate names.
func (s *ConfigServer) ListCertificates(
	ctx context.Context,
	req *connect.Request[apiv1.ListCertificatesRequest],
) (*connect.Response[apiv1.ListCertificatesResponse], error) {
	certs, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	infos := make([]*apiv1.CertificateInfo, len(certs))
	for i, c := range certs {
		infos[i] = &apiv1.CertificateInfo{Id: c.ID, Name: c.Name}
	}
	return connect.NewResponse(&apiv1.ListCertificatesResponse{Certificates: infos}), nil
}

// findCertByName returns the certificate with the given name, or nil if not found.
func (s *ConfigServer) findCertByName(ctx context.Context, name string) (*config.CertPEM, error) {
	certs, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range certs {
		if c.Name == name {
			return &c, nil
		}
	}
	return nil, nil
}

// GetCertificate returns a certificate by ID.
func (s *ConfigServer) GetCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.GetCertificateRequest],
) (*connect.Response[apiv1.GetCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}
	pem, err := s.cfgStore.GetCertificate(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if pem == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("certificate not found"))
	}
	return connect.NewResponse(&apiv1.GetCertificateResponse{
		Id:       pem.ID,
		Name:     pem.Name,
		CertPem:  pem.CertPEM,
		KeyPem:   "", // Never expose private keys via API
		CertFile: pem.CertFile,
		KeyFile:  pem.KeyFile,
	}), nil
}

// PutCertificate creates or updates a certificate.
func (s *ConfigServer) PutCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.PutCertificateRequest],
) (*connect.Response[apiv1.PutCertificateResponse], error) {
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name required"))
	}

	// Load existing cert by ID (if given) or by name for key-reuse logic.
	var existing *config.CertPEM
	var err error
	if req.Msg.Id != "" {
		existing, err = s.cfgStore.GetCertificate(ctx, req.Msg.Id)
	} else {
		existing, err = s.findCertByName(ctx, req.Msg.Name)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		existing = &config.CertPEM{}
	}

	hasPEM := req.Msg.CertPem != "" && req.Msg.KeyPem != ""
	hasFiles := req.Msg.CertFile != "" && req.Msg.KeyFile != ""
	// Update PEM cert: certPem + empty keyPem means keep existing key
	hasPEMUpdate := req.Msg.CertPem != "" && (req.Msg.KeyPem != "" || (existing.KeyPEM != "" && existing.CertFile == ""))
	if !hasPEM && !hasFiles && !hasPEMUpdate {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("provide either cert_pem+key_pem or cert_file+key_file"))
	}

	keyPEM := req.Msg.KeyPem
	if req.Msg.CertPem != "" && keyPEM == "" && existing.KeyPEM != "" {
		keyPEM = existing.KeyPEM
	}

	// Validate PEM before storing to avoid enabling HTTPS with invalid certs
	if req.Msg.CertPem != "" && keyPEM != "" {
		if _, err := tls.X509KeyPair([]byte(req.Msg.CertPem), []byte(keyPEM)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key PEM: %w", err))
		}
	}
	if hasFiles {
		keyPath := req.Msg.KeyFile
		if keyPath == "" {
			keyPath = existing.KeyFile
		}
		if keyPath != "" {
			if _, err := tls.LoadX509KeyPair(req.Msg.CertFile, keyPath); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key file: %w", err))
			}
		}
	}

	// Reuse request ID, fall back to existing ID, then generate new UUID.
	certID := req.Msg.Id
	if certID == "" {
		certID = existing.ID
	}
	if certID == "" {
		certID = uuid.Must(uuid.NewV7()).String()
	}

	newCert := config.CertPEM{
		ID:       certID,
		Name:     req.Msg.Name,
		CertPEM:  req.Msg.CertPem,
		KeyPEM:   keyPEM,
		CertFile: req.Msg.CertFile,
		KeyFile:  req.Msg.KeyFile,
	}
	if err := s.cfgStore.PutCertificate(ctx, newCert); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Update default cert in server config if requested.
	if req.Msg.SetAsDefault {
		sc, err := config.LoadServerConfig(ctx, s.cfgStore)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		sc.TLS.DefaultCert = req.Msg.Name
		if err := config.SaveServerConfig(ctx, s.cfgStore, sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := s.reloadCertManager(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload certs: %w", err))
	}
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}
	return connect.NewResponse(&apiv1.PutCertificateResponse{}), nil
}

// DeleteCertificate removes a certificate.
func (s *ConfigServer) DeleteCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCertificateRequest],
) (*connect.Response[apiv1.DeleteCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}
	pem, err := s.cfgStore.GetCertificate(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if pem == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("certificate not found"))
	}
	if err := s.cfgStore.DeleteCertificate(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Clear default and disable TLS if the deleted cert was the default.
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if sc.TLS.DefaultCert == pem.Name {
		sc.TLS.DefaultCert = ""
		sc.TLS.TLSEnabled = false
		sc.TLS.HTTPToHTTPSRedirect = false
		if err := config.SaveServerConfig(ctx, s.cfgStore, sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := s.reloadCertManager(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload certs: %w", err))
	}
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}
	return connect.NewResponse(&apiv1.DeleteCertificateResponse{}), nil
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
