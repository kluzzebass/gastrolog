package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/ingester/docker"
	"gastrolog/internal/orchestrator"
)

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
	metaMap := make(map[uuid.UUID]ingMeta)
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
			Id:      id.String(),
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

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	found := slices.Contains(s.orch.ListIngesters(), id)
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
				if ing.ID == id {
					resp.Type = ing.Type
					break
				}
			}
		}
	}

	// Populate stats from orchestrator.
	if stats := s.orch.GetIngesterStats(id); stats != nil {
		resp.MessagesIngested = stats.MessagesIngested.Load()
		resp.Errors = stats.Errors.Load()
		resp.BytesIngested = stats.BytesIngested.Load()
	}

	return connect.NewResponse(resp), nil
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

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	ingCfg := config.IngesterConfig{
		ID:      id,
		Name:    req.Msg.Config.Name,
		Type:    req.Msg.Config.Type,
		Enabled: req.Msg.Config.Enabled,
		Params:  req.Msg.Config.Params,
	}

	// Check if ingester already exists in runtime — if so, remove it first.
	existing := slices.Contains(s.orch.ListIngesters(), ingCfg.ID)

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
		if s.factories.HomeDir != "" {
			params = make(map[string]string, len(ingCfg.Params)+1)
			maps.Copy(params, ingCfg.Params)
			params["_state_dir"] = s.factories.HomeDir
		}

		ingester, err := factory(ingCfg.ID, params, s.factories.Logger)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("create ingester: %w", err))
		}

		if err := s.orch.AddIngester(ingCfg.ID, ingester); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add ingester: %w", err))
		}
	}

	// Persist to config vault (always, even when disabled).
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

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Remove from runtime.
	if err := s.orch.RemoveIngester(id); err != nil {
		if errors.Is(err, orchestrator.ErrIngesterNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Remove from config vault.
	if err := s.cfgStore.DeleteIngester(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteIngesterResponse{}), nil
}

// GetIngesterDefaults returns default parameter values for each ingester type.
func (s *ConfigServer) GetIngesterDefaults(
	ctx context.Context,
	req *connect.Request[apiv1.GetIngesterDefaultsRequest],
) (*connect.Response[apiv1.GetIngesterDefaultsResponse], error) {
	types := make(map[string]*apiv1.IngesterTypeDefaults, len(s.factories.IngesterDefaults))
	for name, fn := range s.factories.IngesterDefaults {
		types[name] = &apiv1.IngesterTypeDefaults{Params: fn()}
	}
	return connect.NewResponse(&apiv1.GetIngesterDefaultsResponse{Types: types}), nil
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
			return connect.NewResponse(&apiv1.TestIngesterResponse{ //nolint:nilerr // test failure is reported in the response body, not as an RPC error
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
