package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
)

// ConfigServer implements the ConfigService.
type ConfigServer struct {
	orch *orchestrator.Orchestrator
}

var _ gastrologv1connect.ConfigServiceHandler = (*ConfigServer)(nil)

// NewConfigServer creates a new ConfigServer.
func NewConfigServer(orch *orchestrator.Orchestrator) *ConfigServer {
	return &ConfigServer{orch: orch}
}

// GetConfig returns the current configuration.
func (s *ConfigServer) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	resp := &apiv1.GetConfigResponse{
		Stores:    make([]*apiv1.StoreConfig, 0),
		Ingesters: make([]*apiv1.IngesterConfig, 0),
	}

	// Get store configs
	for _, id := range s.orch.ListStores() {
		cfg, err := s.orch.StoreConfig(id)
		if err != nil {
			continue
		}
		resp.Stores = append(resp.Stores, &apiv1.StoreConfig{
			Id:    cfg.ID,
			Type:  cfg.Type,
			Route: cfg.Route,
		})
	}

	// Get ingester configs
	for _, id := range s.orch.ListIngesters() {
		resp.Ingesters = append(resp.Ingesters, &apiv1.IngesterConfig{
			Id: id,
			// Type not tracked after creation
		})
	}

	return connect.NewResponse(resp), nil
}

// UpdateStoreRoute updates a store's routing expression.
func (s *ConfigServer) UpdateStoreRoute(
	ctx context.Context,
	req *connect.Request[apiv1.UpdateStoreRouteRequest],
) (*connect.Response[apiv1.UpdateStoreRouteResponse], error) {
	if req.Msg.StoreId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store_id required"))
	}

	if err := s.orch.UpdateStoreRoute(req.Msg.StoreId, req.Msg.Route); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	return connect.NewResponse(&apiv1.UpdateStoreRouteResponse{}), nil
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

	// Check if ingester exists
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

	// TODO: track per-ingester metrics
	return connect.NewResponse(&apiv1.GetIngesterStatusResponse{
		Id:      req.Msg.Id,
		Running: s.orch.IsRunning(),
	}), nil
}
