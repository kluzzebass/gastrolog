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
		Receivers: make([]*apiv1.ReceiverConfig, 0),
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

	// Get receiver configs
	for _, id := range s.orch.ListReceivers() {
		resp.Receivers = append(resp.Receivers, &apiv1.ReceiverConfig{
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

// ListReceivers returns all registered receivers.
func (s *ConfigServer) ListReceivers(
	ctx context.Context,
	req *connect.Request[apiv1.ListReceiversRequest],
) (*connect.Response[apiv1.ListReceiversResponse], error) {
	ids := s.orch.ListReceivers()

	resp := &apiv1.ListReceiversResponse{
		Receivers: make([]*apiv1.ReceiverInfo, 0, len(ids)),
	}

	for _, id := range ids {
		resp.Receivers = append(resp.Receivers, &apiv1.ReceiverInfo{
			Id:      id,
			Running: s.orch.IsRunning(),
		})
	}

	return connect.NewResponse(resp), nil
}

// GetReceiverStatus returns status for a specific receiver.
func (s *ConfigServer) GetReceiverStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetReceiverStatusRequest],
) (*connect.Response[apiv1.GetReceiverStatusResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	// Check if receiver exists
	found := false
	for _, id := range s.orch.ListReceivers() {
		if id == req.Msg.Id {
			found = true
			break
		}
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("receiver not found"))
	}

	// TODO: track per-receiver metrics
	return connect.NewResponse(&apiv1.GetReceiverStatusResponse{
		Id:      req.Msg.Id,
		Running: s.orch.IsRunning(),
	}), nil
}
