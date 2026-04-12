package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/orchestrator"
)

// PutFilter creates or updates a filter.
func (s *ConfigServer) PutFilter(
	ctx context.Context,
	req *connect.Request[apiv1.PutFilterRequest],
) (*connect.Response[apiv1.PutFilterResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	// Validate expression by trying to compile it.
	if _, err := orchestrator.CompileFilter(uuid.Nil, req.Msg.Config.Expression); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid filter expression: %w", err))
	}

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	filters, err := s.cfgStore.ListFilters(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("filter", id, req.Msg.Config.Name, filters, func(f config.FilterConfig) (uuid.UUID, string) { return f.ID, f.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := config.FilterConfig{ID: id, Name: req.Msg.Config.Name, Expression: req.Msg.Config.Expression}
	if err := s.cfgStore.PutFilter(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyFilterPut, ID: id})

	fullCfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutFilterResponse{Config: fullCfg}), nil
}

// DeleteFilter removes a filter.
func (s *ConfigServer) DeleteFilter(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteFilterRequest],
) (*connect.Response[apiv1.DeleteFilterResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Check referential integrity: reject if any route references this filter.
	routes, err := s.cfgStore.ListRoutes(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, rt := range routes {
		if rt.FilterID != nil && *rt.FilterID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("filter %q is referenced by route %q", req.Msg.Id, rt.ID))
		}
	}

	if err := s.cfgStore.DeleteFilter(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullConfig(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteFilterResponse{Config: cfg}), nil
}
