package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"
)

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
	if _, err := orchestrator.CompileFilter(uuid.Nil, req.Msg.Config.Expression); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid filter expression: %w", err))
	}

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}
	cfg := config.FilterConfig{ID: id, Name: req.Msg.Config.Name, Expression: req.Msg.Config.Expression}
	if err := s.cfgStore.PutFilter(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Reload filters in orchestrator.
	if err := s.orch.ReloadFilters(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload filters: %w", err))
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

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Check referential integrity: reject if any vault references this filter.
	stores, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		if st.Filter != nil && *st.Filter == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("filter %q is referenced by vault %q", req.Msg.Id, st.ID))
		}
	}

	if err := s.cfgStore.DeleteFilter(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteFilterResponse{}), nil
}
