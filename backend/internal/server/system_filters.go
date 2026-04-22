package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// PutFilter creates or updates a filter.
func (s *SystemServer) PutFilter(
	ctx context.Context,
	req *connect.Request[apiv1.PutFilterRequest],
) (*connect.Response[apiv1.PutFilterResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	// Validate expression by trying to compile it.
	if _, err := orchestrator.CompileFilter(glid.Nil, req.Msg.Config.Expression); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid filter expression: %w", err))
	}

	id, connErr := parseProtoID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	filters, err := s.sysStore.ListFilters(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("filter", id, req.Msg.Config.Name, filters, func(f system.FilterConfig) (glid.GLID, string) { return f.ID, f.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := system.FilterConfig{ID: id, Name: req.Msg.Config.Name, Expression: req.Msg.Config.Expression}
	if err := s.sysStore.PutFilter(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyFilterPut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutFilterResponse{System: fullCfg}), nil
}

// DeleteFilter removes a filter.
func (s *SystemServer) DeleteFilter(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteFilterRequest],
) (*connect.Response[apiv1.DeleteFilterResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Check referential integrity: reject if any route references this filter.
	routes, err := s.sysStore.ListRoutes(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, rt := range routes {
		if rt.FilterID != nil && *rt.FilterID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("filter %q is referenced by route %q", req.Msg.Id, rt.ID))
		}
	}

	if err := s.sysStore.DeleteFilter(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteFilterResponse{System: cfg}), nil
}
