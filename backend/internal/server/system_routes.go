package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"slices"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// PutRoute creates or updates a route.
func (s *SystemServer) PutRoute(
	ctx context.Context,
	req *connect.Request[apiv1.PutRouteRequest],
) (*connect.Response[apiv1.PutRouteResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}

	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	id, connErr := parseProtoID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	if connErr := checkNameConflict(ctx, s.sysStore, "route", id, req.Msg.Config.Name, s.sysStore.ListRoutes,
		func(r system.RouteConfig) (glid.GLID, string) { return r.ID, r.Name }); connErr != nil {
		return nil, connErr
	}

	// No FilterConfig lookup: the gating expression arrives inline on
	// req.Msg.Config.Stages and is validated structurally here. Semantic
	// validation (parse, attribute existence) is the orchestrator's job at
	// reload time.

	// Validate all destination vault IDs reference existing vaults.
	var destinations []glid.GLID
	for _, dest := range req.Msg.Config.Destinations {
		vid, connErr := parseProtoID(dest.VaultId)
		if connErr != nil {
			return nil, connErr
		}
		vc, err := s.sysStore.GetVault(ctx, vid)
		if err != nil {
			return nil, errInternal(err)
		}
		if vc == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("destination vault %q not found", vid))
		}
		destinations = append(destinations, vid)
	}

	distribution, err := routing.ParseDistribution(req.Msg.Config.Distribution)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	cfg := system.RouteConfig{
		ID:           id,
		Name:         req.Msg.Config.Name,
		Priority:     req.Msg.Config.GetPriority(),
		Stages:       convert.RouteStagesFromProto(req.Msg.Config.GetStages()),
		Destinations: destinations,
		Distribution: system.DistributionMode(distribution),
		Enabled:      req.Msg.Config.Enabled,
	}
	if err := s.sysStore.PutRoute(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRoutePut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutRouteResponse{System: fullCfg}), nil
}

// DeleteRoute removes a route.
func (s *SystemServer) DeleteRoute(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRouteRequest],
) (*connect.Response[apiv1.DeleteRouteResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// No referential-integrity check: retention rules carry no route
	// targets — the routing engine owns the WHAT via the route's source
	// predicate — so nothing in config can dangle when a route goes away.

	if err := s.sysStore.DeleteRoute(ctx, id); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRouteDeleted, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteRouteResponse{System: cfg}), nil
}

// routeReferencingVault returns a route that names the vault as a
// destination, or nil when no route does.
func (s *SystemServer) routeReferencingVault(ctx context.Context, vaultID glid.GLID) (*system.RouteConfig, error) {
	routes, err := s.sysStore.ListRoutes(ctx)
	if err != nil {
		return nil, err
	}
	for i := range routes {
		if slices.Contains(routes[i].Destinations, vaultID) {
			return &routes[i], nil
		}
	}
	return nil, nil
}
