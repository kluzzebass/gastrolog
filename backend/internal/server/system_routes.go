package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
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
	routes, err := s.sysStore.ListRoutes(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("route", id, req.Msg.Config.Name, routes, func(r system.RouteConfig) (glid.GLID, string) { return r.ID, r.Name }); connErr != nil {
		return nil, connErr
	}

	// gastrolog-4kkoo (Phase 5): no FilterConfig lookup; the gating expression
	// arrives inline on req.Msg.Config.Stages and is validated structurally
	// here. Semantic validation (parse, attribute existence) is the
	// orchestrator's job at reload time.

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

	// Default distribution to "fanout".
	distribution := req.Msg.Config.Distribution
	if distribution == "" {
		distribution = "fanout"
	}
	if distribution != "fanout" && distribution != "round-robin" && distribution != "failover" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("distribution must be \"fanout\", \"round-robin\", or \"failover\", got %q", distribution))
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

	// Phase 4 (gastrolog-42f9z): retention rules no longer carry route
	// targets — the routing engine owns the WHAT via the route's source
	// predicate. The eject-route referential-integrity check is gone.
	// When Phase 5 adds richer routing, route deletion may need new
	// integrity guards (e.g. block deletion if any retention-trigger
	// route is the sole drain for a vault), but those don't exist yet.

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

// vaultReferencedByRoute checks if a vault ID is used as a destination in any route.
func (s *SystemServer) vaultReferencedByRoute(ctx context.Context, vaultID glid.GLID) (glid.GLID, bool, error) {
	routes, err := s.sysStore.ListRoutes(ctx)
	if err != nil {
		return glid.Nil, false, err
	}
	for _, rt := range routes {
		if slices.Contains(rt.Destinations, vaultID) {
			return rt.ID, true, nil
		}
	}
	return glid.Nil, false, nil
}
