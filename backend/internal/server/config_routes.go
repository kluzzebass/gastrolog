package server

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
)

// PutRoute creates or updates a route.
func (s *ConfigServer) PutRoute(
	ctx context.Context,
	req *connect.Request[apiv1.PutRouteRequest],
) (*connect.Response[apiv1.PutRouteResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}

	id, connErr := parseUUID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Validate filter_id references an existing filter (if non-empty).
	var filterID *uuid.UUID
	if req.Msg.Config.FilterId != "" {
		fid, connErr := parseUUID(req.Msg.Config.FilterId)
		if connErr != nil {
			return nil, connErr
		}
		fc, err := s.cfgStore.GetFilter(ctx, fid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if fc == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("filter %q not found", req.Msg.Config.FilterId))
		}
		filterID = &fid
	}

	// Validate all destination vault IDs reference existing vaults.
	var destinations []uuid.UUID
	for _, dest := range req.Msg.Config.Destinations {
		vid, connErr := parseUUID(dest.VaultId)
		if connErr != nil {
			return nil, connErr
		}
		vc, err := s.cfgStore.GetVault(ctx, vid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if vc == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("destination vault %q not found", dest.VaultId))
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

	cfg := config.RouteConfig{
		ID:           id,
		Name:         req.Msg.Config.Name,
		FilterID:     filterID,
		Destinations: destinations,
		Distribution: distribution,
		Enabled:      req.Msg.Config.Enabled,
	}
	if err := s.cfgStore.PutRoute(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRoutePut, ID: id})

	return connect.NewResponse(&apiv1.PutRouteResponse{}), nil
}

// DeleteRoute removes a route.
func (s *ConfigServer) DeleteRoute(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRouteRequest],
) (*connect.Response[apiv1.DeleteRouteResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	if err := s.cfgStore.DeleteRoute(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRouteDeleted, ID: id})

	return connect.NewResponse(&apiv1.DeleteRouteResponse{}), nil
}

// vaultReferencedByRoute checks if a vault ID is used as a destination in any route.
func (s *ConfigServer) vaultReferencedByRoute(ctx context.Context, vaultID uuid.UUID) (uuid.UUID, bool, error) {
	routes, err := s.cfgStore.ListRoutes(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	for _, rt := range routes {
		if slices.Contains(rt.Destinations, vaultID) {
			return rt.ID, true, nil
		}
	}
	return uuid.Nil, false, nil
}
