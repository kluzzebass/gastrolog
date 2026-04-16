package server

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"strings"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/orchestrator"
)

// ListIngesters returns all configured ingesters across the cluster.
// Running status is only known for ingesters on the local node.
func (s *SystemServer) ListIngesters(
	ctx context.Context,
	req *connect.Request[apiv1.ListIngestersRequest],
) (*connect.Response[apiv1.ListIngestersResponse], error) {
	// Config store is the source of truth for all ingesters (cluster-wide).
	var allIngesters []system.IngesterConfig
	if s.sysStore != nil {
		var err error
		allIngesters, err = s.sysStore.ListIngesters(ctx)
		if err != nil {
			return nil, errInternal(err)
		}
	}

	// Local orchestrator knows which ingesters are running on this node.
	localIDs := make(map[glid.GLID]struct{})
	for _, id := range s.orch.ListIngesters() {
		localIDs[id] = struct{}{}
	}

	resp := &apiv1.ListIngestersResponse{
		Ingesters: make([]*apiv1.IngesterInfo, 0, len(allIngesters)),
	}

	for _, ing := range allIngesters {
		nodeID := ing.NodeID
		if nodeID == "" {
			nodeID = s.localNodeID
		}
		_, isLocal := localIDs[ing.ID]
		info := &apiv1.IngesterInfo{
			Id:     ing.ID.ToProto(),
			Name:   ing.Name,
			Type:   ing.Type,
			NodeId: []byte(nodeID),
		}
		if isLocal {
			info.Running = s.orch.IsRunning()
		} else if ps := s.findPeerIngesterStats(ing.ID); ps != nil {
			info.Running = ps.Running
		}
		resp.Ingesters = append(resp.Ingesters, info)
	}

	return connect.NewResponse(resp), nil
}

// GetIngesterStatus returns status for a specific ingester.
// Works for any configured ingester, not just locally running ones.
func (s *SystemServer) GetIngesterStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetIngesterStatusRequest],
) (*connect.Response[apiv1.GetIngesterStatusResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Check config store for existence (cluster-wide).
	var ingCfg *system.IngesterConfig
	if s.sysStore != nil {
		var ingErr error
		ingCfg, ingErr = s.sysStore.GetIngester(ctx, id)
		if ingErr != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get ingester config: %w", ingErr))
		}
	}
	if ingCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("ingester not found"))
	}

	resp := &apiv1.GetIngesterStatusResponse{
		Id:   req.Msg.Id,
		Type: ingCfg.Type,
	}

	// Local ingester: get live stats from orchestrator.
	if stats := s.orch.GetIngesterStats(id); stats != nil {
		resp.Running = s.orch.IsRunning()
		resp.MessagesIngested = stats.MessagesIngested.Load()
		resp.Errors = stats.Errors.Load()
		resp.BytesIngested = stats.BytesIngested.Load()
	} else if ps := s.findPeerIngesterStats(id); ps != nil {
		// Remote ingester: use peer broadcast stats.
		resp.Running = ps.Running
		resp.MessagesIngested = int64(ps.MessagesIngested) //nolint:gosec // G115: broadcast uses uint64
		resp.Errors = int64(ps.Errors)                     //nolint:gosec // G115: broadcast uses uint64
		resp.BytesIngested = int64(ps.BytesIngested)       //nolint:gosec // G115: broadcast uses uint64
	}

	return connect.NewResponse(resp), nil
}

// PutIngester creates or updates an ingester.
func (s *SystemServer) PutIngester(
	ctx context.Context,
	req *connect.Request[apiv1.PutIngesterRequest],
) (*connect.Response[apiv1.PutIngesterResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ingester type required"))
	}

	id, connErr := parseProtoID(req.Msg.Config.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Reject duplicate names.
	ingesters, err := s.sysStore.ListIngesters(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("ingester", id, req.Msg.Config.Name, ingesters, func(i system.IngesterConfig) (glid.GLID, string) { return i.ID, i.Name }); connErr != nil {
		return nil, connErr
	}

	ingCfg := system.IngesterConfig{
		ID:      id,
		Name:    req.Msg.Config.Name,
		Type:    req.Msg.Config.Type,
		Enabled: req.Msg.Config.Enabled,
		Params:  req.Msg.Config.Params,
		NodeID:  string(req.Msg.Config.NodeId),
	}

	// Auto-assign local node ID when not specified.
	if ingCfg.NodeID == "" {
		ingCfg.NodeID = s.localNodeID
	}

	// Dry-run validation: verify type is known and factory can construct the
	// ingester before persisting. Construction test only runs on the local
	// node — remote ingesters may depend on resources (files, sockets) that
	// only exist on the owning node.
	if ingCfg.Enabled {
		if err := s.validateIngester(ingCfg, ingesters); err != nil {
			return nil, err
		}
	}

	if err := s.sysStore.PutIngester(ctx, ingCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutIngesterResponse{System: cfg}), nil
}

func (s *SystemServer) validateIngester(ingCfg system.IngesterConfig, existing []system.IngesterConfig) error {
	reg, ok := s.factories.IngesterTypes[ingCfg.Type]
	if !ok {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown ingester type: %s", ingCfg.Type))
	}
	if ingCfg.NodeID != s.localNodeID {
		return nil // skip construction test for remote ingesters
	}
	params := ingCfg.Params
	if s.factories.HomeDir != "" {
		params = make(map[string]string, len(ingCfg.Params)+1)
		maps.Copy(params, ingCfg.Params)
		params["_state_dir"] = s.factories.HomeDir
	}
	if _, err := reg.Factory(ingCfg.ID, params, s.factories.Logger); err != nil {
		return errInvalidArg(err)
	}

	// For listener ingesters: (1) reject config-level address collisions
	// with other gastrolog ingesters, then (2) trial-bind to catch ports
	// held by external processes. Skip the trial bind when this ingester
	// is already running — it legitimately holds its own ports.
	if reg.ListenAddrs != nil {
		if err := s.checkListenAddrConflicts(ingCfg, existing); err != nil {
			return err
		}
		if s.orch.GetIngesterStats(ingCfg.ID) == nil {
			if err := checkListenAddrs(reg.ListenAddrs(ingCfg.Params)); err != nil {
				return errInvalidArg(err)
			}
		}
	}

	return nil
}

// checkListenAddrs verifies that all addresses are available to bind.
func checkListenAddrs(addrs []orchestrator.ListenAddr) error {
	for _, a := range addrs {
		if err := tryBind(a.Network, a.Address); err != nil {
			return fmt.Errorf("%s %s: %w", a.Network, a.Address, err)
		}
	}
	return nil
}

// normalizeAddr ensures the address has a host:port format.
// Bare port numbers like "514" become ":514".
func normalizeAddr(address string) string {
	if address == "" {
		return address
	}
	// Already has a colon → assume host:port or :port.
	if strings.Contains(address, ":") {
		return address
	}
	// Bare number → treat as port.
	return ":" + address
}

// tryBind attempts a trial bind on the given network/address and immediately
// closes the listener. Returns nil if the address is available.
func tryBind(network, address string) error {
	address = normalizeAddr(address)
	switch network {
	case "udp":
		pc, err := net.ListenPacket("udp", address)
		if err != nil {
			return err
		}
		return pc.Close()
	default:
		ln, err := net.Listen(network, address)
		if err != nil {
			return err
		}
		return ln.Close()
	}
}

// checkListenAddrConflicts detects address collisions between listener
// ingesters. Two ingesters on the same node cannot bind the same
// network+address pair.
func (s *SystemServer) checkListenAddrConflicts(ingCfg system.IngesterConfig, existing []system.IngesterConfig) error {
	reg := s.factories.IngesterTypes[ingCfg.Type]
	wanted := reg.ListenAddrs(ingCfg.Params)

	for _, other := range existing {
		if other.ID == ingCfg.ID {
			continue // same ingester — updating self
		}
		otherReg, ok := s.factories.IngesterTypes[other.Type]
		if !ok || otherReg.ListenAddrs == nil {
			continue
		}
		// Only compare ingesters on the same node.
		otherNode := other.NodeID
		if otherNode == "" {
			otherNode = s.localNodeID
		}
		if otherNode != ingCfg.NodeID {
			continue
		}
		for _, w := range wanted {
			for _, o := range otherReg.ListenAddrs(other.Params) {
				if w.Network == o.Network && w.Address == o.Address {
					return connect.NewError(connect.CodeInvalidArgument,
						fmt.Errorf("listen address %s %s is already used by ingester %q", w.Network, w.Address, other.Name))
				}
			}
		}
	}
	return nil
}

// DeleteIngester removes an ingester.
func (s *SystemServer) DeleteIngester(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteIngesterRequest],
) (*connect.Response[apiv1.DeleteIngesterResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Verify the ingester exists in config before touching the orchestrator.
	existing, err := s.sysStore.GetIngester(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("ingester not found"))
	}

	// Remove from local runtime. ErrIngesterNotFound is expected when the
	// ingester belongs to another node — the owning node's FSM dispatcher
	// handles its own cleanup.
	if err := s.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
		return nil, errInternal(err)
	}

	// Remove from config store.
	if err := s.sysStore.DeleteIngester(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteIngesterResponse{System: cfg}), nil
}

// GetIngesterDefaults returns default parameter values for each ingester type.
func (s *SystemServer) GetIngesterDefaults(
	ctx context.Context,
	req *connect.Request[apiv1.GetIngesterDefaultsRequest],
) (*connect.Response[apiv1.GetIngesterDefaultsResponse], error) {
	types := make(map[string]*apiv1.IngesterTypeDefaults, len(s.factories.IngesterTypes))
	for name, reg := range s.factories.IngesterTypes {
		td := &apiv1.IngesterTypeDefaults{
			Mode: apiv1.IngesterMode_INGESTER_MODE_ACTIVE,
		}
		if reg.ListenAddrs != nil {
			td.Mode = apiv1.IngesterMode_INGESTER_MODE_PASSIVE
		}
		if reg.Defaults != nil {
			td.Params = reg.Defaults()
		}
		types[name] = td
	}
	return connect.NewResponse(&apiv1.GetIngesterDefaultsResponse{Types: types}), nil
}

// TestIngester tests an ingester configuration without saving it.
// For connection-based ingesters (kafka, mqtt, …) it tests connectivity.
// For listener ingesters (syslog, otlp, …) it checks port availability.
func (s *SystemServer) TestIngester(
	ctx context.Context,
	req *connect.Request[apiv1.TestIngesterRequest],
) (*connect.Response[apiv1.TestIngesterResponse], error) {
	reg, ok := s.factories.IngesterTypes[req.Msg.Type]
	if !ok {
		return connect.NewResponse(&apiv1.TestIngesterResponse{
			Success: false,
			Message: fmt.Sprintf("unknown ingester type %q", req.Msg.Type),
		}), nil
	}

	// Connection-based ingesters: delegate to the registered tester.
	if reg.Tester != nil {
		msg, err := reg.Tester(ctx, req.Msg.Params)
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
	}

	// Listener ingesters: check port availability.
	if reg.ListenAddrs != nil {
		addrs := reg.ListenAddrs(req.Msg.Params)
		// Skip trial bind if this ingester is already running (it holds its ports).
		if len(req.Msg.Id) != 0 {
			if id, connErr := parseProtoID(req.Msg.Id); connErr == nil && s.orch.GetIngesterStats(id) != nil {
				return connect.NewResponse(&apiv1.TestIngesterResponse{
					Success: true,
					Message: "ports held by running ingester",
				}), nil
			}
		}
		if err := checkListenAddrs(addrs); err != nil {
			return connect.NewResponse(&apiv1.TestIngesterResponse{ //nolint:nilerr // port conflict is reported in the response body
				Success: false,
				Message: err.Error(),
			}), nil
		}
		return connect.NewResponse(&apiv1.TestIngesterResponse{
			Success: true,
			Message: "listen addresses available",
		}), nil
	}

	return connect.NewResponse(&apiv1.TestIngesterResponse{
		Success: true,
		Message: "no checks available",
	}), nil
}

// TriggerIngester sends a one-shot trigger to a running ingester.
func (s *SystemServer) TriggerIngester(
	_ context.Context,
	req *connect.Request[apiv1.TriggerIngesterRequest],
) (*connect.Response[apiv1.TriggerIngesterResponse], error) {
	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}
	if err := s.orch.TriggerIngester(id); err != nil {
		return nil, errNotFound(err)
	}
	return connect.NewResponse(&apiv1.TriggerIngesterResponse{}), nil
}

// findPeerIngesterStats returns broadcast stats for a remote ingester, or nil.
func (s *SystemServer) findPeerIngesterStats(id glid.GLID) *apiv1.IngesterNodeStats {
	if s.peerStats == nil {
		return nil
	}
	return s.peerStats.FindIngesterStats(id.String())
}
