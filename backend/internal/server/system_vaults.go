package server

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/orchestrator"
)

// PutVault creates or updates a vault.
func (s *SystemServer) PutVault(
	ctx context.Context,
	req *connect.Request[apiv1.PutVaultRequest],
) (*connect.Response[apiv1.PutVaultResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = glid.New().String()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	vaultCfg, err := protoToVaultConfig(req.Msg.Config)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	// Reject duplicate names.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("vault", vaultCfg.ID, vaultCfg.Name, vaults, func(v system.VaultConfig) (glid.GLID, string) { return v.ID, v.Name }); connErr != nil {
		return nil, connErr
	}

	// Note: tier ID validation is intentionally omitted here.
	// RouteLeader RPCs run on any node with Raft writes forwarded to the leader,
	// but reads are local. In a multi-node cluster, tiers created moments before
	// the vault may not have replicated to this node's FSM yet. The orchestrator's
	// buildTierInstances handles missing tiers gracefully (logs a warning, skips).
	// Referential integrity is enforced on the delete path (DeleteTier rejects
	// if any vault references the tier).

	// Persist to config store. For raft stores, the FSM notification callback
	// handles orchestrator side effects. For non-raft stores, notify() does.
	if err := s.sysStore.PutVault(ctx, vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultCfg.ID})

	// Run placement synchronously so the response includes placements.
	if s.placementReconcile != nil {
		s.placementReconcile(ctx)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutVaultResponse{System: cfg}), nil
}

// DeleteVault removes a vault. If force is false, the vault must be empty.
// If force is true, the vault is removed regardless of content: active chunks are sealed,
// all indexes and chunks are deleted, and for file vaults the vault directory is removed.
func (s *SystemServer) DeleteVault(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteVaultRequest],
) (*connect.Response[apiv1.DeleteVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Verify the vault exists in config before touching the orchestrator.
	// The vault may belong to another node (not in local orchestrator) but
	// must exist in the shared config store.
	existing, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	// Referential integrity: reject if any route references this vault as a destination.
	if routeID, used, err := s.vaultReferencedByRoute(ctx, id); err != nil {
		return nil, errInternal(err)
	} else if used {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault %q is referenced as destination in route %q", req.Msg.Id, routeID))
	}

	if req.Msg.Force {
		if err := s.forceDeleteVault(id); err != nil {
			return nil, err
		}
	} else {
		if err := s.removeVault(id); err != nil {
			return nil, err
		}
	}

	if err := s.sysStore.DeleteVault(ctx, id, req.Msg.GetDeleteData()); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteVaultResponse{System: cfg}), nil
}

func (s *SystemServer) forceDeleteVault(id glid.GLID) error {
	if err := s.orch.ForceRemoveVault(id); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return errInternal(err)
	}
	return nil
}

func (s *SystemServer) removeVault(id glid.GLID) error {
	err := s.orch.RemoveVault(id)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, orchestrator.ErrVaultNotFound):
		// Expected when the vault belongs to another node — the owning
		// node's FSM dispatcher handles its own runtime cleanup.
		return nil
	case errors.Is(err, orchestrator.ErrVaultNotEmpty):
		return errPrecondition(err)
	default:
		return errInternal(err)
	}
}

// PauseVault disables ingestion for a vault.
// It reads the current config, flips Enabled to false, and writes it back.
// The VaultPut FSM notification handles the runtime DisableVault call.
func (s *SystemServer) PauseVault(
	ctx context.Context,
	req *connect.Request[apiv1.PauseVaultRequest],
) (*connect.Response[apiv1.PauseVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = false
	if err := s.sysStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PauseVaultResponse{System: cfg}), nil
}

// ResumeVault enables ingestion for a vault.
// It reads the current config, flips Enabled to true, and writes it back.
// The VaultPut FSM notification handles the runtime EnableVault call.
func (s *SystemServer) ResumeVault(
	ctx context.Context,
	req *connect.Request[apiv1.ResumeVaultRequest],
) (*connect.Response[apiv1.ResumeVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = true
	if err := s.sysStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.ResumeVaultResponse{System: cfg}), nil
}

// protoToVaultConfig converts a proto VaultConfig to a system.VaultConfig.
func protoToVaultConfig(p *apiv1.VaultConfig) (system.VaultConfig, error) {
	id, err := glid.ParseUUID(p.Id)
	if err != nil {
		return system.VaultConfig{}, fmt.Errorf("invalid vault ID: %w", err)
	}
	cfg := system.VaultConfig{
		ID:      id,
		Name:    p.Name,
		Enabled: p.Enabled,
	}
	return cfg, nil
}

// CloudServiceTester validates connectivity for a cloud storage configuration.
type CloudServiceTester func(ctx context.Context, params map[string]string) (string, error)

// TestCloudService tests connectivity for a cloud storage configuration without saving it.
func (s *SystemServer) TestCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.TestCloudServiceRequest],
) (*connect.Response[apiv1.TestCloudServiceResponse], error) {
	tester := s.cloudTesters[req.Msg.Type]
	if tester == nil {
		return connect.NewResponse(&apiv1.TestCloudServiceResponse{
			Success: false,
			Message: fmt.Sprintf("connection test not supported for cloud service type %q", req.Msg.Type),
		}), nil
	}

	msg, err := tester(ctx, req.Msg.Params)
	if err != nil {
		return connect.NewResponse(&apiv1.TestCloudServiceResponse{ //nolint:nilerr // test failure is reported in the response body, not as an RPC error
			Success: false,
			Message: err.Error(),
		}), nil
	}
	return connect.NewResponse(&apiv1.TestCloudServiceResponse{
		Success: true,
		Message: msg,
	}), nil
}
