package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/units"
)

// resolveMaxSizeBudget settles a vault's size budget from the wire, where an
// absent max_size_bytes ("unset") and a present 0 ("explicitly zero") are
// distinguishable and mean opposite things (gastrolog-1epfgb). It mutates
// vaultCfg.MaxSizeBytes to the resolved, always-non-zero value.
//
//   - present and 0     → rejected: a 0 budget accepts no records.
//   - present and > 0   → used as given (an operator's explicit choice,
//     including a large value for effectively-unlimited).
//   - absent, creating  → DefaultVaultMaxSizeBytes.
//   - absent, updating   → the existing vault's stored budget is preserved,
//     so an update that does not mention max-size never silently re-defaults
//     a previously-chosen value.
func resolveMaxSizeBudget(p *apiv1.VaultConfig, vaultCfg *system.VaultConfig, existing []system.VaultConfig) *connect.Error {
	// max-size is a disk-claim budget: it applies to file vaults. Memory
	// vaults bound their footprint via memory-budget; leave their (unused)
	// max-size untouched so a UI/CLI that sends 0 for a non-file vault is not
	// read as an explicit-0 rejection.
	if vaultCfg.Type != system.VaultTypeFile {
		return nil
	}
	if p.MaxSizeBytes != nil {
		if *p.MaxSizeBytes == 0 {
			return errInvalidArg(fmt.Errorf(
				"max-size of 0 accepts no records for vault %q; omit it for the default (%s) or set a large value for effectively-unlimited",
				vaultCfg.Name, units.FormatBytesDisplay(int64(system.DefaultVaultMaxSizeBytes))))
		}
		vaultCfg.MaxSizeBytes = *p.MaxSizeBytes
		return nil
	}
	for i := range existing {
		if existing[i].ID == vaultCfg.ID {
			vaultCfg.MaxSizeBytes = existing[i].MaxSizeBytes // preserve on update
			return nil
		}
	}
	vaultCfg.MaxSizeBytes = system.DefaultVaultMaxSizeBytes // default on create
	return nil
}

// checkVaultShapeImmutable rejects PutVault when an existing vault's shape
// fields (type, cloud_service_id) would change. New vaults pass through —
// the existing-vault lookup returns nil and we have nothing to compare.
// See gastrolog-3ul0s for the failure mode this guards against.
func checkVaultShapeImmutable(ctx context.Context, store system.Store, incoming system.VaultConfig) *connect.Error {
	existing, err := store.GetVault(ctx, incoming.ID)
	if err != nil {
		return errInternal(err)
	}
	if existing == nil {
		return nil // creating a new vault — no shape to preserve
	}
	if existing.Type != incoming.Type {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault type is immutable: cannot change %s → %s on existing vault %q (create a new vault and migrate)",
				existing.Type, incoming.Type, incoming.Name))
	}
	if !cloudServiceIDEqual(existing.CloudServiceID, incoming.CloudServiceID) {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault cloud_service_id is immutable on existing vault %q: changing it would orphan blobs in the old bucket (create a new vault and migrate)",
				incoming.Name))
	}
	return nil
}

// cloudServiceIDEqual compares two optional *glid.GLID values. Both nil and
// both pointing at the same GLID are equal; everything else is a change.
func cloudServiceIDEqual(a, b *glid.GLID) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// PutVault creates or updates a vault.
func (s *SystemServer) PutVault(
	ctx context.Context,
	req *connect.Request[apiv1.PutVaultRequest],
) (*connect.Response[apiv1.PutVaultResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
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

	// Resolve the size budget: the wire distinguishes "unset" (absent) from
	// "explicitly 0" (present, zero), and they mean opposite things
	// (gastrolog-1epfgb). This is the single ingress for every surface — CLI
	// create, UI, and config import all call PutVault — so resolving here
	// makes an unbounded vault unrepresentable regardless of who asked.
	if connErr := resolveMaxSizeBudget(req.Msg.Config, &vaultCfg, vaults); connErr != nil {
		return nil, connErr
	}

	// Shape immutability: `type` and `cloud_service_id` determine *where*
	// and *how* this vault's chunks are stored. Changing either on a vault
	// with existing chunks would either reinterpret on-disk layout with
	// the wrong manager (type swap) or orphan blobs in the old cloud
	// bucket while pointing the new manager at an empty one (cloud-service
	// swap). The running orchestrator's applyExistingVaultChanges only
	// reloads filters / rotation / retention, so the change looks like a
	// no-op in dev — until the next restart rebuilds the manager from the
	// updated config. See gastrolog-3ul0s.
	if connErr := checkVaultShapeImmutable(ctx, s.sysStore, vaultCfg); connErr != nil {
		return nil, connErr
	}

	// Note: vault ID validation is intentionally omitted here.
	// RouteLeader RPCs run on any node with Raft writes forwarded to the leader,
	// but reads are local. In a multi-node cluster, vaults created moments before
	// the vault may not have replicated to this node's FSM yet. The orchestrator's
	// buildVaultInstances handles missing vaults gracefully (logs a warning, skips).
	// Referential integrity is enforced on the delete path (DeleteVault rejects
	// if any vault references the vault).

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
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
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
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
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
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
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
// Delegates to convert.VaultConfigFromProto so the field mapping has one
// source of truth (shared with the FSM command path).
func protoToVaultConfig(p *apiv1.VaultConfig) (system.VaultConfig, error) {
	return convert.VaultConfigFromProto(p)
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
