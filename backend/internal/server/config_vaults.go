package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/orchestrator"
)

// validateVaultDir checks that a file vault's directory does not overlap (nest
// inside or contain) any other file vault's directory. Returns an error
// describing the conflict, or nil if the directory is safe.
func (s *ConfigServer) validateVaultDir(ctx context.Context, vaultID uuid.UUID, dir string) error {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	// Normalize: ensure trailing separator for prefix comparison.
	normDir := filepath.Clean(absDir) + string(filepath.Separator)

	existing, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return fmt.Errorf("list vaults: %w", err)
	}

	for _, st := range existing {
		if st.ID == vaultID {
			continue // Updating self is OK.
		}
		if st.Type != "file" {
			continue // Only check file vaults.
		}
		otherDir := st.Params["dir"]
		if otherDir == "" {
			continue
		}
		absOther, err := filepath.Abs(otherDir)
		if err != nil {
			continue // Can't resolve — skip.
		}
		normOther := filepath.Clean(absOther) + string(filepath.Separator)

		// Check for exact match or nesting in either direction.
		if normDir == normOther {
			return fmt.Errorf("directory %q is already used by vault %q", dir, st.ID)
		}
		if strings.HasPrefix(normDir, normOther) {
			return fmt.Errorf("directory %q is nested inside vault %q directory %q", dir, st.ID, otherDir)
		}
		if strings.HasPrefix(normOther, normDir) {
			return fmt.Errorf("directory %q contains vault %q directory %q", dir, st.ID, otherDir)
		}
	}

	return nil
}

// PutVault creates or updates a vault.
func (s *ConfigServer) PutVault(
	ctx context.Context,
	req *connect.Request[apiv1.PutVaultRequest],
) (*connect.Response[apiv1.PutVaultResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault type required"))
	}

	vaultCfg, err := protoToVaultConfig(req.Msg.Config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Auto-assign local node ID when not specified.
	if vaultCfg.NodeID == "" {
		vaultCfg.NodeID = s.localNodeID
	}

	// Validate file vault directory against nesting.
	if vaultCfg.Type == "file" {
		if dir := vaultCfg.Params["dir"]; dir != "" {
			if err := s.validateVaultDir(ctx, vaultCfg.ID, dir); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		}
	}

	// Persist to config store. For raft stores, the FSM notification callback
	// handles orchestrator side effects. For non-raft stores, notify() does.
	if err := s.cfgStore.PutVault(ctx, vaultCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultCfg.ID})

	return connect.NewResponse(&apiv1.PutVaultResponse{}), nil
}

// DeleteVault removes a vault. If force is false, the vault must be empty.
// If force is true, the vault is removed regardless of content: active chunks are sealed,
// all indexes and chunks are deleted, and for file vaults the vault directory is removed.
func (s *ConfigServer) DeleteVault(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteVaultRequest],
) (*connect.Response[apiv1.DeleteVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Verify the vault exists in config before touching the orchestrator.
	// The vault may belong to another node (not in local orchestrator) but
	// must exist in the shared config store.
	existing, err := s.cfgStore.GetVault(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
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

	if err := s.cfgStore.DeleteVault(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteVaultResponse{}), nil
}

func (s *ConfigServer) forceDeleteVault(id uuid.UUID) error {
	if err := s.orch.ForceRemoveVault(id); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return connect.NewError(connect.CodeInternal, err)
	}
	// Directory cleanup is handled by the FSM dispatcher on the owning node
	// (see configDispatcher.handleVaultDeleted). Doing it here would remove
	// the directory on whichever node handles the HTTP request, which may
	// not be the node that owns the vault's data.
	return nil
}

func (s *ConfigServer) removeVault(id uuid.UUID) error {
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
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// PauseVault disables ingestion for a vault.
// It reads the current config, flips Enabled to false, and writes it back.
// The VaultPut FSM notification handles the runtime DisableVault call.
func (s *ConfigServer) PauseVault(
	ctx context.Context,
	req *connect.Request[apiv1.PauseVaultRequest],
) (*connect.Response[apiv1.PauseVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.cfgStore.GetVault(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = false
	if err := s.cfgStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	return connect.NewResponse(&apiv1.PauseVaultResponse{}), nil
}

// ResumeVault enables ingestion for a vault.
// It reads the current config, flips Enabled to true, and writes it back.
// The VaultPut FSM notification handles the runtime EnableVault call.
func (s *ConfigServer) ResumeVault(
	ctx context.Context,
	req *connect.Request[apiv1.ResumeVaultRequest],
) (*connect.Response[apiv1.ResumeVaultResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.cfgStore.GetVault(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = true
	if err := s.cfgStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	return connect.NewResponse(&apiv1.ResumeVaultResponse{}), nil
}

// protoToVaultConfig converts a proto VaultConfig to a config.VaultConfig.
func protoToVaultConfig(p *apiv1.VaultConfig) (config.VaultConfig, error) {
	id, err := uuid.Parse(p.Id)
	if err != nil {
		return config.VaultConfig{}, fmt.Errorf("invalid vault ID: %w", err)
	}
	cfg := config.VaultConfig{
		ID:      id,
		Name:    p.Name,
		Type:    p.Type,
		Params:  p.Params,
		Enabled: p.Enabled,
		NodeID:  p.NodeId,
	}
	if p.Filter != "" {
		fid, err := uuid.Parse(p.Filter)
		if err != nil {
			return config.VaultConfig{}, fmt.Errorf("invalid filter ID: %w", err)
		}
		cfg.Filter = new(fid)
	}
	if p.Policy != "" {
		pid, err := uuid.Parse(p.Policy)
		if err != nil {
			return config.VaultConfig{}, fmt.Errorf("invalid policy ID: %w", err)
		}
		cfg.Policy = new(pid)
	}
	for _, pb := range p.RetentionRules {
		b := config.RetentionRule{
			Action: config.RetentionAction(pb.Action),
		}
		rpID, err := uuid.Parse(pb.RetentionPolicyId)
		if err != nil {
			return config.VaultConfig{}, fmt.Errorf("invalid retention policy ID: %w", err)
		}
		b.RetentionPolicyID = rpID
		if pb.DestinationId != "" {
			dstID, err := uuid.Parse(pb.DestinationId)
			if err != nil {
				return config.VaultConfig{}, fmt.Errorf("invalid destination ID: %w", err)
			}
			b.Destination = &dstID
		}
		cfg.RetentionRules = append(cfg.RetentionRules, b)
	}
	return cfg, nil
}
