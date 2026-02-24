package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"
)

// validateStoreDir checks that a file store's directory does not overlap (nest
// inside or contain) any other file store's directory. Returns an error
// describing the conflict, or nil if the directory is safe.
func (s *ConfigServer) validateStoreDir(ctx context.Context, storeID uuid.UUID, dir string) error {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	// Normalize: ensure trailing separator for prefix comparison.
	normDir := filepath.Clean(absDir) + string(filepath.Separator)

	existing, err := s.cfgStore.ListStores(ctx)
	if err != nil {
		return fmt.Errorf("list stores: %w", err)
	}

	for _, st := range existing {
		if st.ID == storeID {
			continue // Updating self is OK.
		}
		if st.Type != "file" {
			continue // Only check file stores.
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
			return fmt.Errorf("directory %q is already used by store %q", dir, st.ID)
		}
		if strings.HasPrefix(normDir, normOther) {
			return fmt.Errorf("directory %q is nested inside store %q directory %q", dir, st.ID, otherDir)
		}
		if strings.HasPrefix(normOther, normDir) {
			return fmt.Errorf("directory %q contains store %q directory %q", dir, st.ID, otherDir)
		}
	}

	return nil
}

// PutStore creates or updates a store.
func (s *ConfigServer) PutStore(
	ctx context.Context,
	req *connect.Request[apiv1.PutStoreRequest],
) (*connect.Response[apiv1.PutStoreResponse], error) {
	if req.Msg.Config == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config required"))
	}
	if req.Msg.Config.Id == "" {
		req.Msg.Config.Id = uuid.Must(uuid.NewV7()).String()
	}
	if req.Msg.Config.Type == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store type required"))
	}

	storeCfg, err := protoToStoreConfig(req.Msg.Config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Validate file store directory against nesting.
	if storeCfg.Type == "file" {
		if dir := storeCfg.Params["dir"]; dir != "" {
			if err := s.validateStoreDir(ctx, storeCfg.ID, dir); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		}
	}

	// Persist to config store.
	if err := s.cfgStore.PutStore(ctx, storeCfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Apply to runtime: check if store already exists.
	if slices.Contains(s.orch.ListStores(), storeCfg.ID) {
		if err := s.applyExistingStoreChanges(ctx, storeCfg); err != nil {
			return nil, err
		}
	} else {
		if err := s.orch.AddStore(ctx, storeCfg, s.factories); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("add store: %w", err))
		}
	}

	return connect.NewResponse(&apiv1.PutStoreResponse{}), nil
}

func (s *ConfigServer) applyExistingStoreChanges(ctx context.Context, storeCfg config.StoreConfig) error {
	if err := s.orch.ReloadFilters(ctx); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("reload filters: %w", err))
	}
	if err := s.orch.ReloadRotationPolicies(ctx); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("reload rotation policies: %w", err))
	}
	if err := s.orch.ReloadRetentionPolicies(ctx); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("reload retention policies: %w", err))
	}
	if !storeCfg.Enabled {
		_ = s.orch.DisableStore(storeCfg.ID)
	} else {
		_ = s.orch.EnableStore(storeCfg.ID)
	}
	if storeCfg.Type == "file" {
		_ = s.orch.SetStoreCompression(storeCfg.ID, storeCfg.Params["compression"] == "zstd")
	}
	return nil
}

// DeleteStore removes a store. If force is false, the store must be empty.
// If force is true, the store is removed regardless of content: active chunks are sealed,
// all indexes and chunks are deleted, and for file stores the store directory is removed.
func (s *ConfigServer) DeleteStore(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteStoreRequest],
) (*connect.Response[apiv1.DeleteStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	if req.Msg.Force {
		if err := s.forceDeleteStore(ctx, id); err != nil {
			return nil, err
		}
	} else {
		if err := s.removeStore(id); err != nil {
			return nil, err
		}
	}

	if err := s.cfgStore.DeleteStore(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteStoreResponse{}), nil
}

func (s *ConfigServer) forceDeleteStore(ctx context.Context, id uuid.UUID) error {
	storeCfg, err := s.cfgStore.GetStore(ctx, id)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("read store config: %w", err))
	}

	if err := s.orch.ForceRemoveStore(id); err != nil && !errors.Is(err, orchestrator.ErrStoreNotFound) {
		return connect.NewError(connect.CodeInternal, err)
	}

	if storeCfg != nil && storeCfg.Type == "file" {
		if dir := storeCfg.Params["dir"]; dir != "" {
			if err := os.RemoveAll(dir); err != nil {
				return connect.NewError(connect.CodeInternal, fmt.Errorf("remove store directory: %w", err))
			}
		}
	}
	return nil
}

func (s *ConfigServer) removeStore(id uuid.UUID) error {
	err := s.orch.RemoveStore(id)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, orchestrator.ErrStoreNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, orchestrator.ErrStoreNotEmpty):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// PauseStore disables ingestion for a store.
func (s *ConfigServer) PauseStore(
	ctx context.Context,
	req *connect.Request[apiv1.PauseStoreRequest],
) (*connect.Response[apiv1.PauseStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Update runtime state.
	if err := s.orch.DisableStore(id); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Persist to config.
	storeCfg, err := s.cfgStore.GetStore(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if storeCfg != nil {
		storeCfg.Enabled = false
		if err := s.cfgStore.PutStore(ctx, *storeCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.PauseStoreResponse{}), nil
}

// ResumeStore enables ingestion for a store.
func (s *ConfigServer) ResumeStore(
	ctx context.Context,
	req *connect.Request[apiv1.ResumeStoreRequest],
) (*connect.Response[apiv1.ResumeStoreResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Update runtime state.
	if err := s.orch.EnableStore(id); err != nil {
		if errors.Is(err, orchestrator.ErrStoreNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Persist to config.
	storeCfg, err := s.cfgStore.GetStore(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if storeCfg != nil {
		storeCfg.Enabled = true
		if err := s.cfgStore.PutStore(ctx, *storeCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&apiv1.ResumeStoreResponse{}), nil
}

// protoToStoreConfig converts a proto StoreConfig to a config.StoreConfig.
func protoToStoreConfig(p *apiv1.StoreConfig) (config.StoreConfig, error) {
	id, err := uuid.Parse(p.Id)
	if err != nil {
		return config.StoreConfig{}, fmt.Errorf("invalid store ID: %w", err)
	}
	cfg := config.StoreConfig{
		ID:      id,
		Name:    p.Name,
		Type:    p.Type,
		Params:  p.Params,
		Enabled: p.Enabled,
	}
	if p.Filter != "" {
		fid, err := uuid.Parse(p.Filter)
		if err != nil {
			return config.StoreConfig{}, fmt.Errorf("invalid filter ID: %w", err)
		}
		cfg.Filter = new(fid)
	}
	if p.Policy != "" {
		pid, err := uuid.Parse(p.Policy)
		if err != nil {
			return config.StoreConfig{}, fmt.Errorf("invalid policy ID: %w", err)
		}
		cfg.Policy = new(pid)
	}
	for _, pb := range p.RetentionRules {
		b := config.RetentionRule{
			Action: config.RetentionAction(pb.Action),
		}
		rpID, err := uuid.Parse(pb.RetentionPolicyId)
		if err != nil {
			return config.StoreConfig{}, fmt.Errorf("invalid retention policy ID: %w", err)
		}
		b.RetentionPolicyID = rpID
		if pb.DestinationId != "" {
			dstID, err := uuid.Parse(pb.DestinationId)
			if err != nil {
				return config.StoreConfig{}, fmt.Errorf("invalid destination ID: %w", err)
			}
			b.Destination = &dstID
		}
		cfg.RetentionRules = append(cfg.RetentionRules, b)
	}
	return cfg, nil
}
