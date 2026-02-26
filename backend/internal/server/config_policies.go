package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
)

// PutRotationPolicy creates or updates a rotation policy.
func (s *ConfigServer) PutRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRotationPolicyRequest],
) (*connect.Response[apiv1.PutRotationPolicyResponse], error) {
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

	cfg := protoToRotationPolicy(req.Msg.Config)
	cfg.ID = id
	cfg.Name = req.Msg.Config.Name

	// Validate by trying to convert.
	if _, err := cfg.ToRotationPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid rotation policy: %w", err))
	}
	if err := cfg.ValidateCron(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := s.cfgStore.PutRotationPolicy(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Hot-reload rotation policies for running vaults.
	if err := s.orch.ReloadRotationPolicies(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload rotation policies: %w", err))
	}

	return connect.NewResponse(&apiv1.PutRotationPolicyResponse{}), nil
}

// DeleteRotationPolicy removes a rotation policy.
func (s *ConfigServer) DeleteRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRotationPolicyRequest],
) (*connect.Response[apiv1.DeleteRotationPolicyResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Clear policy reference on any vaults that use it.
	stores, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		if st.Policy != nil && *st.Policy == id {
			st.Policy = nil
			if err := s.cfgStore.PutVault(ctx, st); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}

	if err := s.cfgStore.DeleteRotationPolicy(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteRotationPolicyResponse{}), nil
}

// PutRetentionPolicy creates or updates a retention policy.
func (s *ConfigServer) PutRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRetentionPolicyRequest],
) (*connect.Response[apiv1.PutRetentionPolicyResponse], error) {
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

	cfg := protoToRetentionPolicy(req.Msg.Config)
	cfg.ID = id
	cfg.Name = req.Msg.Config.Name

	// Validate by trying to convert.
	if _, err := cfg.ToRetentionPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid retention policy: %w", err))
	}

	if err := s.cfgStore.PutRetentionPolicy(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Hot-reload retention policies for running vaults.
	if err := s.orch.ReloadRetentionPolicies(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload retention policies: %w", err))
	}

	return connect.NewResponse(&apiv1.PutRetentionPolicyResponse{}), nil
}

// DeleteRetentionPolicy removes a retention policy.
func (s *ConfigServer) DeleteRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRetentionPolicyRequest],
) (*connect.Response[apiv1.DeleteRetentionPolicyResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Clear retention rules that reference this policy.
	stores, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, st := range stores {
		changed := false
		var kept []config.RetentionRule
		for _, b := range st.RetentionRules {
			if b.RetentionPolicyID == id {
				changed = true
				continue
			}
			kept = append(kept, b)
		}
		if changed {
			st.RetentionRules = kept
			if err := s.cfgStore.PutVault(ctx, st); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}

	if err := s.cfgStore.DeleteRetentionPolicy(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.DeleteRetentionPolicyResponse{}), nil
}

// --- Proto <-> Config conversion helpers for policies ---

// protoToRotationPolicy converts a proto RotationPolicyConfig to a config.RotationPolicyConfig.
func protoToRotationPolicy(p *apiv1.RotationPolicyConfig) config.RotationPolicyConfig {
	var cfg config.RotationPolicyConfig

	if p.MaxBytes > 0 {
		s := formatBytes(uint64(p.MaxBytes))
		cfg.MaxBytes = &s
	}
	if p.MaxAgeSeconds > 0 {
		s := (time.Duration(p.MaxAgeSeconds) * time.Second).String()
		cfg.MaxAge = &s
	}
	if p.MaxRecords > 0 {
		cfg.MaxRecords = new(p.MaxRecords)
	}
	if p.Cron != "" {
		cfg.Cron = new(p.Cron)
	}

	return cfg
}

// rotationPolicyToProto converts a config.RotationPolicyConfig to a proto RotationPolicyConfig.
func rotationPolicyToProto(cfg config.RotationPolicyConfig) *apiv1.RotationPolicyConfig {
	p := &apiv1.RotationPolicyConfig{}

	if cfg.MaxBytes != nil {
		// Parse the human-readable byte string back to raw bytes.
		if bytes, err := config.ParseBytes(*cfg.MaxBytes); err == nil {
			p.MaxBytes = int64(bytes) //nolint:gosec // G115: parsed byte count is always reasonable
		}
	}
	if cfg.MaxAge != nil {
		if d, err := time.ParseDuration(*cfg.MaxAge); err == nil {
			p.MaxAgeSeconds = int64(d.Seconds())
		}
	}
	if cfg.MaxRecords != nil {
		p.MaxRecords = *cfg.MaxRecords
	}
	if cfg.Cron != nil {
		p.Cron = *cfg.Cron
	}

	return p
}

// protoToRetentionPolicy converts a proto RetentionPolicyConfig to a config.RetentionPolicyConfig.
func protoToRetentionPolicy(p *apiv1.RetentionPolicyConfig) config.RetentionPolicyConfig {
	var cfg config.RetentionPolicyConfig

	if p.MaxAgeSeconds > 0 {
		s := (time.Duration(p.MaxAgeSeconds) * time.Second).String()
		cfg.MaxAge = &s
	}
	if p.MaxBytes > 0 {
		s := formatBytes(uint64(p.MaxBytes))
		cfg.MaxBytes = &s
	}
	if p.MaxChunks > 0 {
		cfg.MaxChunks = new(p.MaxChunks)
	}

	return cfg
}

// retentionPolicyToProto converts a config.RetentionPolicyConfig to a proto RetentionPolicyConfig.
func retentionPolicyToProto(cfg config.RetentionPolicyConfig) *apiv1.RetentionPolicyConfig {
	p := &apiv1.RetentionPolicyConfig{}

	if cfg.MaxAge != nil {
		if d, err := time.ParseDuration(*cfg.MaxAge); err == nil {
			p.MaxAgeSeconds = int64(d.Seconds())
		}
	}
	if cfg.MaxBytes != nil {
		if bytes, err := config.ParseBytes(*cfg.MaxBytes); err == nil {
			p.MaxBytes = int64(bytes) //nolint:gosec // G115: parsed byte count is always reasonable
		}
	}
	if cfg.MaxChunks != nil {
		p.MaxChunks = *cfg.MaxChunks
	}

	return p
}
