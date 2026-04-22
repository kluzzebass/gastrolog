package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/units"
)

// PutRotationPolicy creates or updates a rotation policy.
func (s *SystemServer) PutRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRotationPolicyRequest],
) (*connect.Response[apiv1.PutRotationPolicyResponse], error) {
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
	rotPolicies, err := s.sysStore.ListRotationPolicies(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("rotation policy", id, req.Msg.Config.Name, rotPolicies, func(p system.RotationPolicyConfig) (glid.GLID, string) { return p.ID, p.Name }); connErr != nil {
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
		return nil, errInvalidArg(err)
	}

	if err := s.sysStore.PutRotationPolicy(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRotationPolicyPut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutRotationPolicyResponse{System: fullCfg}), nil
}

// DeleteRotationPolicy removes a rotation policy.
func (s *SystemServer) DeleteRotationPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRotationPolicyRequest],
) (*connect.Response[apiv1.DeleteRotationPolicyResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Clear policy reference on any tiers that use it.
	tiers, err := s.sysStore.ListTiers(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, t := range tiers {
		if t.RotationPolicyID != nil && *t.RotationPolicyID == id {
			t.RotationPolicyID = nil
			if err := s.sysStore.PutTier(ctx, t); err != nil {
				return nil, errInternal(err)
			}
		}
	}

	if err := s.sysStore.DeleteRotationPolicy(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteRotationPolicyResponse{System: cfg}), nil
}

// PutRetentionPolicy creates or updates a retention policy.
func (s *SystemServer) PutRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.PutRetentionPolicyRequest],
) (*connect.Response[apiv1.PutRetentionPolicyResponse], error) {
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
	retPolicies, err := s.sysStore.ListRetentionPolicies(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("retention policy", id, req.Msg.Config.Name, retPolicies, func(p system.RetentionPolicyConfig) (glid.GLID, string) { return p.ID, p.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := protoToRetentionPolicy(req.Msg.Config)
	cfg.ID = id
	cfg.Name = req.Msg.Config.Name

	// Validate by trying to convert.
	if _, err := cfg.ToRetentionPolicy(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid retention policy: %w", err))
	}

	if err := s.sysStore.PutRetentionPolicy(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyRetentionPolicyPut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutRetentionPolicyResponse{System: fullCfg}), nil
}

// DeleteRetentionPolicy removes a retention policy.
func (s *SystemServer) DeleteRetentionPolicy(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteRetentionPolicyRequest],
) (*connect.Response[apiv1.DeleteRetentionPolicyResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Clear retention rules that reference this policy from tiers.
	tiers, err := s.sysStore.ListTiers(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, t := range tiers {
		changed := false
		var kept []system.RetentionRule
		for _, b := range t.RetentionRules {
			if b.RetentionPolicyID == id {
				changed = true
				continue
			}
			kept = append(kept, b)
		}
		if changed {
			t.RetentionRules = kept
			if err := s.sysStore.PutTier(ctx, t); err != nil {
				return nil, errInternal(err)
			}
		}
	}

	if err := s.sysStore.DeleteRetentionPolicy(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteRetentionPolicyResponse{System: cfg}), nil
}

// --- Proto <-> Config conversion helpers for policies ---

// protoToRotationPolicy converts a proto RotationPolicyConfig to a system.RotationPolicyConfig.
func protoToRotationPolicy(p *apiv1.RotationPolicyConfig) system.RotationPolicyConfig {
	var cfg system.RotationPolicyConfig

	if p.MaxBytes > 0 {
		s := units.FormatBytesCompact(uint64(p.MaxBytes))
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

// rotationPolicyToProto converts a system.RotationPolicyConfig to a proto RotationPolicyConfig.
func rotationPolicyToProto(cfg system.RotationPolicyConfig) *apiv1.RotationPolicyConfig {
	p := &apiv1.RotationPolicyConfig{}

	if cfg.MaxBytes != nil {
		// Parse the human-readable byte string back to raw bytes.
		if bytes, err := system.ParseBytes(*cfg.MaxBytes); err == nil {
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

// protoToRetentionPolicy converts a proto RetentionPolicyConfig to a system.RetentionPolicyConfig.
func protoToRetentionPolicy(p *apiv1.RetentionPolicyConfig) system.RetentionPolicyConfig {
	var cfg system.RetentionPolicyConfig

	if p.MaxAgeSeconds > 0 {
		s := (time.Duration(p.MaxAgeSeconds) * time.Second).String()
		cfg.MaxAge = &s
	}
	if p.MaxBytes > 0 {
		s := units.FormatBytesCompact(uint64(p.MaxBytes))
		cfg.MaxBytes = &s
	}
	if p.MaxChunks > 0 {
		cfg.MaxChunks = new(p.MaxChunks)
	}

	return cfg
}

// retentionPolicyToProto converts a system.RetentionPolicyConfig to a proto RetentionPolicyConfig.
func retentionPolicyToProto(cfg system.RetentionPolicyConfig) *apiv1.RetentionPolicyConfig {
	p := &apiv1.RetentionPolicyConfig{}

	if cfg.MaxAge != nil {
		if d, err := time.ParseDuration(*cfg.MaxAge); err == nil {
			p.MaxAgeSeconds = int64(d.Seconds())
		}
	}
	if cfg.MaxBytes != nil {
		if bytes, err := system.ParseBytes(*cfg.MaxBytes); err == nil {
			p.MaxBytes = int64(bytes) //nolint:gosec // G115: parsed byte count is always reasonable
		}
	}
	if cfg.MaxChunks != nil {
		p.MaxChunks = *cfg.MaxChunks
	}

	return p
}
