package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
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

	// Reject policies with no conditions: they're silent no-ops that almost
	// always reflect operator confusion rather than intent. See gastrolog-1rbuf.
	if cfg.IsEmpty() {
		return nil, errInvalidArg(errors.New("rotation policy must set at least one of maxBytes, maxAge, maxRecords, or cron"))
	}

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

	// Clear policy reference on any vaults that use it.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, v := range vaults {
		if v.RotationPolicyID != nil && *v.RotationPolicyID == id {
			v.RotationPolicyID = nil
			if err := s.sysStore.PutVault(ctx, v); err != nil {
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

	// max_size parse-checks FIRST, before the IsEmpty no-op gate below
	// (gastrolog-33ul6h finding 7, carried forward from an earlier,
	// superseded design's separate refuse-bound field): IsEmpty's
	// positiveSize check treats an
	// unparseable expression the same as absent (both contribute nothing to
	// "is this policy non-empty"), so an otherwise-empty policy with a
	// garbled max_size would fall into IsEmpty()==true and surface the
	// generic "must set at least one" error instead of the actual parse
	// failure — the wrong diagnostic for what the operator typed. Checking
	// parseability first ensures a malformed max_size always gets its own
	// error, regardless of what else the policy sets.
	//
	// Must parse, and an explicit "0" is rejected: max_size is now both the
	// drain trigger and the refuse bound, and "0" would mean "no bound", not
	// "no restriction" — the state resolution treats as absent, so it must
	// not be sayable.
	if connErr := validateRetentionMaxSize(cfg.MaxSize, cfg.Name); connErr != nil {
		return nil, connErr
	}

	// Reject policies with no conditions: they're silent no-ops that almost
	// always reflect operator confusion rather than intent. See
	// gastrolog-1rbuf.
	if cfg.IsEmpty() {
		return nil, errInvalidArg(errors.New("retention policy must set at least one of maxAge, maxSize, or maxChunks"))
	}

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

	// Clear retention rules that reference this policy from vaults.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, v := range vaults {
		changed := false
		var kept []system.RetentionRule
		for _, b := range v.RetentionRules {
			if b.RetentionPolicyID == id {
				changed = true
				continue
			}
			kept = append(kept, b)
		}
		if changed {
			v.RetentionRules = kept
			if err := s.sysStore.PutVault(ctx, v); err != nil {
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

// validateRetentionMaxSize parse-checks a retention policy's max_size —
// the combined drain-trigger-and-refuse-bound quantity (gastrolog-33ul6h):
// unset is fine (no bound stored — the vault simply has no size bound;
// only when no attached policy has one at all); an unparseable expression is
// rejected at write; an explicit "0" is rejected because it means "no
// bound", the unrepresentable state this model exists to prevent.
func validateRetentionMaxSize(maxSize *string, policyName string) *connect.Error {
	if maxSize == nil || system.IsQuantityUnset(*maxSize) {
		return nil
	}
	bytes, err := system.ParseSize(*maxSize)
	if err != nil {
		return errInvalidArg(fmt.Errorf("max-size %q on retention policy %q: %w", *maxSize, policyName, err))
	}
	if bytes == 0 {
		return errInvalidArg(fmt.Errorf(
			"max-size of %q on retention policy %q means no bound; omit it to leave this policy's bound unset, or set a real size",
			*maxSize, policyName))
	}
	return nil
}

// --- Proto <-> Config conversion helpers for policies ---

// protoToRotationPolicy converts a proto RotationPolicyConfig to a system.RotationPolicyConfig.
// Numeric passthrough: quantities stay numbers at rest — the old converters
// FORMATTED numbers into human strings for storage and parsed them back on
// every read, so stored config meaning depended on the parser du jour.
func protoToRotationPolicy(p *apiv1.RotationPolicyConfig) system.RotationPolicyConfig {
	var cfg system.RotationPolicyConfig

	if p.MaxSize != "" {
		cfg.MaxSize = new(p.MaxSize)
	}
	if p.MaxAge != "" {
		cfg.MaxAge = new(p.MaxAge)
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

	if cfg.MaxSize != nil {
		p.MaxSize = *cfg.MaxSize
	}
	if cfg.MaxAge != nil {
		p.MaxAge = *cfg.MaxAge
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

	if p.MaxAge != "" {
		cfg.MaxAge = new(p.MaxAge)
	}
	if p.MaxSize != "" {
		cfg.MaxSize = new(p.MaxSize)
	}
	if p.MaxChunks > 0 {
		cfg.MaxChunks = new(p.MaxChunks)
	}
	// refuse is genuinely tri-state (unset must default to false —
	// gastrolog-5yfaqj: bounds are drain-first, refusal is the explicit
	// hard mode), unlike the other fields above which use the
	// empty-string/zero convention — so it's the one field carried as
	// *bool straight through, matching the proto's own optional bool
	// representation.
	cfg.Refuse = p.Refuse

	return cfg
}

// retentionPolicyToProto converts a system.RetentionPolicyConfig to a proto RetentionPolicyConfig.
func retentionPolicyToProto(cfg system.RetentionPolicyConfig) *apiv1.RetentionPolicyConfig {
	p := &apiv1.RetentionPolicyConfig{}

	if cfg.MaxAge != nil {
		p.MaxAge = *cfg.MaxAge
	}
	if cfg.MaxSize != nil {
		p.MaxSize = *cfg.MaxSize
	}
	if cfg.MaxChunks != nil {
		p.MaxChunks = *cfg.MaxChunks
	}
	p.Refuse = cfg.Refuse

	return p
}
