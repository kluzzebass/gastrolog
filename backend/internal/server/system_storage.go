package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/blobstore"
	"gastrolog/internal/convert"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// --- Cloud Services ---

// PutCloudService creates or updates a cloud service.
func (s *SystemServer) PutCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.PutCloudServiceRequest],
) (*connect.Response[apiv1.PutCloudServiceResponse], error) {
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
	if connErr := checkNameConflict(ctx, s.sysStore, "cloud service", id, req.Msg.Config.Name, s.sysStore.ListCloudServices,
		func(cs system.CloudService) (glid.GLID, string) { return cs.ID, cs.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := convert.CloudServiceFromProto(req.Msg.Config)
	cfg.ID = id

	// Reads redact credentials, so a client editing an existing service has
	// none to send back: an empty credential field keeps the stored value.
	// Clearing instead would strip credentials on the first save after a
	// config read and lock the cluster out of sealed chunks already in the
	// object store. clear_credentials is the explicit way to drop them and
	// fall back to the provider's ambient chain. Merged before validation so
	// the checks below see the credentials the service will actually run with.
	//
	// getConfirmed, not a plain read: the handler runs wherever the request
	// landed, and a node that has not applied the service yet would find
	// nothing to preserve and store the service with its credentials wiped.
	existing, err := getConfirmed(ctx, s.sysStore, id, s.sysStore.GetCloudService)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing != nil && !req.Msg.ClearCredentials {
		cfg = cfg.WithPreservedCredentials(*existing)
	}
	cfg = cfg.WithoutUnusedCredentials()

	// Config-accept validation: reject configs that would fail blobstore store
	// creation at vault init, so a bad provider config (bare endpoint, missing
	// bucket, …) errors here — visible to the CLI/UI/API caller — instead of
	// persisting and killing vault init on every node. Deterministic shape
	// checks only (blobstore.ValidateConfig): same verdict on every node, no
	// network, and it runs before the Raft apply — never inside the FSM apply
	// path, where a rejection would break replay of persisted state.
	if err := blobstore.ValidateConfig(cfg.Provider, cfg.StoreParams()); err != nil {
		return nil, errInvalidArg(fmt.Errorf("cloud service %q: %w", cfg.Name, err))
	}

	if err := s.sysStore.PutCloudService(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.logCloudCredentialChange(existing, cfg)
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServicePut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutCloudServiceResponse{System: fullCfg}), nil
}

// logCloudCredentialChange records credential material leaving a cloud
// service, after the write it describes has succeeded. Credentials are
// write-only, so the log is the only place the change is visible at all —
// and a service that quietly stops carrying credentials degrades to
// whatever ambient chain the provider finds, which is a change an operator
// must be able to find after the fact.
//
// Any credential material leaving counts, not just a complete set: an S3
// service holding only an access key still has a secret to lose.
func (s *SystemServer) logCloudCredentialChange(prev *system.CloudService, next system.CloudService) {
	if s.logger == nil || prev == nil {
		return
	}
	if !carriesCredentials(prev.StoreParams()) || carriesCredentials(next.StoreParams()) {
		return
	}
	s.logger.Warn("cloud service credentials removed",
		"cloud_service", next.ID, "name", next.Name, "provider", next.Provider)
}

// DeleteCloudService removes a cloud service.
func (s *SystemServer) DeleteCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCloudServiceRequest],
) (*connect.Response[apiv1.DeleteCloudServiceResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	existing, err := s.sysStore.GetCloudService(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("cloud service not found"))
	}

	// Referential integrity: reject if any vault references this cloud service.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	for _, v := range vaults {
		if v.CloudServiceID != nil && *v.CloudServiceID == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("cloud service %q is referenced by vault %q", req.Msg.Id, v.ID))
		}
	}

	if err := s.sysStore.DeleteCloudService(ctx, id); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServiceDeleted, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteCloudServiceResponse{System: cfg}), nil
}

// --- Node Storage ---

// SetNodeStorageConfig creates or updates a node storage configuration.
func (s *SystemServer) SetNodeStorageConfig(
	ctx context.Context,
	req *connect.Request[apiv1.SetNodeStorageConfigRequest],
) (*connect.Response[apiv1.SetNodeStorageConfigResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.NodeId) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id required"))
	}

	cfg := convert.NodeStorageConfigFromProto(req.Msg.Config)

	// Assign UUIDs to file storages that don't have one.
	for i := range cfg.FileStorages {
		if cfg.FileStorages[i].ID == glid.Nil {
			cfg.FileStorages[i].ID = glid.New()
		}
	}

	for _, fs := range cfg.FileStorages {
		if connErr := validateFileStorageExpressions(fs); connErr != nil {
			return nil, connErr
		}
	}

	if err := s.sysStore.SetNodeStorageConfig(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyNodeStorageConfigSet})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.SetNodeStorageConfigResponse{System: fullCfg}), nil
}

// validateFileStorageExpressions parse-checks a file storage's disk-guard
// free-space thresholds. They live on the storage because they guard the
// volume a storage entity represents, not the vaults placed on it. An
// empty value legitimately means "inherit the node default"; a percentage
// of the volume ("10%") is allowed alongside an absolute size because the
// threshold guards this storage's own volume, so a share composes; an
// explicit zero ("0", "0%") would disable the guard for this storage and
// is rejected, like the vault-quantity explicit-0 rule.
func validateFileStorageExpressions(fs system.FileStorage) *connect.Error {
	for _, f := range []struct {
		flag string
		expr string
	}{
		{"disk-free-warn", fs.DiskFreeWarn},
		{"disk-free-floor", fs.DiskFreeFloor},
	} {
		if system.IsQuantityUnset(f.expr) {
			continue
		}
		sp, err := system.ParseSizeOrPercent(f.expr)
		if err != nil {
			return errInvalidArg(fmt.Errorf("%s %q on storage %q: %w", f.flag, f.expr, fs.Name, err))
		}
		if sp.IsZero() {
			return errInvalidArg(fmt.Errorf(
				"%s of %q on storage %q disables the guard; omit it to inherit the node default, or set a real size or percentage",
				f.flag, f.expr, fs.Name))
		}
	}
	return nil
}

// --- Proto <-> Config conversion ---
//
// Canonical converters live in the convert package.
