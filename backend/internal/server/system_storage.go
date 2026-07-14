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
	services, err := s.sysStore.ListCloudServices(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("cloud service", id, req.Msg.Config.Name, services, func(cs system.CloudService) (glid.GLID, string) { return cs.ID, cs.Name }); connErr != nil {
		return nil, connErr
	}

	cfg := convert.CloudServiceFromProto(req.Msg.Config)
	cfg.ID = id

	// Config-accept validation (gastrolog-7au6u9): reject configs that
	// would fail blobstore store creation at vault init, so a bad
	// provider config (bare endpoint, missing bucket, …) errors here —
	// visible to the CLI/UI/API caller — instead of persisting and
	// killing vault init on every node. Deterministic shape checks only
	// (blobstore.ValidateConfig): same verdict on every node, no network,
	// and it runs before the Raft apply — never inside the FSM apply
	// path, where a rejection would break replay of persisted state.
	if err := blobstore.ValidateConfig(cfg.Provider, cfg.StoreParams()); err != nil {
		return nil, errInvalidArg(fmt.Errorf("cloud service %q: %w", cfg.Name, err))
	}

	if err := s.sysStore.PutCloudService(ctx, cfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyCloudServicePut, ID: id})

	fullCfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutCloudServiceResponse{System: fullCfg}), nil
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

// --- Proto <-> Config conversion ---
//
// Canonical converters live in the convert package (gastrolog-2f8et).
