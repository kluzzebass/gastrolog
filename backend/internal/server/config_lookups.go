package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config/raftfsm"
	"time"
)

// ListManagedFiles returns all uploaded managed files.
func (s *ConfigServer) ListManagedFiles(
	ctx context.Context,
	_ *connect.Request[apiv1.ListManagedFilesRequest],
) (*connect.Response[apiv1.ListManagedFilesResponse], error) {
	files, err := s.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	out := make([]*apiv1.ManagedFileInfo, len(files))
	for i, f := range files {
		out[i] = &apiv1.ManagedFileInfo{
			Id:         f.ID.String(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	return connect.NewResponse(&apiv1.ListManagedFilesResponse{Files: out}), nil
}

// DeleteManagedFile removes an uploaded managed file.
func (s *ConfigServer) DeleteManagedFile(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteManagedFileRequest],
) (*connect.Response[apiv1.DeleteManagedFileResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, err := uuid.Parse(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := s.cfgStore.DeleteManagedFile(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyManagedFileDeleted, ID: id})

	return connect.NewResponse(&apiv1.DeleteManagedFileResponse{}), nil
}

func (s *ConfigServer) loadConfigManagedFiles(ctx context.Context, resp *apiv1.GetConfigResponse) error {
	files, err := s.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return fmt.Errorf("list managed files: %w", err)
	}
	for _, f := range files {
		resp.ManagedFiles = append(resp.ManagedFiles, &apiv1.ManagedFileInfo{
			Id:         f.ID.String(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format(time.RFC3339),
		})
	}
	return nil
}
