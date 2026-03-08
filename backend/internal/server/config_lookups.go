package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config/raftfsm"
	"time"
)

// ListLookupFiles returns all uploaded lookup files.
func (s *ConfigServer) ListLookupFiles(
	ctx context.Context,
	_ *connect.Request[apiv1.ListLookupFilesRequest],
) (*connect.Response[apiv1.ListLookupFilesResponse], error) {
	files, err := s.cfgStore.ListLookupFiles(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	out := make([]*apiv1.LookupFileInfo, len(files))
	for i, f := range files {
		out[i] = &apiv1.LookupFileInfo{
			Id:         f.ID.String(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	return connect.NewResponse(&apiv1.ListLookupFilesResponse{Files: out}), nil
}

// DeleteLookupFile removes an uploaded lookup file.
func (s *ConfigServer) DeleteLookupFile(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteLookupFileRequest],
) (*connect.Response[apiv1.DeleteLookupFileResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, err := uuid.Parse(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := s.cfgStore.DeleteLookupFile(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyLookupFileDeleted, ID: id})

	return connect.NewResponse(&apiv1.DeleteLookupFileResponse{}), nil
}

func (s *ConfigServer) loadConfigLookupFiles(ctx context.Context, resp *apiv1.GetConfigResponse) {
	files, err := s.cfgStore.ListLookupFiles(ctx)
	if err != nil {
		return
	}
	for _, f := range files {
		resp.LookupFiles = append(resp.LookupFiles, &apiv1.LookupFileInfo{
			Id:         f.ID.String(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format(time.RFC3339),
		})
	}
}
