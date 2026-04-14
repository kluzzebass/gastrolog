package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system/raftfsm"
	"time"
)

// ListManagedFiles returns all uploaded managed files.
func (s *SystemServer) ListManagedFiles(
	ctx context.Context,
	_ *connect.Request[apiv1.ListManagedFilesRequest],
) (*connect.Response[apiv1.ListManagedFilesResponse], error) {
	files, err := s.sysStore.ListManagedFiles(ctx)
	if err != nil {
		return nil, errInternal(err)
	}

	out := make([]*apiv1.ManagedFileInfo, len(files))
	for i, f := range files {
		out[i] = &apiv1.ManagedFileInfo{
			Id:         f.ID.ToProto(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	return connect.NewResponse(&apiv1.ListManagedFilesResponse{Files: out}), nil
}

// DeleteManagedFile removes an uploaded managed file.
func (s *SystemServer) DeleteManagedFile(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteManagedFileRequest],
) (*connect.Response[apiv1.DeleteManagedFileResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	if err := s.sysStore.DeleteManagedFile(ctx, id); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyManagedFileDeleted, ID: id})

	return connect.NewResponse(&apiv1.DeleteManagedFileResponse{}), nil
}

func (s *SystemServer) loadConfigManagedFiles(ctx context.Context, resp *apiv1.GetSystemResponse) error {
	files, err := s.sysStore.ListManagedFiles(ctx)
	if err != nil {
		return fmt.Errorf("list managed files: %w", err)
	}
	for _, f := range files {
		resp.ManagedFiles = append(resp.ManagedFiles, &apiv1.ManagedFileInfo{
			Id:         f.ID.ToProto(),
			Name:       f.Name,
			Sha256:     f.SHA256,
			Size:       f.Size,
			UploadedAt: f.UploadedAt.Format(time.RFC3339),
		})
	}
	return nil
}
