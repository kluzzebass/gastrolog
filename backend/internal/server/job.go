package server

import (
	"context"
	"errors"
	"hash/fnv"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
)

// JobServer implements the JobService.
type JobServer struct {
	scheduler *orchestrator.Scheduler
}

var _ gastrologv1connect.JobServiceHandler = (*JobServer)(nil)

// NewJobServer creates a new JobServer.
func NewJobServer(scheduler *orchestrator.Scheduler) *JobServer {
	return &JobServer{scheduler: scheduler}
}

// GetJob returns a single job by ID.
func (s *JobServer) GetJob(
	ctx context.Context,
	req *connect.Request[apiv1.GetJobRequest],
) (*connect.Response[apiv1.GetJobResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	info, ok := s.scheduler.GetJob(req.Msg.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("job not found"))
	}

	return connect.NewResponse(&apiv1.GetJobResponse{
		Job: jobInfoToProto(info.Snapshot()),
	}), nil
}

// ListJobs returns all jobs (cron, one-time, and recently completed).
func (s *JobServer) ListJobs(
	ctx context.Context,
	req *connect.Request[apiv1.ListJobsRequest],
) (*connect.Response[apiv1.ListJobsResponse], error) {
	jobs := s.scheduler.ListJobs()
	protoJobs := make([]*apiv1.Job, 0, len(jobs))
	for _, info := range jobs {
		protoJobs = append(protoJobs, jobInfoToProto(info.Snapshot()))
	}
	return connect.NewResponse(&apiv1.ListJobsResponse{Jobs: protoJobs}), nil
}

// WatchJobs streams the full job list whenever state changes.
func (s *JobServer) WatchJobs(
	ctx context.Context,
	req *connect.Request[apiv1.WatchJobsRequest],
	stream *connect.ServerStream[apiv1.WatchJobsResponse],
) error {
	var lastHash uint64

	for {
		jobs := s.scheduler.ListJobs()
		resp := &apiv1.WatchJobsResponse{
			Jobs: make([]*apiv1.Job, 0, len(jobs)),
		}
		for _, info := range jobs {
			resp.Jobs = append(resp.Jobs, jobInfoToProto(info.Snapshot()))
		}

		h := fnv.New64a()
		data, _ := proto.Marshal(resp)
		h.Write(data)
		hash := h.Sum64()

		if hash != lastHash {
			if err := stream.Send(resp); err != nil {
				return err
			}
			lastHash = hash
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func jobInfoToProto(info orchestrator.JobInfo) *apiv1.Job {
	pj := &apiv1.Job{
		Id:          info.ID,
		Name:        info.Name,
		Description: info.Description,
		Schedule:    info.Schedule,
	}

	if info.Schedule == "once" {
		pj.Kind = apiv1.JobKind_JOB_KIND_TASK
	} else {
		pj.Kind = apiv1.JobKind_JOB_KIND_SCHEDULED
	}

	if !info.LastRun.IsZero() {
		pj.LastRun = timestamppb.New(info.LastRun)
	}
	if !info.NextRun.IsZero() {
		pj.NextRun = timestamppb.New(info.NextRun)
	}

	if info.Progress != nil {
		pj.ChunksTotal = info.Progress.ChunksTotal
		pj.ChunksDone = info.Progress.ChunksDone
		pj.RecordsDone = info.Progress.RecordsDone
		pj.Error = info.Progress.Error
		pj.ErrorDetails = info.Progress.ErrorDetails
		if !info.Progress.StartedAt.IsZero() {
			pj.StartedAt = timestamppb.New(info.Progress.StartedAt)
		}
		if !info.Progress.CompletedAt.IsZero() {
			pj.CompletedAt = timestamppb.New(info.Progress.CompletedAt)
		}

		switch info.Progress.Status {
		case orchestrator.JobStatusPending:
			pj.Status = apiv1.JobStatus_JOB_STATUS_PENDING
		case orchestrator.JobStatusRunning:
			pj.Status = apiv1.JobStatus_JOB_STATUS_RUNNING
		case orchestrator.JobStatusCompleted:
			pj.Status = apiv1.JobStatus_JOB_STATUS_COMPLETED
		case orchestrator.JobStatusFailed:
			pj.Status = apiv1.JobStatus_JOB_STATUS_FAILED
		}
	}

	return pj
}
