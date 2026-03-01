package server

import (
	"cmp"
	"context"
	"errors"
	"hash/fnv"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
)

// PeerJobsProvider returns active jobs from peer cluster nodes.
type PeerJobsProvider interface {
	GetAll() map[string][]*apiv1.Job
}

// JobServer implements the JobService.
type JobServer struct {
	scheduler   *orchestrator.Scheduler
	localNodeID string
	peerJobs    PeerJobsProvider // nil in single-node mode
}

var _ gastrologv1connect.JobServiceHandler = (*JobServer)(nil)

// NewJobServer creates a new JobServer.
func NewJobServer(scheduler *orchestrator.Scheduler, localNodeID string, peerJobs PeerJobsProvider) *JobServer {
	return &JobServer{scheduler: scheduler, localNodeID: localNodeID, peerJobs: peerJobs}
}

// GetJob returns a single job by ID (local node only).
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
		Job: JobInfoToProto(info.Snapshot(), s.localNodeID),
	}), nil
}

// ListJobs returns all jobs (local + peer) including cron, one-time, and recently completed.
func (s *JobServer) ListJobs(
	ctx context.Context,
	req *connect.Request[apiv1.ListJobsRequest],
) (*connect.Response[apiv1.ListJobsResponse], error) {
	return connect.NewResponse(&apiv1.ListJobsResponse{Jobs: s.allJobs()}), nil
}

// WatchJobs streams the full job list (local + peer) whenever state changes.
func (s *JobServer) WatchJobs(
	ctx context.Context,
	req *connect.Request[apiv1.WatchJobsRequest],
	stream *connect.ServerStream[apiv1.WatchJobsResponse],
) error {
	var lastHash uint64

	for {
		resp := &apiv1.WatchJobsResponse{Jobs: s.allJobs()}

		h := fnv.New64a()
		data, _ := proto.Marshal(resp)
		_, _ = h.Write(data)
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

// allJobs merges local jobs with peer jobs from the cluster broadcast.
// Results are sorted: scheduled jobs first, then tasks. Within each group,
// sorted by description (falling back to name) then by node_id.
func (s *JobServer) allJobs() []*apiv1.Job {
	localJobs := s.scheduler.ListJobs()
	all := make([]*apiv1.Job, 0, len(localJobs))
	for _, info := range localJobs {
		all = append(all, JobInfoToProto(info.Snapshot(), s.localNodeID))
	}
	if s.peerJobs != nil {
		for _, peerJobList := range s.peerJobs.GetAll() {
			all = append(all, peerJobList...)
		}
	}

	slices.SortFunc(all, func(a, b *apiv1.Job) int {
		// Scheduled before tasks.
		if a.Kind != b.Kind {
			if a.Kind == apiv1.JobKind_JOB_KIND_SCHEDULED {
				return -1
			}
			if b.Kind == apiv1.JobKind_JOB_KIND_SCHEDULED {
				return 1
			}
		}
		// By description (or name as fallback).
		aDesc := a.Description
		if aDesc == "" {
			aDesc = a.Name
		}
		bDesc := b.Description
		if bDesc == "" {
			bDesc = b.Name
		}
		if c := cmp.Compare(aDesc, bDesc); c != 0 {
			return c
		}
		// By node ID to group same-node jobs together.
		return cmp.Compare(a.NodeId, b.NodeId)
	})

	return all
}

// JobInfoToProto converts an orchestrator.JobInfo to a proto Job message.
func JobInfoToProto(info orchestrator.JobInfo, nodeID string) *apiv1.Job {
	pj := &apiv1.Job{
		Id:          info.ID,
		Name:        info.Name,
		Description: info.Description,
		Schedule:    info.Schedule,
		NodeId:      nodeID,
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
