package server

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// JobScheduler is the subset of orchestrator.Scheduler used by JobServer.
type JobScheduler interface {
	GetJob(id string) (orchestrator.JobInfo, bool)
	ListJobs() []orchestrator.JobInfo
}

// PeerJobsProvider returns active jobs from peer cluster nodes plus a
// signal that fires whenever the underlying peer data changes.
type PeerJobsProvider interface {
	GetAll() map[string][]*apiv1.Job
	Changes() *notify.Signal
}

// JobEventSubscriber exposes per-transition job events from the local
// scheduler. WatchJobs subscribes here instead of polling.
type JobEventSubscriber interface {
	Subscribe() (*orchestrator.JobSubscription, func())
}

// JobServer implements the JobService.
type JobServer struct {
	scheduler   JobScheduler
	localNodeID string
	peerJobs    PeerJobsProvider   // nil in single-node mode
	events      JobEventSubscriber // nil only in tests that don't exercise WatchJobs
}

var _ gastrologv1connect.JobServiceHandler = (*JobServer)(nil)

// NewJobServer creates a new JobServer. events may be nil in tests that
// don't exercise WatchJobs; WatchJobs returns an error if so.
func NewJobServer(scheduler JobScheduler, localNodeID string, peerJobs PeerJobsProvider, events JobEventSubscriber) *JobServer {
	return &JobServer{scheduler: scheduler, localNodeID: localNodeID, peerJobs: peerJobs, events: events}
}

// GetJob returns a single job by ID, checking the local scheduler first
// and falling back to peer broadcasts for jobs on remote nodes.
func (s *JobServer) GetJob(
	ctx context.Context,
	req *connect.Request[apiv1.GetJobRequest],
) (*connect.Response[apiv1.GetJobResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	// Check local scheduler first.
	info, ok := s.scheduler.GetJob(string(req.Msg.Id))
	if ok {
		return connect.NewResponse(&apiv1.GetJobResponse{
			Job: JobInfoToProto(info.Snapshot(), s.localNodeID),
		}), nil
	}

	// Fall back to peer jobs from cluster broadcast.
	if s.peerJobs != nil {
		for _, peerJobList := range s.peerJobs.GetAll() {
			for _, job := range peerJobList {
				if string(job.Id) == string(req.Msg.Id) {
					return connect.NewResponse(&apiv1.GetJobResponse{Job: job}), nil
				}
			}
		}
	}

	return nil, connect.NewError(connect.CodeNotFound, errors.New("job not found"))
}

// ListJobs returns all jobs (local + peer) including cron, one-time, and recently completed.
func (s *JobServer) ListJobs(
	ctx context.Context,
	req *connect.Request[apiv1.ListJobsRequest],
) (*connect.Response[apiv1.ListJobsResponse], error) {
	return connect.NewResponse(&apiv1.ListJobsResponse{Jobs: s.allJobs()}), nil
}

// WatchJobs streams the full job list (local + peer) on every state
// transition. Event-driven: subscribes to the local scheduler's job-event
// broker and to a change signal from the peer-jobs cache. No polling.
func (s *JobServer) WatchJobs(
	ctx context.Context,
	req *connect.Request[apiv1.WatchJobsRequest],
	stream *connect.ServerStream[apiv1.WatchJobsResponse],
) error {
	return s.watchJobsLoop(ctx, stream.Send)
}

// watchJobsLoop is the testable core of WatchJobs — parameterized on the
// send function so tests can capture emissions without HTTP plumbing.
func (s *JobServer) watchJobsLoop(ctx context.Context, send func(*apiv1.WatchJobsResponse) error) error {
	if s.events == nil {
		return connect.NewError(connect.CodeUnimplemented, errors.New("WatchJobs: no event source wired"))
	}

	// Initial snapshot so the client gets the current state immediately.
	if err := send(&apiv1.WatchJobsResponse{Jobs: s.allJobs()}); err != nil {
		return err
	}

	sub, cancel := s.events.Subscribe()
	defer cancel()

	var peerCh <-chan struct{} // nil == no peers to watch; read-from-nil blocks forever
	if s.peerJobs != nil {
		peerCh = s.peerJobs.Changes().C()
	}

	for {
		// Wait for a change — any event or peer update.
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-sub.Events():
			if !ok {
				return nil // broker closed
			}
		case <-peerCh:
			// Peer data changed — refresh the change-channel reference
			// for the next wakeup (close-and-recreate pattern).
			if s.peerJobs != nil {
				peerCh = s.peerJobs.Changes().C()
			}
		}

		// Drain additional wakeups without blocking so a burst of events
		// coalesces into a single send.
		coalescing := true
		for coalescing {
			select {
			case _, ok := <-sub.Events():
				if !ok {
					return nil
				}
			case <-peerCh:
				if s.peerJobs != nil {
					peerCh = s.peerJobs.Changes().C()
				}
			default:
				coalescing = false
			}
		}

		if err := send(&apiv1.WatchJobsResponse{Jobs: s.allJobs()}); err != nil {
			return err
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
		return cmp.Compare(string(a.NodeId), string(b.NodeId))
	})

	return all
}

// JobInfoToProto converts an orchestrator.JobInfo to a proto Job message.
func JobInfoToProto(info orchestrator.JobInfo, nodeID string) *apiv1.Job {
	pj := &apiv1.Job{
		Id:          []byte(info.ID),
		Name:        info.Name,
		Description: info.Description,
		Schedule:    info.Schedule,
		NodeId:      []byte(nodeID),
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
