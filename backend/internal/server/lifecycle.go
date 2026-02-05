package server

import (
	"context"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
)

// Version is set at build time.
var Version = "dev"

// LifecycleServer implements the LifecycleService.
type LifecycleServer struct {
	orch      *orchestrator.Orchestrator
	startTime time.Time
	shutdown  func()
}

var _ gastrologv1connect.LifecycleServiceHandler = (*LifecycleServer)(nil)

// NewLifecycleServer creates a new LifecycleServer.
// The shutdown function is called when Shutdown is invoked.
func NewLifecycleServer(orch *orchestrator.Orchestrator, shutdown func()) *LifecycleServer {
	return &LifecycleServer{
		orch:      orch,
		startTime: time.Now(),
		shutdown:  shutdown,
	}
}

// Health returns the server health status.
func (s *LifecycleServer) Health(
	ctx context.Context,
	req *connect.Request[apiv1.HealthRequest],
) (*connect.Response[apiv1.HealthResponse], error) {
	status := apiv1.Status_STATUS_HEALTHY
	if !s.orch.IsRunning() {
		status = apiv1.Status_STATUS_UNHEALTHY
	}

	return connect.NewResponse(&apiv1.HealthResponse{
		Status:        status,
		Version:       Version,
		UptimeSeconds: int64(time.Since(s.startTime).Seconds()),
	}), nil
}

// Shutdown initiates a graceful shutdown.
func (s *LifecycleServer) Shutdown(
	ctx context.Context,
	req *connect.Request[apiv1.ShutdownRequest],
) (*connect.Response[apiv1.ShutdownResponse], error) {
	if s.shutdown != nil {
		// Run shutdown in background so we can return the response
		go s.shutdown()
	}
	return connect.NewResponse(&apiv1.ShutdownResponse{}), nil
}
