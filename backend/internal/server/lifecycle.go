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
	shutdown  func(drain bool)
}

var _ gastrologv1connect.LifecycleServiceHandler = (*LifecycleServer)(nil)

// NewLifecycleServer creates a new LifecycleServer.
// The shutdown function is called when Shutdown is invoked with the drain flag.
func NewLifecycleServer(orch *orchestrator.Orchestrator, shutdown func(drain bool)) *LifecycleServer {
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
// If drain is true in the request, waits for in-flight requests to complete.
func (s *LifecycleServer) Shutdown(
	ctx context.Context,
	req *connect.Request[apiv1.ShutdownRequest],
) (*connect.Response[apiv1.ShutdownResponse], error) {
	if s.shutdown != nil {
		drain := req.Msg.Drain
		// Run shutdown in background so we can return the response
		go s.shutdown(drain)
	}
	return connect.NewResponse(&apiv1.ShutdownResponse{}), nil
}
