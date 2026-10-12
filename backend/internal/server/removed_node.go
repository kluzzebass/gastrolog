package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// RemovedNode is a node the cluster removed, as the API it keeps serving
// describes it.
type RemovedNode struct {
	NodeID    string
	RemovedAt time.Time
	Reason    string
}

// RemovedNodeRemedy is the documented way back for a removed machine.
const RemovedNodeRemedy = "To bring this machine back, start it from an empty home directory: it joins the cluster as a new node."

// Explanation is the operator-facing account of the removal, remedy included.
func (n RemovedNode) Explanation() string {
	return fmt.Sprintf("node %s was removed from the cluster at %s (%s) and takes no part in it. %s",
		n.NodeID, n.RemovedAt.UTC().Format(time.RFC3339), n.Reason, RemovedNodeRemedy)
}

// RemovedNodeHandler serves the API of a removed node. It answers cluster
// status, health and leadership questions from what the node knows about its
// removal, and refuses every other call with that account. It reads no
// cluster state: a removed node opens neither its Raft store nor its vaults.
func RemovedNodeHandler(n RemovedNode, version string) http.Handler {
	started := time.Now()
	mux := http.NewServeMux()

	mux.Handle(gastrologv1connect.LifecycleServiceGetClusterStatusProcedure, connect.NewUnaryHandler(
		gastrologv1connect.LifecycleServiceGetClusterStatusProcedure,
		func(context.Context, *connect.Request[apiv1.GetClusterStatusRequest]) (*connect.Response[apiv1.GetClusterStatusResponse], error) {
			return connect.NewResponse(&apiv1.GetClusterStatusResponse{
				ClusterEnabled: true,
				LocalNodeId:    []byte(n.NodeID),
				Removal: &apiv1.NodeRemoval{
					RemovedAt: timestamppb.New(n.RemovedAt),
					Reason:    n.Reason,
				},
			}), nil
		}))
	mux.Handle(gastrologv1connect.LifecycleServiceHealthProcedure, connect.NewUnaryHandler(
		gastrologv1connect.LifecycleServiceHealthProcedure,
		func(context.Context, *connect.Request[apiv1.HealthRequest]) (*connect.Response[apiv1.HealthResponse], error) {
			return connect.NewResponse(&apiv1.HealthResponse{
				Status:        apiv1.Status_STATUS_UNHEALTHY,
				Version:       version,
				UptimeSeconds: int64(time.Since(started).Seconds()),
			}), nil
		}))
	// A removed node holds no leadership, so a preStop hook asking it to
	// yield is answered, not failed.
	mux.Handle(gastrologv1connect.LifecycleServiceYieldLeadershipProcedure, connect.NewUnaryHandler(
		gastrologv1connect.LifecycleServiceYieldLeadershipProcedure,
		func(context.Context, *connect.Request[apiv1.YieldLeadershipRequest]) (*connect.Response[apiv1.YieldLeadershipResponse], error) {
			return connect.NewResponse(&apiv1.YieldLeadershipResponse{Transferred: false}), nil
		}))

	// The process is alive and must stay up, so the liveness probe passes;
	// it serves nothing, so the readiness probe never does.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	explain := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, n.Explanation()+"\n")
	}
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { explain(w) })

	refusal := connect.NewError(connect.CodeFailedPrecondition, errors.New(n.Explanation()))
	errWriter := connect.NewErrorWriter()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && errWriter.IsSupported(r) {
			_ = errWriter.Write(w, r, refusal)
			return
		}
		explain(w)
	})

	return securityHeadersMiddleware(mux)
}

// removedNodeDrain bounds how long shutdown waits for in-flight requests.
// Every answer a removed node gives is computed in memory, so anything
// still running after this is a client that stopped reading.
const removedNodeDrain = time.Second

// ServeRemovedNode serves RemovedNodeHandler on the node's API address, when
// it has one, and on its Unix socket until ctx ends.
func ServeRemovedNode(ctx context.Context, n RemovedNode, version, addr, socketPath string, logger *slog.Logger) error {
	handler := RemovedNodeHandler(n, version)
	var servers []*http.Server
	var wg sync.WaitGroup
	serve := func(ln net.Listener) {
		srv := newTimedServer(handler, readHeaderTimeout, readTimeout, idleTimeout)
		servers = append(servers, srv)
		wg.Go(func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("removed node: server error", "addr", ln.Addr().String(), "error", err)
			}
		})
	}

	if addr != "" {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		serve(ln)
	}
	if socketPath != "" {
		ln, err := listenUnixSocket(socketPath)
		if err != nil {
			logger.Warn("removed node: unix socket unavailable", "error", err)
		} else {
			defer func() { _ = os.Remove(socketPath) }()
			serve(ln)
		}
	}

	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), removedNodeDrain)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(stopCtx)
	}
	wg.Wait()
	return nil
}
