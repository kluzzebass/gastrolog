// Package server provides the Connect RPC server for GastroLog.
package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Config holds server configuration.
type Config struct {
	// Logger for structured logging.
	Logger *slog.Logger
}

// Server is the Connect RPC server for GastroLog.
type Server struct {
	orch   *orchestrator.Orchestrator
	logger *slog.Logger

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	shutdown chan struct{}
	inFlight sync.WaitGroup // tracks in-flight requests for graceful drain
	draining atomic.Bool    // true when server is draining (rejecting new requests)
}

// New creates a new Server.
func New(orch *orchestrator.Orchestrator, cfg Config) *Server {
	return &Server{
		orch:     orch,
		logger:   logging.Default(cfg.Logger).With("component", "server"),
		shutdown: make(chan struct{}),
	}
}

// trackingMiddleware wraps an http.Handler to track in-flight requests.
func (s *Server) trackingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			http.Error(w, "server is draining", http.StatusServiceUnavailable)
			return
		}
		s.inFlight.Add(1)
		defer s.inFlight.Done()
		next.ServeHTTP(w, r)
	})
}

// Serve starts the server on the given listener.
// It blocks until the server is stopped or an error occurs.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	mux := http.NewServeMux()

	// Create service handlers
	queryServer := NewQueryServer(s.orch)
	storeServer := NewStoreServer(s.orch)
	configServer := NewConfigServer(s.orch)
	lifecycleServer := NewLifecycleServer(s.orch, s.initiateShutdown)

	// Register handlers
	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer))
	mux.Handle(gastrologv1connect.NewStoreServiceHandler(storeServer))
	mux.Handle(gastrologv1connect.NewConfigServiceHandler(configServer))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer))

	// Use h2c for HTTP/2 without TLS (for Unix sockets and local connections)
	handler := h2c.NewHandler(mux, &http2.Server{})

	// Wrap with tracking middleware for graceful drain
	s.server = &http.Server{Handler: s.trackingMiddleware(handler)}

	s.logger.Info("server starting", "addr", listener.Addr().String())

	err := s.server.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// ServeUnix starts the server on a Unix socket.
func (s *Server) ServeUnix(path string) error {
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// ServeTCP starts the server on a TCP address.
func (s *Server) ServeTCP(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// Stop gracefully stops the server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()

	if server == nil {
		return nil
	}

	s.logger.Info("server stopping")
	return server.Shutdown(ctx)
}

// initiateShutdown is called by the LifecycleServer to trigger shutdown.
// If drain is true, it waits for in-flight requests to complete before signaling.
func (s *Server) initiateShutdown(drain bool) {
	s.mu.Lock()
	alreadyShuttingDown := false
	select {
	case <-s.shutdown:
		alreadyShuttingDown = true
	default:
	}
	s.mu.Unlock()

	if alreadyShuttingDown {
		return
	}

	if drain {
		s.logger.Info("draining in-flight requests")
		s.draining.Store(true)
		s.inFlight.Wait()
		s.logger.Info("drain complete")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.shutdown:
		// Already closed by another goroutine
	default:
		close(s.shutdown)
	}
}

// ShutdownChan returns a channel that is closed when shutdown is initiated.
func (s *Server) ShutdownChan() <-chan struct{} {
	return s.shutdown
}

// Handler returns an http.Handler for the server.
// This is useful for testing or embedding in another server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	queryServer := NewQueryServer(s.orch)
	storeServer := NewStoreServer(s.orch)
	configServer := NewConfigServer(s.orch)
	lifecycleServer := NewLifecycleServer(s.orch, s.initiateShutdown)

	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer))
	mux.Handle(gastrologv1connect.NewStoreServiceHandler(storeServer))
	mux.Handle(gastrologv1connect.NewConfigServiceHandler(configServer))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer))

	handler := h2c.NewHandler(mux, &http2.Server{})
	return s.trackingMiddleware(handler)
}

// Client creates a set of Connect clients for the given base URL.
type Client struct {
	Query     gastrologv1connect.QueryServiceClient
	Store     gastrologv1connect.StoreServiceClient
	Config    gastrologv1connect.ConfigServiceClient
	Lifecycle gastrologv1connect.LifecycleServiceClient
}

// NewClient creates Connect clients for the given base URL.
func NewClient(baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL, opts...),
		Store:     gastrologv1connect.NewStoreServiceClient(http.DefaultClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(http.DefaultClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(http.DefaultClient, baseURL, opts...),
	}
}

// NewClientWithHTTP creates Connect clients with a custom HTTP client.
func NewClientWithHTTP(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(httpClient, baseURL, opts...),
		Store:     gastrologv1connect.NewStoreServiceClient(httpClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(httpClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL, opts...),
	}
}
