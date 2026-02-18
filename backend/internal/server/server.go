// Package server provides the Connect RPC server for GastroLog.
package server

import (
	"cmp"
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/config"
	"gastrolog/internal/frontend"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Config holds server configuration.
type Config struct {
	// Logger for structured logging.
	Logger *slog.Logger

	// CertManager provides TLS certificates. When non-nil and a server cert is configured,
	// the server can serve HTTPS (see gastrolog-q232).
	CertManager CertManager
}

// CertManager interface for TLS certificate management.
type CertManager interface {
	Certificate(name string) *tls.Certificate
	GetCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error)
	TLSConfig() *tls.Config
	LoadFromConfig(defaultCert string, certs map[string]cert.CertSource) error
}

// Server is the Connect RPC server for GastroLog.
// HTTP is always on; HTTPS is added when TLS enabled and default cert exists.
type Server struct {
	orch        *orchestrator.Orchestrator
	cfgStore    config.Store
	factories   orchestrator.Factories
	tokens      *auth.TokenService
	certManager CertManager
	logger      *slog.Logger

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	handler  http.Handler // core handler (mux + CORS + tracking), shared by HTTP and HTTPS
	shutdown chan struct{}
	inFlight sync.WaitGroup // tracks in-flight requests for graceful drain
	draining atomic.Bool    // true when server is draining (rejecting new requests)

	// Dynamic TLS: HTTPS listener when enabled
	httpsListener net.Listener
	httpsServer   *http.Server
	httpsPort     string
	redirectToHTTPS atomic.Bool
}

// New creates a new Server.
func New(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, tokens *auth.TokenService, cfg Config) *Server {
	return &Server{
		orch:        orch,
		cfgStore:    cfgStore,
		factories:   factories,
		tokens:      tokens,
		certManager: cfg.CertManager,
		logger:      logging.Default(cfg.Logger).With("component", "server"),
		shutdown:    make(chan struct{}),
	}
}

// registerProbes adds Kubernetes liveness and readiness probe endpoints.
func (s *Server) registerProbes(mux *http.ServeMux) {
	// Liveness probe - returns 200 if the process is alive
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness probe - returns 200 if ready to accept traffic
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.orch.IsRunning() && !s.draining.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
}

// isLoopback returns true if host is a loopback address (localhost, 127.0.0.1, ::1).
func isLoopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// corsMiddleware adds CORS headers for browser clients.
// Only allows same-origin requests; never reflects arbitrary Origin to avoid
// cross-origin theft of sensitive data (private keys, JWT secret).
// For loopback (dev with proxy), allows Origin from same hostname on any port.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			sameOrigin := scheme + "://" + r.Host
			allowed := origin == sameOrigin
			if !allowed {
				// Dev with proxy: frontend (e.g. localhost:3000) proxies to backend (localhost:4564).
				// Allow any loopback origin when request is to loopback.
				reqHost, _, _ := net.SplitHostPort(r.Host)
				reqHost = cmp.Or(reqHost, r.Host)
				if isLoopback(reqHost) {
					if u, err := url.Parse(origin); err == nil {
						oHost, _, _ := net.SplitHostPort(u.Host)
						if oHost == "" {
							oHost = u.Host
						}
						allowed = isLoopback(oHost)
					}
				}
			}
			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, Grpc-Timeout, X-Grpc-Web, X-User-Agent")
				w.Header().Set("Access-Control-Expose-Headers", "Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}
		}

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
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

// buildMux creates a new ServeMux with all RPC service handlers and probe endpoints registered.
func (s *Server) buildMux() *http.ServeMux {
	mux := http.NewServeMux()

	var handlerOpts []connect.HandlerOption
	if s.tokens != nil {
		authInterceptor := auth.NewAuthInterceptor(s.tokens, s.cfgStore)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(authInterceptor))
	}

	queryTimeout := 30 * time.Second // default
	if s.cfgStore != nil {
		if sc, err := config.LoadServerConfig(context.Background(), s.cfgStore); err == nil && sc.Query.Timeout != "" {
			if d, err := time.ParseDuration(sc.Query.Timeout); err == nil {
				queryTimeout = d
			}
		}
	}
	queryServer := NewQueryServer(s.orch, queryTimeout)
	storeServer := NewStoreServer(s.orch, s.cfgStore, s.factories, s.logger)
	configServer := NewConfigServer(s.orch, s.cfgStore, s.factories, s.certManager)
	configServer.SetOnTLSConfigChange(s.reconfigureTLS)
	lifecycleServer := NewLifecycleServer(s.orch, s.initiateShutdown)
	authServer := NewAuthServer(s.cfgStore, s.tokens)
	jobServer := NewJobServer(s.orch.Scheduler())

	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewStoreServiceHandler(storeServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewConfigServiceHandler(configServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(authServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewJobServiceHandler(jobServer, handlerOpts...))

	s.registerProbes(mux)

	if h := frontend.Handler(); h != nil {
		mux.Handle("/", h)
	}

	return mux
}

// Serve starts the server on the given listener.
// HTTP is always on; HTTPS is started when TLS enabled and default cert exists.
// It blocks until the server is stopped or an error occurs.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	// Build the core handler once — reused by both HTTP and HTTPS.
	mux := s.buildMux()
	s.handler = s.trackingMiddleware(s.corsMiddleware(compressMiddleware(mux)))

	// HTTP adds redirect-to-HTTPS + h2c (HTTP/2 without TLS).
	redirectHandler := s.redirectMiddleware(s.handler)
	s.server = &http.Server{
		Handler: h2c.NewHandler(redirectHandler, &http2.Server{}),
	}

	// Initial TLS config: start HTTPS if enabled
	s.reconfigureTLS()

	s.logger.Info("server starting", "addr", listener.Addr().String())

	err := s.server.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// redirectMiddleware redirects HTTP requests to HTTPS when both listeners are active.
// Skips redirect for localhost/127.0.0.1 so dev proxies (e.g. Vite) can keep using HTTP.
func (s *Server) redirectMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.redirectToHTTPS.Load() {
			next.ServeHTTP(w, r)
			return
		}
		port := s.httpsPort
		if port == "" {
			next.ServeHTTP(w, r)
			return
		}
		host, _, _ := net.SplitHostPort(r.Host)
		if host == "" {
			host = r.Host
		}
		if isLoopback(host) {
			next.ServeHTTP(w, r)
			return
		}
		httpsURL := "https://" + host + ":" + port + r.URL.RequestURI()
		http.Redirect(w, r, httpsURL, http.StatusTemporaryRedirect)
	})
}

// reconfigureTLS starts/stops HTTPS listener based on config. Safe to call from any goroutine.
func (s *Server) reconfigureTLS() {
	ctx := context.Background()
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		s.logger.Warn("reconfigure TLS: load config failed", "error", err)
		return
	}
	// Fall back to HTTP if no default cert or TLS disabled
	tlsEnabled := sc.TLS.TLSEnabled && sc.TLS.DefaultCert != ""
	redirectEnabled := sc.TLS.HTTPToHTTPSRedirect && tlsEnabled

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update redirect state
	s.redirectToHTTPS.Store(redirectEnabled)

	if !tlsEnabled {
		s.stopHTTPSLocked()
		return
	}

	if s.certManager == nil {
		s.stopHTTPSLocked()
		return
	}

	// HTTPS port: derive from HTTP listener port + 1
	httpsPort := s.deriveHTTPSPort()
	if httpsPort == "" {
		s.logger.Warn("reconfigure TLS: cannot derive HTTPS port")
		return
	}
	s.httpsPort = httpsPort

	// Already running?
	if s.httpsListener != nil {
		return
	}

	// Start HTTPS listener
	httpsAddr := ":" + httpsPort
	ln, err := net.Listen("tcp", httpsAddr)
	if err != nil {
		s.logger.Warn("reconfigure TLS: listen failed", "addr", httpsAddr, "error", err)
		return
	}
	tlsConfig := s.certManager.TLSConfig()
	tlsLn := tls.NewListener(ln, tlsConfig)

	s.httpsListener = tlsLn
	s.httpsServer = &http.Server{
		Handler: s.handler,
	}
	s.logger.Info("HTTPS listener started", "addr", httpsAddr)

	go func() {
		if err := s.httpsServer.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
			s.logger.Warn("HTTPS serve error", "error", err)
		}
	}()
}

func (s *Server) deriveHTTPSPort() string {
	if s.listener == nil {
		return ""
	}
	addr := s.listener.Addr().String()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return ""
	}
	return strconv.Itoa(port + 1)
}

func (s *Server) stopHTTPSLocked() {
	if s.httpsServer != nil {
		_ = s.httpsServer.Shutdown(context.Background())
		s.httpsServer = nil
	}
	if s.httpsListener != nil {
		_ = s.httpsListener.Close()
		s.httpsListener = nil
	}
	s.httpsPort = ""
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
	httpsServer := s.httpsServer
	s.httpsServer = nil
	s.httpsListener = nil
	s.mu.Unlock()

	if httpsServer != nil {
		_ = httpsServer.Shutdown(ctx)
	}

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
	mux := s.buildMux()
	handler := h2c.NewHandler(mux, &http2.Server{})
	return s.trackingMiddleware(handler)
}

// Client creates a set of Connect clients for the given base URL.
type Client struct {
	Query     gastrologv1connect.QueryServiceClient
	Store     gastrologv1connect.StoreServiceClient
	Config    gastrologv1connect.ConfigServiceClient
	Lifecycle gastrologv1connect.LifecycleServiceClient
	Auth      gastrologv1connect.AuthServiceClient
	Job       gastrologv1connect.JobServiceClient
}

// NewClient creates Connect clients for the given base URL.
func NewClient(baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL, opts...),
		Store:     gastrologv1connect.NewStoreServiceClient(http.DefaultClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(http.DefaultClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(http.DefaultClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(http.DefaultClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(http.DefaultClient, baseURL, opts...),
	}
}

// NewClientWithHTTP creates Connect clients with a custom HTTP client.
func NewClientWithHTTP(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(httpClient, baseURL, opts...),
		Store:     gastrologv1connect.NewStoreServiceClient(httpClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(httpClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(httpClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(httpClient, baseURL, opts...),
	}
}
