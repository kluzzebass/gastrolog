// Package server provides the Connect RPC server for GastroLog.
package server

import (
	"cmp"
	"context"
	"crypto/tls"
	"gastrolog/internal/glid"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/frontend"
	"gastrolog/internal/logging"
	"gastrolog/internal/lookup"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server/routing"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// systemLoadTimeout bounds how long background config store reads can take.
// Prevents indefinite hangs if the Raft FSM or underlying store is slow.
const systemLoadTimeout = 5 * time.Second

// readHeaderTimeout is the maximum time to read HTTP request headers.
// Shared by HTTP, HTTPS, and Unix socket servers.
const readHeaderTimeout = 10 * time.Second

// Config holds server configuration.
type Config struct {
	// Logger for structured logging.
	Logger *slog.Logger

	// CertManager provides TLS certificates. When non-nil and a server cert is configured,
	// the server can serve HTTPS (see gastrolog-q232).
	CertManager CertManager

	// NoAuth disables authentication. All requests are treated as admin.
	NoAuth bool

	// HomeDir is the gastrolog home directory path. Used for auto-downloaded
	// lookup databases. Empty when running with in-memory system.
	HomeDir string

	// AfterConfigApply is called after the server handler persists a config
	// mutation that requires orchestrator side effects. For raft-backed stores
	// this should be nil (the FSM's onApply callback handles it). For non-raft
	// stores (memory, tests), set this to trigger the same dispatcher.
	AfterConfigApply func(raftfsm.Notification)

	// NodeID is the local raft server ID. Used to auto-assign node ownership
	// when creating vaults and ingesters.
	NodeID string

	// UnixSocket is the path to a Unix domain socket for local CLI access.
	// When set, the server listens on this socket with authentication bypassed
	// (the OS file-system permissions provide access control). Empty disables.
	UnixSocket string

	// Cluster provides Raft topology for the GetClusterStatus RPC.
	// Nil in single-node mode.
	Cluster ClusterStatusProvider

	// PeerStats provides the latest broadcast stats from peer nodes.
	PeerStats NodeStatsProvider

	// PeerVaultStats looks up vault-level stats from cluster peers.
	// Nil in single-node mode. Typically the same *cluster.PeerState as PeerStats.
	PeerVaultStats PeerVaultStatsProvider

	// PeerIngesterStats looks up ingester-level stats from cluster peers.
	// Nil in single-node mode. Typically the same *cluster.PeerState as PeerStats.
	PeerIngesterStats PeerIngesterStatsProvider

	// PeerRouteStats aggregates route stats from cluster peers.
	// Nil in single-node mode. Typically the same *cluster.PeerState as PeerStats.
	PeerRouteStats PeerRouteStatsProvider

	// RemoteSearcher forwards search requests to remote cluster nodes.
	// Nil in single-node mode.
	RemoteSearcher    RemoteSearcher
	RemoteChunkLister RemoteChunkLister

	// PeerJobs provides active jobs from peer cluster nodes.
	// Nil in single-node mode.
	PeerJobs PeerJobsProvider

	// LocalStats returns real-time stats for the local node.
	LocalStats func() *apiv1.NodeStats

	// ConfigSignal broadcasts config changes to WatchConfig streams.
	// May be nil in tests.
	ConfigSignal *notify.Signal

	// StatsSignal broadcasts stats updates to WatchSystemStatus streams.
	// Fired by the stats collector on each broadcast tick. May be nil in tests.
	StatsSignal *notify.Signal

	// ClusterAddress is the cluster gRPC listen address (e.g., ":4566").
	// Exposed in GetClusterStatus for join info. Empty for non-raft mode.
	ClusterAddress string

	// JoinClusterFunc is called by the JoinCluster RPC to join a running
	// single-node server to an existing cluster at runtime. Nil disables.
	JoinClusterFunc func(ctx context.Context, leaderAddr, joinToken string) error

	// RemoveNodeFunc is called by the RemoveNode RPC to evict a node from the
	// cluster. Nil disables.
	RemoveNodeFunc func(ctx context.Context, nodeID string) error

	// SetNodeSuffrageFunc is called by the SetNodeSuffrage RPC to promote or
	// demote a node. Handles leader-forwarding internally. Nil disables.
	SetNodeSuffrageFunc func(ctx context.Context, nodeID string, voter bool) error

	// CloudTesters maps cloud service types to connection test functions.
	CloudTesters map[string]CloudServiceTester

	// RoutingForwarder forwards requests to remote nodes via ForwardRPC.
	// Nil in single-node mode. Satisfies routing.UnaryForwarder.
	RoutingForwarder routing.UnaryForwarder

	// PlacementReconcile runs synchronous placement so RPC responses include
	// tier placements. Nil in single-node or non-cluster mode.
	PlacementReconcile func(ctx context.Context)
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
	orch               *orchestrator.Orchestrator
	cfgStore           system.Store
	factories          orchestrator.Factories
	tokens             *auth.TokenService
	certManager        CertManager
	noAuth             bool
	logger             *slog.Logger
	cluster            ClusterStatusProvider
	peerStats          NodeStatsProvider
	peerVaultStats     PeerVaultStatsProvider
	peerIngesterStats  PeerIngesterStatsProvider
	peerRouteStats     PeerRouteStatsProvider
	remoteSearcher     RemoteSearcher
	remoteChunkLister  RemoteChunkLister
	peerJobs           PeerJobsProvider
	localStatsFn       func() *apiv1.NodeStats
	localNodeID        string
	clusterAddress     string
	joinClusterFn      func(ctx context.Context, leaderAddr, joinToken string) error
	removeNodeFn       func(ctx context.Context, nodeID string) error
	setNodeSuffrageFn  func(ctx context.Context, nodeID string, voter bool) error
	startTime          time.Time
	homeDir            string                     // gastrolog home directory; empty for in-memory config
	afterConfigApply   func(raftfsm.Notification) // non-raft dispatch hook
	configSignal       *notify.Signal             // broadcasts config changes to WatchConfig streams
	statsSignal        *notify.Signal             // broadcasts stats updates to WatchSystemStatus streams
	cloudTesters       map[string]CloudServiceTester
	repairManagedFile  func(fileID string) bool  // on-demand pull from peer; set by app wiring
	queryServer        *QueryServer              // stored for ExportToVault executor wiring
	routingForwarder   routing.UnaryForwarder    // forwards requests to remote nodes; nil in single-node
	placementReconcile func(ctx context.Context) // synchronous placement; nil in non-cluster mode
	mu                 sync.Mutex
	listener           net.Listener
	server             *http.Server
	handler            http.Handler // core handler (mux + CORS + tracking), shared by HTTP and HTTPS
	shutdown           chan struct{}
	inFlight           sync.WaitGroup // tracks in-flight requests for graceful drain
	draining           atomic.Bool    // true when server is draining (rejecting new requests)

	rl       *rateLimiter // per-IP rate limiter for auth endpoints
	rlCancel context.CancelFunc
	rlWG     sync.WaitGroup

	// Dynamic TLS: HTTPS listener when enabled
	httpsListener   net.Listener
	httpsServer     *http.Server
	httpsPort       string
	redirectToHTTPS atomic.Bool

	// Unix socket listener for local CLI access (no auth required)
	unixSocketConfig string // path from Config.UnixSocket, consumed by Serve()
	unixListener     net.Listener
	unixServer       *http.Server
	unixPath         string
}

// New creates a new Server.
func New(orch *orchestrator.Orchestrator, cfgStore system.Store, factories orchestrator.Factories, tokens *auth.TokenService, cfg Config) *Server {
	return &Server{
		orch:               orch,
		cfgStore:           cfgStore,
		factories:          factories,
		tokens:             tokens,
		certManager:        cfg.CertManager,
		noAuth:             cfg.NoAuth,
		logger:             logging.Default(cfg.Logger).With("component", "server"),
		cluster:            cfg.Cluster,
		peerStats:          cfg.PeerStats,
		peerVaultStats:     cfg.PeerVaultStats,
		peerIngesterStats:  cfg.PeerIngesterStats,
		peerRouteStats:     cfg.PeerRouteStats,
		remoteSearcher:     cfg.RemoteSearcher,
		remoteChunkLister:  cfg.RemoteChunkLister,
		peerJobs:           cfg.PeerJobs,
		localStatsFn:       cfg.LocalStats,
		localNodeID:        cfg.NodeID,
		clusterAddress:     cfg.ClusterAddress,
		joinClusterFn:      cfg.JoinClusterFunc,
		removeNodeFn:       cfg.RemoveNodeFunc,
		setNodeSuffrageFn:  cfg.SetNodeSuffrageFunc,
		startTime:          time.Now(),
		homeDir:            cfg.HomeDir,
		unixSocketConfig:   cfg.UnixSocket,
		cloudTesters:       cfg.CloudTesters,
		afterConfigApply:   cfg.AfterConfigApply,
		configSignal:       cfg.ConfigSignal,
		statsSignal:        cfg.StatsSignal,
		routingForwarder:   cfg.RoutingForwarder,
		placementReconcile: cfg.PlacementReconcile,
		shutdown:           make(chan struct{}),
		rl:                 newRateLimiter(5.0/60.0, 5), // 5 req/min per IP, burst of 5
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

// routingInterceptor returns a routing interceptor if a forwarder is
// configured (cluster mode). Returns nil slice in single-node mode.
func (s *Server) routingInterceptor() []connect.Interceptor {
	if s.routingForwarder == nil && s.cfgStore == nil {
		return nil
	}
	registry := routing.NewRegistry(routing.DefaultRoutes())
	resolver := &configVaultOwner{cfgStore: s.cfgStore, localNodeID: s.localNodeID}
	ri := routing.NewRoutingInterceptor(registry, s.localNodeID, resolver, s.routingForwarder)
	return []connect.Interceptor{ri}
}

// configVaultOwner resolves vault ownership from the config store.
type configVaultOwner struct {
	cfgStore    system.Store
	localNodeID string
}

// temporary: uses tier-level NodeID for node assignment until tier election.
func (c *configVaultOwner) ResolveVaultOwner(ctx context.Context, vaultID string) string {
	if c.cfgStore == nil {
		return ""
	}
	id, err := glid.ParseUUID(vaultID)
	if err != nil {
		return ""
	}
	vaultCfg, err := c.cfgStore.GetVault(ctx, id)
	if err != nil || vaultCfg == nil {
		return ""
	}

	tiers, err := c.cfgStore.ListTiers(ctx)
	if err != nil {
		return ""
	}
	nscs, err := c.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return ""
	}

	tierMap := make(map[glid.GLID]*system.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	// temporary: find the tier's leader node to determine the owning node (until tier election).
	for _, tierID := range system.VaultTierIDs(tiers, vaultCfg.ID) {
		tc := tierMap[tierID]
		if tc == nil {
			continue
		}
		placements, _ := c.cfgStore.GetTierPlacements(ctx, tc.ID)
		leaderNodeID := system.LeaderNodeID(placements, nscs)
		if leaderNodeID != "" && leaderNodeID != c.localNodeID {
			return leaderNodeID
		}
	}
	return ""
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
		if origin != "" && isOriginAllowed(origin, r) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, Grpc-Timeout, X-Grpc-Web, X-User-Agent")
			w.Header().Set("Access-Control-Expose-Headers", "Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func isOriginAllowed(origin string, r *http.Request) bool {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if origin == scheme+"://"+r.Host {
		return true
	}
	reqHost, _, _ := net.SplitHostPort(r.Host)
	reqHost = cmp.Or(reqHost, r.Host)
	if !isLoopback(reqHost) {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	oHost, _, _ := net.SplitHostPort(u.Host)
	if oHost == "" {
		oHost = u.Host
	}
	return isLoopback(oHost)
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
func (s *Server) buildMux(overrideOpts ...connect.HandlerOption) *http.ServeMux {
	mux := http.NewServeMux()

	// Cap inbound message size to 4 MB to prevent memory exhaustion.
	handlerOpts := []connect.HandlerOption{
		connect.WithReadMaxBytes(4 << 20),
	}
	switch {
	case len(overrideOpts) > 0:
		// Internal mux (unix socket, ForwardRPC dispatch): caller provides
		// interceptors directly — no routing interceptor to prevent loops.
		handlerOpts = append(handlerOpts, overrideOpts...)
	case s.noAuth:
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger), &auth.NoAuthInterceptor{}}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	case s.tokens != nil:
		authInterceptor := auth.NewAuthInterceptor(s.tokens, s.cfgStore, &tokenValidator{cfgStore: s.cfgStore})
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger), authInterceptor}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	default:
		// No auth configured (tests without NoAuth flag). Still attach the
		// RPC error logger; routing interceptor is appended only in cluster mode.
		ri := s.routingInterceptor()
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger)}
		interceptors = append(interceptors, ri...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	}

	queryTimeout, maxFollowDuration, maxResultCount := s.loadQueryConfig()

	lookupRegistry := lookup.Registry{
		"rdns":      lookup.NewRDNS(),
		"useragent": lookup.NewUserAgent(),
	}

	s.loadInitialLookupConfig(lookupRegistry)

	queryServer := NewQueryServer(s.orch, s.cfgStore, s.remoteSearcher, s.localNodeID, lookupRegistry.Resolve, lookupRegistry.Names(), queryTimeout, maxFollowDuration, maxResultCount, s.logger.With("component", "query"))
	s.queryServer = queryServer
	vaultServer := NewVaultServer(s.orch, s.cfgStore, s.factories, s.peerVaultStats, s.remoteChunkLister, s.localNodeID, s.logger)
	configServer := NewSystemServer(SystemServerConfig{
		Orch:               s.orch,
		CfgStore:           s.cfgStore,
		Factories:          s.factories,
		CertManager:        s.certManager,
		PeerStats:          s.peerIngesterStats,
		PeerRouteStats:     s.peerRouteStats,
		LocalNodeID:        s.localNodeID,
		AfterConfigApply:   s.afterConfigApply,
		ConfigSignal:       s.configSignal,
		ResolveManagedFile: s.ResolveManagedFileByID,
		CloudTesters:       s.cloudTesters,
		Tokens:             s.tokens,
		PlacementReconcile: s.placementReconcile,
		OnTLSConfigChange:  s.reconfigureTLS,
		OnLookupConfigChange: func(cfg system.LookupConfig, mm system.MaxMindConfig) {
			s.applyLookupConfig(cfg, mm, lookupRegistry)
		},
	})
	lifecycleServer := NewLifecycleServer(s.orch, s.initiateShutdown, s.cluster, s.cfgStore, s.localNodeID, s.clusterAddress, s.peerStats, s.localStatsFn, s.logger)
	if s.joinClusterFn != nil {
		lifecycleServer.SetJoinClusterFunc(s.joinClusterFn)
	}
	if s.removeNodeFn != nil {
		lifecycleServer.SetRemoveNodeFunc(s.removeNodeFn)
	}
	if s.setNodeSuffrageFn != nil {
		lifecycleServer.SetNodeSuffrageFunc(s.setNodeSuffrageFn)
	}
	if s.statsSignal != nil {
		lifecycleServer.SetStatsSignal(s.statsSignal)
	}
	if s.peerRouteStats != nil {
		lifecycleServer.SetPeerRouteStats(s.peerRouteStats)
	}
	lifecycleServer.SetVaultFuncs(vaultServer.allVaultInfos, func(ctx context.Context) *apiv1.GetStatsResponse {
		resp, _ := vaultServer.GetStats(ctx, connect.NewRequest(&apiv1.GetStatsRequest{}))
		if resp != nil {
			return resp.Msg
		}
		return nil
	})
	authServer := NewAuthServer(s.cfgStore, s.tokens, s.logger, s.noAuth)
	jobServer := NewJobServer(s.orch.Scheduler(), s.localNodeID, s.peerJobs)

	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(vaultServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewSystemServiceHandler(configServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(authServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewJobServiceHandler(jobServer, handlerOpts...))

	s.registerProbes(mux)
	s.registerUploadHandler(mux)

	if h := frontend.Handler(); h != nil {
		mux.Handle("/", h)
	}

	return mux
}

func (s *Server) loadQueryConfig() (queryTimeout, maxFollowDuration time.Duration, maxResultCount int64) {
	if s.cfgStore == nil {
		return 0, 0, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), systemLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return 0, 0, 0
	}
	if ss.Query.Timeout != "" {
		if d, err := time.ParseDuration(ss.Query.Timeout); err == nil {
			queryTimeout = d
		}
	}
	if ss.Query.MaxFollowDuration != "" {
		if d, err := time.ParseDuration(ss.Query.MaxFollowDuration); err == nil {
			maxFollowDuration = d
		}
	}
	maxResultCount = int64(ss.Query.MaxResultCount)
	return queryTimeout, maxFollowDuration, maxResultCount
}

// Serve starts the server on the given listener.
// HTTP is always on; HTTPS is started when TLS enabled and default cert exists.
// It blocks until the server is stopped or an error occurs.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	// Start rate-limiter cleanup goroutine.
	rlCtx, rlCancel := context.WithCancel(context.Background())
	s.rlCancel = rlCancel
	s.rl.startCleanup(rlCtx, &s.rlWG, 3*time.Minute, 5*time.Minute)

	// Build the core handler once — reused by both HTTP and HTTPS.
	// Chain: tracking → CORS → securityHeaders → rateLimit → compress → mux
	mux := s.buildMux()
	s.handler = s.trackingMiddleware(s.corsMiddleware(securityHeadersMiddleware(rateLimitMiddleware(s.rl)(compressMiddleware(mux)))))

	// HTTP adds redirect-to-HTTPS + h2c (HTTP/2 without TLS).
	redirectHandler := s.redirectMiddleware(s.handler)
	s.server = &http.Server{
		Handler:           h2c.NewHandler(redirectHandler, &http2.Server{}),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	// Initial TLS config: start HTTPS if enabled
	s.reconfigureTLS()

	// Start Unix socket for local CLI access (no auth).
	if s.unixSocketConfig != "" {
		if err := s.ListenUnix(s.unixSocketConfig); err != nil {
			s.logger.Warn("unix socket failed, CLI will require --token", "error", err)
		}
	}

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

// BuildInternalHandler returns an http.Handler backed by a Connect mux with
// NoAuthInterceptor and NO routing interceptor. Used by the cluster's
// ForwardRPC handler to dispatch requests locally — mTLS on the cluster
// port already authenticated the peer, and the lack of routing interceptor
// prevents forwarding loops.
func (s *Server) BuildInternalHandler() http.Handler {
	noAuthOpt := connect.WithInterceptors(
		newRPCErrorLogInterceptor(s.logger),
		&auth.NoAuthInterceptor{},
	)
	return s.buildMux(noAuthOpt)
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
	// Stop rate-limiter cleanup goroutine.
	if s.rlCancel != nil {
		s.rlCancel()
		s.rlWG.Wait()
	}

	s.mu.Lock()
	server := s.server
	httpsServer := s.httpsServer
	s.httpsServer = nil
	s.httpsListener = nil
	unixServer := s.unixServer
	unixPath := s.unixPath
	s.unixServer = nil
	s.unixListener = nil
	s.unixPath = ""
	s.mu.Unlock()

	if unixServer != nil {
		_ = unixServer.Shutdown(ctx)
		_ = os.Remove(unixPath)
	}

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

// Handler returns an http.Handler for the server.
// This is useful for testing or embedding in another server.
func (s *Server) Handler() http.Handler {
	mux := s.buildMux()
	handler := h2c.NewHandler(mux, &http2.Server{})
	return s.trackingMiddleware(handler)
}
