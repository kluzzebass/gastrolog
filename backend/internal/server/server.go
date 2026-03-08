// Package server provides the Connect RPC server for GastroLog.
package server

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/frontend"
	"gastrolog/internal/home"
	"gastrolog/internal/logging"
	"gastrolog/internal/lookup"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// configLoadTimeout bounds how long background config store reads can take.
// Prevents indefinite hangs if the Raft FSM or underlying store is slow.
const configLoadTimeout = 5 * time.Second

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
	// lookup databases. Empty when running with in-memory config.
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
	RemoteSearcher RemoteSearcher

	// RemoteVaultForwarder forwards vault RPCs (ListChunks, GetIndexes,
	// ValidateVault) to remote cluster nodes. Nil in single-node mode.
	RemoteVaultForwarder RemoteVaultForwarder

	// PeerJobs provides active jobs from peer cluster nodes.
	// Nil in single-node mode.
	PeerJobs PeerJobsProvider

	// LocalStats returns real-time stats for the local node.
	LocalStats func() *apiv1.NodeStats

	// ConfigSignal broadcasts config changes to WatchConfig streams.
	// May be nil in tests.
	ConfigSignal *notify.Signal

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
	noAuth      bool
	logger      *slog.Logger
	cluster          ClusterStatusProvider
	peerStats        NodeStatsProvider
	peerVaultStats      PeerVaultStatsProvider
	peerIngesterStats   PeerIngesterStatsProvider
	peerRouteStats      PeerRouteStatsProvider
	remoteSearcher      RemoteSearcher
	remoteVaultForwarder RemoteVaultForwarder
	peerJobs             PeerJobsProvider
	localStatsFn     func() *apiv1.NodeStats
	localNodeID      string
	clusterAddress   string
	joinClusterFn    func(ctx context.Context, leaderAddr, joinToken string) error
	removeNodeFn        func(ctx context.Context, nodeID string) error
	setNodeSuffrageFn   func(ctx context.Context, nodeID string, voter bool) error
	startTime        time.Time
	homeDir          string                     // gastrolog home directory; empty for in-memory config
	afterConfigApply func(raftfsm.Notification) // non-raft dispatch hook
	configSignal     *notify.Signal             // broadcasts config changes to WatchConfig streams
	repairManagedFile func(fileID string) bool   // on-demand pull from peer; set by app wiring

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	handler  http.Handler // core handler (mux + CORS + tracking), shared by HTTP and HTTPS
	shutdown chan struct{}
	inFlight sync.WaitGroup // tracks in-flight requests for graceful drain
	draining atomic.Bool    // true when server is draining (rejecting new requests)

	rl       *rateLimiter   // per-IP rate limiter for auth endpoints
	rlCancel context.CancelFunc
	rlWG     sync.WaitGroup

	// Dynamic TLS: HTTPS listener when enabled
	httpsListener net.Listener
	httpsServer   *http.Server
	httpsPort     string
	redirectToHTTPS atomic.Bool

	// Unix socket listener for local CLI access (no auth required)
	unixSocketConfig string // path from Config.UnixSocket, consumed by Serve()
	unixListener     net.Listener
	unixServer       *http.Server
	unixPath         string
}

// New creates a new Server.
func New(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, tokens *auth.TokenService, cfg Config) *Server {
	return &Server{
		orch:        orch,
		cfgStore:    cfgStore,
		factories:   factories,
		tokens:      tokens,
		certManager: cfg.CertManager,
		noAuth:      cfg.NoAuth,
		logger:      logging.Default(cfg.Logger).With("component", "server"),
		cluster:          cfg.Cluster,
		peerStats:        cfg.PeerStats,
		peerVaultStats:    cfg.PeerVaultStats,
		peerIngesterStats: cfg.PeerIngesterStats,
		peerRouteStats:    cfg.PeerRouteStats,
		remoteSearcher:       cfg.RemoteSearcher,
		remoteVaultForwarder: cfg.RemoteVaultForwarder,
		peerJobs:             cfg.PeerJobs,
		localStatsFn:     cfg.LocalStats,
		localNodeID:      cfg.NodeID,
		clusterAddress:   cfg.ClusterAddress,
		joinClusterFn:    cfg.JoinClusterFunc,
		removeNodeFn:        cfg.RemoveNodeFunc,
		setNodeSuffrageFn:   cfg.SetNodeSuffrageFunc,
		startTime:        time.Now(),
		homeDir:          cfg.HomeDir,
		unixSocketConfig: cfg.UnixSocket,
		afterConfigApply: cfg.AfterConfigApply,
		configSignal:     cfg.ConfigSignal,
		shutdown:         make(chan struct{}),
		rl:          newRateLimiter(5.0/60.0, 5), // 5 req/min per IP, burst of 5
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

	var handlerOpts []connect.HandlerOption
	switch {
	case len(overrideOpts) > 0:
		handlerOpts = overrideOpts
	case s.noAuth:
		handlerOpts = append(handlerOpts, connect.WithInterceptors(&auth.NoAuthInterceptor{}))
	case s.tokens != nil:
		authInterceptor := auth.NewAuthInterceptor(s.tokens, s.cfgStore, &tokenValidator{cfgStore: s.cfgStore})
		handlerOpts = append(handlerOpts, connect.WithInterceptors(authInterceptor))
	}

	queryTimeout, maxFollowDuration, maxResultCount := s.loadQueryConfig()

	geoipTable := lookup.NewGeoIP()
	asnTable := lookup.NewASN()
	lookupRegistry := lookup.Registry{
		"rdns":  lookup.NewRDNS(),
		"geoip": geoipTable,
		"asn":   asnTable,
	}

	s.loadInitialLookupConfig(geoipTable, asnTable)

	queryServer := NewQueryServer(s.orch, s.cfgStore, s.remoteSearcher, s.localNodeID, lookupRegistry.Resolve, lookupRegistry.Names(), queryTimeout, maxFollowDuration, maxResultCount, s.logger.With("component", "query"))
	vaultServer := NewVaultServer(s.orch, s.cfgStore, s.factories, s.peerVaultStats, s.remoteVaultForwarder, s.localNodeID, s.logger)
	configServer := NewConfigServer(s.orch, s.cfgStore, s.factories, s.certManager, s.peerIngesterStats, s.peerRouteStats, s.localNodeID, s.afterConfigApply, s.configSignal)
	configServer.SetOnTLSConfigChange(s.reconfigureTLS)
	configServer.SetOnLookupConfigChange(func(cfg config.LookupConfig) {
		s.applyLookupConfig(cfg, geoipTable, asnTable)
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
	authServer := NewAuthServer(s.cfgStore, s.tokens, s.logger, s.noAuth)
	jobServer := NewJobServer(s.orch.Scheduler(), s.localNodeID, s.peerJobs)

	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(vaultServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewConfigServiceHandler(configServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(authServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewJobServiceHandler(jobServer, handlerOpts...))

	s.registerProbes(mux)
	s.registerMetrics(mux)
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
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
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

// loadInitialLookupConfig loads GeoIP and ASN databases from persisted config at startup.
// It also migrates any legacy flat MMDB files into managed file entities.
func (s *Server) loadInitialLookupConfig(geoipTable *lookup.GeoIP, asnTable *lookup.ASN) {
	if s.cfgStore == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return
	}
	s.applyLookupConfig(ss.Lookup, geoipTable, asnTable)
}

// effectiveLookupPaths resolves the actual MMDB paths to use.
// Manual paths (config overrides) take precedence; otherwise paths are resolved
// from the managed file manifest, falling back to the legacy flat directory.
func (s *Server) effectiveLookupPaths(cfg config.LookupConfig) (geoip, asn string) {
	geoip = cfg.GeoIPDBPath
	asn = cfg.ASNDBPath

	if s.homeDir == "" {
		return geoip, asn
	}

	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()

	// Resolve from manifest (managed files).
	if geoip == "" {
		geoip = s.resolveMMDBPath(ctx, "GeoLite2-City.mmdb")
	}
	if asn == "" {
		asn = s.resolveMMDBPath(ctx, "GeoLite2-ASN.mmdb")
	}

	return geoip, asn
}

// resolveMMDBPath finds an MMDB file via the managed file manifest.
func (s *Server) resolveMMDBPath(ctx context.Context, filename string) string {
	return s.ResolveManagedFilePath(ctx, filename)
}

// applyLookupConfig loads (or reloads) GeoIP and ASN databases from the given config.
// It also manages the maxmind-update cron job for automatic downloads.
func (s *Server) applyLookupConfig(cfg config.LookupConfig, geoipTable *lookup.GeoIP, asnTable *lookup.ASN) {
	geoipPath, asnPath := s.effectiveLookupPaths(cfg)

	if geoipPath != "" {
		if info, err := geoipTable.Load(geoipPath); err != nil {
			s.logger.Warn("failed to load GeoIP database", "path", geoipPath, "error", err)
		} else {
			s.logger.Info("loaded GeoIP database", "path", geoipPath, "type", info.DatabaseType, "build", info.BuildTime.Format("2006-01-02"))
			_ = geoipTable.WatchFile(geoipPath)
		}
	}
	if asnPath != "" {
		if info, err := asnTable.Load(asnPath); err != nil {
			s.logger.Warn("failed to load ASN database", "path", asnPath, "error", err)
		} else {
			s.logger.Info("loaded ASN database", "path", asnPath, "type", info.DatabaseType, "build", info.BuildTime.Format("2006-01-02"))
			_ = asnTable.WatchFile(asnPath)
		}
	}

	// Manage the maxmind-update cron job.
	s.manageMaxMindJob(cfg, geoipTable, asnTable)
}

// manageMaxMindJob adds or removes the maxmind-update cron job based on config.
func (s *Server) manageMaxMindJob(cfg config.LookupConfig, geoipTable *lookup.GeoIP, asnTable *lookup.ASN) {
	scheduler := s.orch.Scheduler()
	if scheduler == nil {
		return
	}

	hasCredentials := cfg.MaxMind.AccountID != "" && cfg.MaxMind.LicenseKey != ""
	if !cfg.MaxMind.AutoDownload || !hasCredentials || s.homeDir == "" {
		scheduler.RemoveJob("maxmind-update")
		return
	}

	updateFn := func() { s.runMaxMindUpdate(geoipTable, asnTable) }

	// Add recurring cron job: 03:00 on Tuesdays and Fridays.
	if err := scheduler.AddJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
		// Job may already exist (e.g. config re-applied). Update it.
		if err := scheduler.UpdateJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
			s.logger.Warn("failed to update maxmind-update job", "error", err)
		}
	}
	scheduler.Describe("maxmind-update", "Download MaxMind GeoLite2 databases")

	// If databases don't exist yet, trigger an immediate one-time download.
	geoipPath, asnPath := s.effectiveLookupPaths(cfg)
	if geoipPath == "" || asnPath == "" {
		_ = scheduler.RunOnce("maxmind-update-initial", updateFn)
	}
}

// runMaxMindUpdate downloads both MaxMind editions and updates the config timestamp.
func (s *Server) runMaxMindUpdate(geoipTable *lookup.GeoIP, asnTable *lookup.ASN) {
	loadCtx, loadCancel := context.WithTimeout(context.Background(), configLoadTimeout)
	ss, err := s.cfgStore.LoadServerSettings(loadCtx)
	loadCancel()
	if err != nil {
		s.logger.Warn("maxmind update: load config failed", "error", err)
		return
	}

	if !ss.Lookup.MaxMind.AutoDownload || ss.Lookup.MaxMind.AccountID == "" || ss.Lookup.MaxMind.LicenseKey == "" {
		return
	}

	hd := home.New(s.homeDir)
	downloadDir := hd.ManagedFilesDir()
	if err := os.MkdirAll(downloadDir, 0o750); err != nil {
		s.logger.Warn("maxmind update: create download dir", "error", err)
		return
	}

	ctx := context.Background()

	var anySuccess bool
	for _, edition := range []string{"GeoLite2-City", "GeoLite2-ASN"} {
		if err := lookup.DownloadDB(ctx, ss.Lookup.MaxMind.AccountID, ss.Lookup.MaxMind.LicenseKey, edition, downloadDir); err != nil {
			s.logger.Warn("maxmind update: download failed", "edition", edition, "error", err)
			continue
		}
		s.logger.Info("maxmind update: downloaded", "edition", edition)
		anySuccess = true

		// Register as a managed file entity so it distributes to all nodes.
		flatPath := filepath.Join(downloadDir, edition+".mmdb")
		if lf, err := s.RegisterFile(ctx, flatPath, ""); err != nil {
			s.logger.Warn("maxmind update: register file failed", "edition", edition, "error", err)
		} else {
			s.logger.Info("maxmind update: registered as managed file", "edition", edition, "file_id", lf.ID)
		}
	}

	if !anySuccess {
		return
	}

	// Reload databases from effective paths.
	reloadCtx, reloadCancel := context.WithTimeout(ctx, configLoadTimeout)
	ss, _ = s.cfgStore.LoadServerSettings(reloadCtx)
	reloadCancel()
	geoipPath, asnPath := s.effectiveLookupPaths(ss.Lookup)
	s.reloadGeoIP(geoipPath, geoipTable)
	s.reloadASN(asnPath, asnTable)

	// Update the last-update timestamp.
	saveCtx, saveCancel := context.WithTimeout(ctx, configLoadTimeout)
	defer saveCancel()
	ss.Lookup.MaxMind.LastUpdate = time.Now()
	if err := s.cfgStore.SaveServerSettings(saveCtx, ss); err != nil {
		s.logger.Warn("maxmind update: save timestamp failed", "error", err)
	}
}

func (s *Server) reloadGeoIP(path string, table *lookup.GeoIP) {
	if path == "" {
		return
	}
	info, err := table.Load(path)
	if err != nil {
		s.logger.Warn("maxmind update: reload GeoIP failed", "error", err)
		return
	}
	s.logger.Info("maxmind update: reloaded GeoIP", "build", info.BuildTime.Format("2006-01-02"))
}

func (s *Server) reloadASN(path string, table *lookup.ASN) {
	if path == "" {
		return
	}
	info, err := table.Load(path)
	if err != nil {
		s.logger.Warn("maxmind update: reload ASN failed", "error", err)
		return
	}
	s.logger.Info("maxmind update: reloaded ASN", "build", info.BuildTime.Format("2006-01-02"))
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
		ReadHeaderTimeout: 10 * time.Second,
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

// reconfigureTLS starts/stops HTTPS listener based on config. Safe to call from any goroutine.
func (s *Server) reconfigureTLS() {
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		s.logger.Warn("reconfigure TLS: load config failed", "error", err)
		return
	}
	// Fall back to HTTP if no default cert or TLS disabled
	tlsEnabled := ss.TLS.TLSEnabled && ss.TLS.DefaultCert != ""
	redirectEnabled := ss.TLS.HTTPToHTTPSRedirect && tlsEnabled

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

	// HTTPS port: use configured value, or derive from HTTP listener port + 1
	httpsPort := ss.TLS.HTTPSPort
	if httpsPort == "" {
		httpsPort = s.deriveHTTPSPort()
	}
	if httpsPort == "" {
		s.logger.Warn("reconfigure TLS: cannot determine HTTPS port")
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
	// Harden server-side TLS: require TLS 1.2+ and prefer modern curves.
	// CertManager.TLSConfig() is generic (also used for client certs);
	// server hardening is applied here, not in the shared config.
	tlsConfig.MinVersion = tls.VersionTLS12
	tlsConfig.CurvePreferences = []tls.CurveID{tls.X25519, tls.CurveP256}
	tlsLn := tls.NewListener(ln, tlsConfig)

	s.httpsListener = tlsLn
	s.httpsServer = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
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

// ListenUnix starts a secondary Unix socket listener alongside the primary
// TCP listener. Requests over the socket bypass authentication, providing
// token-free access for the local CLI. The socket file is removed on Stop.
// Must be called after Serve has set up the handler.
func (s *Server) ListenUnix(path string) error {
	// Remove stale socket file from a previous unclean shutdown.
	_ = os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", path, err)
	}
	// Restrict socket to owner only.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("chmod unix socket: %w", err)
	}

	// Build a separate mux with NoAuthInterceptor so the Connect layer
	// skips JWT validation entirely. The OS file permissions on the socket
	// provide the access control.
	noAuthOpt := connect.WithInterceptors(&auth.NoAuthInterceptor{})
	mux := s.buildMux(noAuthOpt)
	handler := s.trackingMiddleware(s.corsMiddleware(securityHeadersMiddleware(rateLimitMiddleware(s.rl)(compressMiddleware(mux)))))

	s.mu.Lock()
	s.unixListener = ln
	s.unixPath = path
	s.unixServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.mu.Unlock()

	s.logger.Info("unix socket listener started", "path", path)

	go func() {
		if err := s.unixServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Warn("unix socket serve error", "error", err)
		}
	}()
	return nil
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

// tokenValidator adapts config.Store to auth.TokenValidator.
type tokenValidator struct {
	cfgStore config.Store
}

func (tv *tokenValidator) IsTokenValid(ctx context.Context, userID string, issuedAt time.Time) (bool, error) {
	uid, err := uuid.Parse(userID)
	if err != nil {
		return false, fmt.Errorf("parse user ID %q: %w", userID, err)
	}
	user, err := tv.cfgStore.GetUser(ctx, uid)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil // deleted user
	}
	if !user.TokenInvalidatedAt.IsZero() && !issuedAt.After(user.TokenInvalidatedAt) {
		return false, nil // token issued before invalidation
	}
	return true, nil
}

// Client creates a set of Connect clients for the given base URL.
type Client struct {
	Query     gastrologv1connect.QueryServiceClient
	Vault     gastrologv1connect.VaultServiceClient
	Config    gastrologv1connect.ConfigServiceClient
	Lifecycle gastrologv1connect.LifecycleServiceClient
	Auth      gastrologv1connect.AuthServiceClient
	Job       gastrologv1connect.JobServiceClient
}

// NewClient creates Connect clients for the given base URL.
func NewClient(baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL, opts...),
		Vault:     gastrologv1connect.NewVaultServiceClient(http.DefaultClient, baseURL, opts...),
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
		Vault:     gastrologv1connect.NewVaultServiceClient(httpClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(httpClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(httpClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(httpClient, baseURL, opts...),
	}
}
