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
	"gastrolog/internal/server/routing"
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
	remoteChunkLister   RemoteChunkLister
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
	statsSignal      *notify.Signal             // broadcasts stats updates to WatchSystemStatus streams
	cloudTesters      map[string]CloudServiceTester
	repairManagedFile  func(fileID string) bool   // on-demand pull from peer; set by app wiring
	queryServer        *QueryServer              // stored for ExportToVault executor wiring
	routingForwarder   routing.UnaryForwarder     // forwards requests to remote nodes; nil in single-node

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
		remoteChunkLister:   cfg.RemoteChunkLister,
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
		cloudTesters:      cfg.CloudTesters,
		afterConfigApply:  cfg.AfterConfigApply,
		configSignal:      cfg.ConfigSignal,
		statsSignal:       cfg.StatsSignal,
		routingForwarder:  cfg.RoutingForwarder,
		shutdown:          make(chan struct{}),
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
	cfgStore    config.Store
	localNodeID string
}

// temporary: uses tier-level NodeID for node assignment until tier election.
func (c *configVaultOwner) ResolveVaultOwner(ctx context.Context, vaultID string) string {
	if c.cfgStore == nil {
		return ""
	}
	id, err := uuid.Parse(vaultID)
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

	tierMap := make(map[uuid.UUID]*config.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	// temporary: find the tier's NodeID to determine the owning node (until tier election).
	for _, tierID := range vaultCfg.TierIDs {
		tc := tierMap[tierID]
		if tc == nil {
			continue
		}
		if tc.NodeID != "" && tc.NodeID != c.localNodeID {
			return tc.NodeID
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
		interceptors := []connect.Interceptor{&auth.NoAuthInterceptor{}}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	case s.tokens != nil:
		authInterceptor := auth.NewAuthInterceptor(s.tokens, s.cfgStore, &tokenValidator{cfgStore: s.cfgStore})
		interceptors := []connect.Interceptor{authInterceptor}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	default:
		// No auth configured (tests without NoAuth flag). Still need the
		// routing interceptor for cluster forwarding.
		if ri := s.routingInterceptor(); len(ri) > 0 {
			handlerOpts = append(handlerOpts, connect.WithInterceptors(ri...))
		}
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
	configServer := NewConfigServer(ConfigServerConfig{
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
		OnTLSConfigChange:  s.reconfigureTLS,
		OnLookupConfigChange: func(cfg config.LookupConfig, mm config.MaxMindConfig) {
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

// loadInitialLookupConfig loads MMDB, HTTP, and JSON lookup tables from persisted config at startup.
func (s *Server) loadInitialLookupConfig(registry lookup.Registry) {
	if s.cfgStore == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()
	ss, err := s.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return
	}
	s.applyLookupConfig(ss.Lookup, ss.MaxMind, registry)
}

// resolveMMDBPath finds an MMDB file via the managed file manifest.
func (s *Server) resolveMMDBPath(ctx context.Context, filename string) string {
	return s.ResolveManagedFilePath(ctx, filename)
}

// applyLookupConfig loads (or reloads) MMDB, HTTP, and JSON lookup tables from the given config.
// It also manages the maxmind-update cron job for automatic downloads.
func (s *Server) applyLookupConfig(cfg config.LookupConfig, mm config.MaxMindConfig, registry lookup.Registry) {
	// Register MMDB lookup tables (GeoIP City / ASN) from config.
	s.registerMMDBLookups(cfg, registry)

	// Register HTTP lookup tables from config.
	s.registerHTTPLookups(cfg, registry)

	// Register JSON file lookup tables from config.
	s.registerJSONFileLookups(cfg, registry)

	// Register CSV lookup tables from config.
	s.registerCSVLookups(cfg, registry)

	// Manage the maxmind-update cron job.
	s.manageMaxMindJob(mm, registry)
}

// registerMMDBLookups registers MMDB-backed lookup tables (GeoIP City / ASN) from config.
// Follows the same lifecycle pattern as registerJSONFileLookups: close+remove stale, create+load new.
func (s *Server) registerMMDBLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()

	// Build keep set of names that should exist.
	keep := make(map[string]struct{}, len(cfg.MMDBLookups))
	for _, mcfg := range cfg.MMDBLookups {
		if mcfg.Name != "" {
			keep[mcfg.Name] = struct{}{}
		}
	}

	// Close and remove any MMDB lookups no longer in config.
	for name, table := range registry {
		if m, ok := table.(*lookup.MMDB); ok {
			if _, exists := keep[name]; !exists {
				m.Close()
				delete(registry, name)
				s.logger.Info("removed MMDB lookup table", "name", name)
			}
		}
	}

	for _, mcfg := range cfg.MMDBLookups {
		if mcfg.Name == "" {
			continue
		}

		// Resolve MMDB path: from managed file ID, or auto-downloaded by db_type.
		var mmdbPath string
		if mcfg.FileID != "" {
			mmdbPath = s.ResolveManagedFileByID(ctx, mcfg.FileID)
		} else {
			// No file_id → use auto-downloaded database by type.
			mmdbPath = s.resolveMMDBPath(ctx, mmdbFileName(mcfg.DBType))
		}
		// Close existing table if any.
		if existing, ok := registry[mcfg.Name]; ok {
			if m, ok := existing.(*lookup.MMDB); ok {
				m.Close()
			}
		}
		m := lookup.NewMMDB(mcfg.DBType)
		if mmdbPath != "" {
			if info, err := m.Load(mmdbPath); err != nil {
				s.logger.Warn("failed to load MMDB", "name", mcfg.Name, "path", mmdbPath, "error", err)
			} else {
				s.logger.Info("loaded MMDB lookup", "name", mcfg.Name, "type", info.DatabaseType, "build", info.BuildTime.Format("2006-01-02"))
				_ = m.WatchFile(mmdbPath)
			}
		}
		registry[mcfg.Name] = m
	}
}

// mmdbFileName returns the auto-download filename for a given MMDB db type.
func mmdbFileName(dbType string) string {
	switch dbType {
	case "city":
		return "GeoLite2-City.mmdb"
	case "asn":
		return "GeoLite2-ASN.mmdb"
	default:
		return ""
	}
}

// registerHTTPLookups registers HTTP API lookup tables from config into the registry.
func (s *Server) registerHTTPLookups(cfg config.LookupConfig, registry lookup.Registry) {
	for _, hcfg := range cfg.HTTPLookups {
		if hcfg.Name == "" || hcfg.URLTemplate == "" {
			continue
		}

		paramNames := make([]string, len(hcfg.Parameters))
		for j, p := range hcfg.Parameters {
			paramNames[j] = p.Name
		}
		lcfg := lookup.HTTPConfig{
			URLTemplate:  hcfg.URLTemplate,
			Headers:      hcfg.Headers,
			ResponsePaths: hcfg.ResponsePaths,
			Parameters:   paramNames,
			CacheSize:    hcfg.CacheSize,
		}
		if hcfg.Timeout != "" {
			if d, err := time.ParseDuration(hcfg.Timeout); err == nil {
				lcfg.Timeout = d
			}
		}
		if hcfg.CacheTTL != "" {
			if d, err := time.ParseDuration(hcfg.CacheTTL); err == nil {
				lcfg.CacheTTL = d
			}
		}

		registry[hcfg.Name] = lookup.NewHTTP(lcfg)
		s.logger.Info("registered HTTP lookup table", "name", hcfg.Name, "url", hcfg.URLTemplate)
	}
}

// registerJSONFileLookups registers JSON file-backed lookup tables from config into the registry.
// It also cleans up any previously registered JSON file lookups that are no longer in the config.
func (s *Server) registerJSONFileLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build set of names that should exist.
	keep := make(map[string]struct{}, len(cfg.JSONFileLookups))
	for _, jcfg := range cfg.JSONFileLookups {
		if jcfg.Name != "" {
			keep[jcfg.Name] = struct{}{}
		}
	}

	// Close and remove any JSON file lookups no longer in config.
	for name, table := range registry {
		if jf, ok := table.(*lookup.JSONFile); ok {
			if _, exists := keep[name]; !exists {
				jf.Close()
				delete(registry, name)
				s.logger.Info("removed JSON file lookup table", "name", name)
			}
		}
	}

	for _, jcfg := range cfg.JSONFileLookups {
		if jcfg.Name == "" || jcfg.FileID == "" {
			continue
		}

		// Close any existing JSON file lookup with the same name (stops its watcher).
		if existing, ok := registry[jcfg.Name]; ok {
			if jf, ok := existing.(*lookup.JSONFile); ok {
				jf.Close()
			}
		}

		filePath := s.ResolveManagedFileByID(ctx, jcfg.FileID)
		if filePath == "" {
			s.logger.Warn("JSON lookup file not found", "name", jcfg.Name, "file_id", jcfg.FileID)
			continue
		}

		paramNames := make([]string, len(jcfg.Parameters))
		for k, p := range jcfg.Parameters {
			paramNames[k] = p.Name
		}
		jf := lookup.NewJSONFile(lookup.JSONFileConfig{
			Query:         jcfg.Query,
			ResponsePaths: jcfg.ResponsePaths,
			Parameters:    paramNames,
		})

		if err := jf.Load(filePath); err != nil {
			s.logger.Warn("failed to load JSON lookup file", "name", jcfg.Name, "path", filePath, "error", err)
			continue
		}
		_ = jf.WatchFile(filePath)

		registry[jcfg.Name] = jf
		s.logger.Info("registered JSON file lookup table", "name", jcfg.Name, "path", filePath)
	}
}

// registerCSVLookups registers CSV file-backed lookup tables from config.
func (s *Server) registerCSVLookups(cfg config.LookupConfig, registry lookup.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), configLoadTimeout)
	defer cancel()

	keep := make(map[string]struct{}, len(cfg.CSVLookups))
	for _, ccfg := range cfg.CSVLookups {
		if ccfg.Name != "" {
			keep[ccfg.Name] = struct{}{}
		}
	}

	// Close and remove any CSV lookups no longer in config.
	for name, table := range registry {
		if ct, ok := table.(*lookup.CSV); ok {
			if _, exists := keep[name]; !exists {
				ct.Close()
				delete(registry, name)
				s.logger.Info("removed CSV lookup table", "name", name)
			}
		}
	}

	for _, ccfg := range cfg.CSVLookups {
		if ccfg.Name == "" || ccfg.FileID == "" {
			continue
		}

		if existing, ok := registry[ccfg.Name]; ok {
			if ct, ok := existing.(*lookup.CSV); ok {
				ct.Close()
			}
		}

		filePath := s.ResolveManagedFileByID(ctx, ccfg.FileID)
		if filePath == "" {
			s.logger.Warn("CSV lookup file not found", "name", ccfg.Name, "file_id", ccfg.FileID)
			continue
		}

		ct := lookup.NewCSV(lookup.CSVConfig{
			KeyColumn:    ccfg.KeyColumn,
			ValueColumns: ccfg.ValueColumns,
		})

		if err := ct.Load(filePath); err != nil {
			s.logger.Warn("failed to load CSV lookup file", "name", ccfg.Name, "path", filePath, "error", err)
			continue
		}
		_ = ct.WatchFile(filePath)

		registry[ccfg.Name] = ct
		s.logger.Info("registered CSV lookup table", "name", ccfg.Name, "path", filePath)
	}
}

// manageMaxMindJob adds or removes the maxmind-update cron job based on config.
func (s *Server) manageMaxMindJob(mm config.MaxMindConfig, registry lookup.Registry) {
	scheduler := s.orch.Scheduler()
	if scheduler == nil {
		return
	}

	hasCredentials := mm.AccountID != "" && mm.LicenseKey != ""
	if !mm.AutoDownload || !hasCredentials || s.homeDir == "" {
		scheduler.RemoveJob("maxmind-update")
		return
	}

	updateFn := func() { s.runMaxMindUpdate(registry) }

	// Add recurring cron job: 03:00 on Tuesdays and Fridays.
	if err := scheduler.AddJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
		// Job may already exist (e.g. config re-applied). Update it.
		if err := scheduler.UpdateJob("maxmind-update", "0 3 * * 2,5", updateFn); err != nil {
			s.logger.Warn("failed to update maxmind-update job", "error", err)
		}
	}
	scheduler.Describe("maxmind-update", "Download MaxMind GeoLite2 databases")

	// If any MMDB entry has no file available yet, trigger an immediate download.
	// Load current lookup config to check MMDB entries.
	loadCtx, loadCancel := context.WithTimeout(context.Background(), configLoadTimeout)
	ss, err := s.cfgStore.LoadServerSettings(loadCtx)
	loadCancel()
	needsDownload := false
	if err == nil {
		for _, mcfg := range ss.Lookup.MMDBLookups {
			if mcfg.FileID == "" {
				needsDownload = true
				break
			}
		}
	}
	if needsDownload {
		_ = scheduler.RunOnce("maxmind-update-initial", updateFn)
	}
}

// runMaxMindUpdate downloads both MaxMind editions, registers them as managed files,
// and reloads any MMDB registry entries that use auto-downloaded databases.
func (s *Server) runMaxMindUpdate(registry lookup.Registry) {
	loadCtx, loadCancel := context.WithTimeout(context.Background(), configLoadTimeout)
	ss, err := s.cfgStore.LoadServerSettings(loadCtx)
	loadCancel()
	if err != nil {
		s.logger.Warn("maxmind update: load config failed", "error", err)
		return
	}

	if !ss.MaxMind.AutoDownload || ss.MaxMind.AccountID == "" || ss.MaxMind.LicenseKey == "" {
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
		if err := lookup.DownloadDB(ctx, ss.MaxMind.AccountID, ss.MaxMind.LicenseKey, edition, downloadDir); err != nil {
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

	// Re-apply config to reload MMDB entries that use auto-downloaded databases.
	reloadCtx, reloadCancel := context.WithTimeout(ctx, configLoadTimeout)
	ss, _ = s.cfgStore.LoadServerSettings(reloadCtx)
	reloadCancel()
	s.registerMMDBLookups(ss.Lookup, registry)

	// Update the last-update timestamp.
	saveCtx, saveCancel := context.WithTimeout(ctx, configLoadTimeout)
	defer saveCancel()
	ss.MaxMind.LastUpdate = time.Now()
	if err := s.cfgStore.SaveServerSettings(saveCtx, ss); err != nil {
		s.logger.Warn("maxmind update: save timestamp failed", "error", err)
	}
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

// BuildInternalHandler returns an http.Handler backed by a Connect mux with
// NoAuthInterceptor and NO routing interceptor. Used by the cluster's
// ForwardRPC handler to dispatch requests locally — mTLS on the cluster
// port already authenticated the peer, and the lack of routing interceptor
// prevents forwarding loops.
func (s *Server) BuildInternalHandler() http.Handler {
	noAuthOpt := connect.WithInterceptors(&auth.NoAuthInterceptor{})
	return s.buildMux(noAuthOpt)
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
