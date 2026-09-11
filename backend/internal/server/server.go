// Package server provides the Connect RPC server for GastroLog.
package server

import (
	"cmp"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/cluster"
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

// Transport-level timeouts for the HTTP, HTTPS, and Unix socket servers.
// These bound resource holding (goroutines, file descriptors) by a client
// that opens a connection and never finishes it — the slowloris family of
// attacks — before any application-level auth or handler code runs.
//
// WriteTimeout is deliberately left unset (zero, meaning unbounded) on all
// three servers. The primary reason is plain HTTP/1.1, which every one of
// these listeners speaks by default (the HTTPS listener never negotiates
// h2 — CertManager.TLSConfig() sets no ALPN NextProtos — and the Unix
// socket listener isn't wrapped in h2c at all): WriteTimeout there is a
// single absolute deadline covering the whole response, headers through
// last byte, not a per-write or per-inactivity bound. This API serves
// long-lived Connect server-streaming RPCs (Follow, Search, ExportVault,
// WatchSystem, WatchChunks, WatchIngesterStatus, WatchJobs,
// WatchSystemStatus); a nonzero WriteTimeout caps every one of them to a
// fixed lifetime measured from when the response started, which is the
// same failure mode a slowloris timeout guards against, just from the
// opposite end of the connection. The main HTTP listener additionally
// speaks unencrypted HTTP/2 (h2c, see Serve below), where WriteTimeout is
// armed as one non-resetting per-stream timer set at stream creation — the
// identical hazard enforced per RPC call instead of per connection.
//
// ReadHeaderTimeout and IdleTimeout close the slowloris/idle-hold vectors
// without this hazard, and Go's native HTTP/2 reads the http.Server's own
// IdleTimeout for h2c connections, so one setting binds both protocols.
const (
	// readHeaderTimeout is the maximum time to read HTTP request headers.
	readHeaderTimeout = 10 * time.Second

	// readTimeout is the maximum time to read an entire request, including
	// the body. Kept tight because it has to work for every handler on
	// these listeners except one: the managed-file upload endpoint (up to
	// 256 MiB, upload.go maxUploadSize) explicitly clears its own read
	// deadline via http.ResponseController, since no single duration both
	// bounds a slowloris body trickle for a 4 MiB Connect RPC and
	// tolerates a legitimate 256 MiB transfer on a slow link.
	readTimeout = 30 * time.Second

	// idleTimeout is the maximum time a keep-alive connection may sit idle
	// between requests, i.e. with no request being read or served. This
	// never fires against an in-progress request or stream on any of the
	// three listeners — see the package comment above.
	idleTimeout = 120 * time.Second
)

// newTimedServer builds an http.Server with the standard transport
// timeouts applied, shared by the HTTP, HTTPS, and Unix socket listeners.
// Tests use this directly with short-lived values to exercise timeout
// behavior without waiting out the production durations.
func newTimedServer(handler http.Handler, hdrTimeout, reqTimeout, connIdleTimeout time.Duration) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: hdrTimeout,
		ReadTimeout:       reqTimeout,
		IdleTimeout:       connIdleTimeout,
	}
}

// Config holds server configuration.
type Config struct {
	// Logger for structured logging.
	Logger *slog.Logger

	// CertManager provides TLS certificates. When non-nil and a server cert
	// is configured, the server can serve HTTPS.
	CertManager CertManager

	// NoAuth disables authentication. All requests are treated as admin.
	NoAuth bool

	// TrustContextClaims attaches no authenticator and serves whatever
	// identity is already on the request context. Only correct where
	// something upstream established that identity; on a listener that
	// faces a network it publishes every RPC to every caller.
	TrustContextClaims bool

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

	// PeerPipelineDisk aggregates per-node pipeline disk counts from peer broadcasts.
	// Nil in single-node mode. Typically the same *cluster.PeerState as PeerRouteStats.
	PeerPipelineDisk PeerPipelineDiskProvider

	// PeerStorageStats looks up a storage's disk-guard state from cluster
	// peer broadcasts, for storages not locally hosted.
	// Nil in single-node mode. Typically the same *cluster.PeerState as PeerStats.
	PeerStorageStats PeerStorageStatsProvider

	// RemoteSearcher forwards search requests to remote cluster nodes.
	// Nil in single-node mode.
	RemoteSearcher             RemoteSearcher
	RemoteChunkLister          RemoteChunkLister
	RemoteVaultValidator       RemoteVaultValidator
	RemoteCloudIndexReconciler RemoteCloudIndexReconciler
	RemotePipelineBacklog      RemotePipelineBacklogGetter
	RemoteChunkWatcher         RemoteChunkWatcher
	RemoteIndexer              RemoteIndexer

	// PeerJobs provides active jobs from peer cluster nodes.
	// Nil in single-node mode.
	PeerJobs PeerJobsProvider

	// LocalStats returns real-time stats for the local node.
	LocalStats func() *apiv1.NodeStats
	// ClusterRouteRates returns the server-side cluster-total route rate
	// series (instant/30s/1m + spark) windowed over summed cluster counters
	// by the stats collector. Nil in single-node mode.
	ClusterRouteRates func() (*apiv1.ThroughputRate, *apiv1.ThroughputRate)

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
	// cluster. opts.Force bypasses the leader-side removal gates
	// (orphan-refusal, RF-preservation) and opts.Policy selects the
	// operator vs preStop-self stance. Nil disables.
	RemoveNodeFunc cluster.RemoveNodeFunc

	// SetNodeSuffrageFunc is called by the SetNodeSuffrage RPC to promote or
	// demote a node. Handles leader-forwarding internally. Nil disables.
	SetNodeSuffrageFunc func(ctx context.Context, nodeID string, voter bool) error

	// CloudTesters maps cloud service types to connection test functions.
	CloudTesters map[string]CloudServiceTester

	// RemoteIngesterCheck asks a specific node to validate a candidate ingester
	// config against itself. Nil in single-node mode.
	RemoteIngesterCheck RemoteIngesterValidator

	// RegisterIngesterChecker, when set, is handed this node's ingester
	// validation so the cluster layer can serve it for peers. The checks are
	// per-node facts — factory construction and a listen-address trial bind —
	// so a peer has to be able to ask this node directly rather than compute
	// the answer itself.
	RegisterIngesterChecker func(func(context.Context, string, map[string]string, []byte) (bool, string))

	// RoutingForwarder forwards requests to remote nodes via ForwardRPC.
	// Nil in single-node mode. Satisfies routing.UnaryForwarder.
	RoutingForwarder routing.UnaryForwarder

	// PlacementReconcile runs synchronous placement so RPC responses include
	// vault placements. Nil in single-node or non-cluster mode.
	PlacementReconcile func(ctx context.Context)

	// BootstrapTokenServeSecret enables the GET /cluster/bootstrap-token
	// endpoint when non-empty. The endpoint is gated on a shared secret
	// sent in the X-Bootstrap-Token-Secret header. Used by joiners
	// configured with --bootstrap-token-url to fetch the join token
	// without log-scraping or shared volumes.
	BootstrapTokenServeSecret string

	// BootstrapTokenFn returns the current cluster join token. Called
	// by the bootstrap-token endpoint on each successful auth check.
	// Nil when the endpoint is disabled or not applicable.
	BootstrapTokenFn func() (string, error)

	// LogFilter is the ComponentFilterHandler whose rule set is driven
	// from the system config store. Used by the PutLogLevels /
	// ListLogComponents RPC handlers. Nil disables both.
	LogFilter *logging.ComponentFilterHandler

	// EnvironmentLabel and EnvironmentColor are display-only deploy
	// metadata surfaced to the UI header so operators can tell at a
	// glance which deployment they are looking at. Empty label hides the
	// banner.
	EnvironmentLabel string
	EnvironmentColor string
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
	orch                       *orchestrator.Orchestrator
	cfgStore                   system.Store
	factories                  orchestrator.Factories
	tokens                     *auth.TokenService
	certManager                CertManager
	noAuth                     bool
	trustContextClaims         bool
	logger                     *slog.Logger
	cluster                    ClusterStatusProvider
	peerStats                  NodeStatsProvider
	peerVaultStats             PeerVaultStatsProvider
	peerIngesterStats          PeerIngesterStatsProvider
	peerRouteStats             PeerRouteStatsProvider
	peerPipelineDisk           PeerPipelineDiskProvider
	peerStorageStats           PeerStorageStatsProvider
	remoteIngesterCheck        RemoteIngesterValidator
	registerIngesterCheck      func(func(context.Context, string, map[string]string, []byte) (bool, string))
	remoteSearcher             RemoteSearcher
	remoteChunkLister          RemoteChunkLister
	remoteVaultValidator       RemoteVaultValidator
	remoteCloudIndexReconciler RemoteCloudIndexReconciler
	remotePipelineBacklog      RemotePipelineBacklogGetter
	remoteChunkWatcher         RemoteChunkWatcher
	remoteIndexer              RemoteIndexer
	peerJobs                   PeerJobsProvider
	localStatsFn               func() *apiv1.NodeStats
	clusterRouteRatesFn        func() (*apiv1.ThroughputRate, *apiv1.ThroughputRate)
	localNodeID                string
	clusterAddress             string
	joinClusterFn              func(ctx context.Context, leaderAddr, joinToken string) error
	removeNodeFn               cluster.RemoveNodeFunc
	setNodeSuffrageFn          func(ctx context.Context, nodeID string, voter bool) error
	startTime                  time.Time
	homeDir                    string                     // gastrolog home directory; empty for in-memory config
	afterConfigApply           func(raftfsm.Notification) // non-raft dispatch hook
	configSignal               *notify.Signal             // broadcasts config changes to WatchConfig streams
	statsSignal                *notify.Signal             // broadcasts stats updates to WatchSystemStatus streams
	cloudTesters               map[string]CloudServiceTester
	repairManagedFile          func(fileID string) bool  // on-demand pull from peer; set by app wiring
	queryServer                *QueryServer              // stored for ExportToVault executor wiring
	routingForwarder           routing.UnaryForwarder    // forwards requests to remote nodes; nil in single-node
	placementReconcile         func(ctx context.Context) // synchronous placement; nil in non-cluster mode

	// Bootstrap-token endpoint configuration. When the secret is non-empty,
	// /cluster/bootstrap-token is registered and gated on the
	// X-Bootstrap-Token-Secret header.
	bootstrapTokenServeSecret string
	bootstrapTokenFn          func() (string, error)

	logFilter *logging.ComponentFilterHandler // nil disables PutLogLevels/ListLogComponents

	// Environment banner. Empty label hides the banner.
	environmentLabel string
	environmentColor string

	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	handler  http.Handler // core handler (mux + CORS + tracking), shared by HTTP and HTTPS
	shutdown chan struct{}
	inFlight sync.WaitGroup // tracks in-flight requests for graceful drain
	draining atomic.Bool    // true when server is draining (rejecting new requests)

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
		orch:                       orch,
		cfgStore:                   cfgStore,
		factories:                  factories,
		tokens:                     tokens,
		certManager:                cfg.CertManager,
		noAuth:                     cfg.NoAuth,
		trustContextClaims:         cfg.TrustContextClaims,
		logger:                     compServer.Apply(logging.Default(cfg.Logger)),
		cluster:                    cfg.Cluster,
		peerStats:                  cfg.PeerStats,
		peerVaultStats:             cfg.PeerVaultStats,
		peerIngesterStats:          cfg.PeerIngesterStats,
		peerRouteStats:             cfg.PeerRouteStats,
		peerPipelineDisk:           cfg.PeerPipelineDisk,
		peerStorageStats:           cfg.PeerStorageStats,
		remoteIngesterCheck:        cfg.RemoteIngesterCheck,
		registerIngesterCheck:      cfg.RegisterIngesterChecker,
		remoteSearcher:             cfg.RemoteSearcher,
		remoteChunkLister:          cfg.RemoteChunkLister,
		remoteVaultValidator:       cfg.RemoteVaultValidator,
		remoteCloudIndexReconciler: cfg.RemoteCloudIndexReconciler,
		remotePipelineBacklog:      cfg.RemotePipelineBacklog,
		remoteChunkWatcher:         cfg.RemoteChunkWatcher,
		remoteIndexer:              cfg.RemoteIndexer,
		peerJobs:                   cfg.PeerJobs,
		localStatsFn:               cfg.LocalStats,
		clusterRouteRatesFn:        cfg.ClusterRouteRates,
		localNodeID:                cfg.NodeID,
		clusterAddress:             cfg.ClusterAddress,
		joinClusterFn:              cfg.JoinClusterFunc,
		removeNodeFn:               cfg.RemoveNodeFunc,
		setNodeSuffrageFn:          cfg.SetNodeSuffrageFunc,
		startTime:                  time.Now(),
		homeDir:                    cfg.HomeDir,
		unixSocketConfig:           cfg.UnixSocket,
		cloudTesters:               cfg.CloudTesters,
		afterConfigApply:           cfg.AfterConfigApply,
		configSignal:               cfg.ConfigSignal,
		statsSignal:                cfg.StatsSignal,
		routingForwarder:           cfg.RoutingForwarder,
		placementReconcile:         cfg.PlacementReconcile,
		bootstrapTokenServeSecret:  cfg.BootstrapTokenServeSecret,
		bootstrapTokenFn:           cfg.BootstrapTokenFn,
		logFilter:                  cfg.LogFilter,
		environmentLabel:           cfg.EnvironmentLabel,
		environmentColor:           cfg.EnvironmentColor,
		shutdown:                   make(chan struct{}),
		rl:                         newRateLimiter(5.0/60.0, 5), // 5 req/min per IP, burst of 5
	}
}

// registerBootstrapToken adds the /cluster/bootstrap-token endpoint
// when the operator configured BootstrapTokenServeSecret + BootstrapTokenFn.
// Joiners running with --bootstrap-token-url + --bootstrap-token-secret
// fetch the join token here, gated on the shared secret in the
// X-Bootstrap-Token-Secret header.
func (s *Server) registerBootstrapToken(mux *http.ServeMux) {
	if s.bootstrapTokenServeSecret == "" || s.bootstrapTokenFn == nil {
		return
	}
	servedSecret := s.bootstrapTokenServeSecret
	tokenFn := s.bootstrapTokenFn
	mux.HandleFunc("/cluster/bootstrap-token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		got := r.Header.Get("X-Bootstrap-Token-Secret")
		if subtle.ConstantTimeCompare([]byte(got), []byte(servedSecret)) != 1 {
			s.logger.Warn("bootstrap token endpoint rejected request: bad secret", "remote", r.RemoteAddr)
			http.Error(w, "invalid secret", http.StatusUnauthorized)
			return
		}
		token, err := tokenFn()
		if err != nil {
			s.logger.Error("bootstrap token endpoint failed to load token", "error", err)
			http.Error(w, "token unavailable", http.StatusServiceUnavailable)
			return
		}
		if token == "" {
			http.Error(w, "token unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(token))
	})
}

// registerProbes adds Kubernetes liveness and readiness probe endpoints.
func (s *Server) registerProbes(mux *http.ServeMux) {
	// Liveness probe - returns 200 if the process is alive
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness probe - returns 200 if ready to accept traffic
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.orch.IsRunning() && !s.draining.Load() && s.orch.LocalVaultsReplicationReady() {
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
	ri := routing.NewRoutingInterceptor(registry, s.localNodeID, ownerResolvers(s.cfgStore), s.routingForwarder)
	return []connect.Interceptor{ri}
}

// ownerResolvers builds the resource-owner resolver set the routing
// interceptor consults for RouteToResourceOwner RPCs. Every resolver reads
// the cluster-ctl Raft store, so any node answers identically. Adding a
// resource kind means one entry here plus a Resource declaration on the
// procedure in routing.DefaultRoutes().
func ownerResolvers(cfgStore system.Store) routing.OwnerResolvers {
	return routing.OwnerResolvers{
		routing.ResourceVault:    &configVaultOwner{cfgStore: cfgStore},
		routing.ResourceIngester: &configIngesterOwner{cfgStore: cfgStore},
	}
}

// configVaultOwner resolves vault ownership from the config store.
type configVaultOwner struct {
	cfgStore system.Store
}

// ResolveOwners returns the vault's leader node, or nil when the vault has
// no resolvable placement. A vault that is absent from config resolves to
// nil rather than an error: the handler's own vault lookup produces the
// domain-accurate error (and some callers legitimately name a vault this
// node knows nothing about yet).
//
// Reads placements from their owner.
func (c *configVaultOwner) ResolveOwners(ctx context.Context, vaultID string) ([]string, error) {
	if c.cfgStore == nil {
		return nil, nil
	}
	id, err := glid.ParseUUID(vaultID)
	if err != nil {
		return nil, nil //nolint:nilerr // not a GLID reference — not the routing layer's to interpret; the handler validates it
	}
	vaultCfg, err := c.cfgStore.GetVault(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read vault config: %w", err)
	}
	if vaultCfg == nil {
		return nil, nil
	}
	// Placements come from their owner; the domain VaultConfig does not
	// carry them.
	placements, err := c.cfgStore.GetVaultPlacements(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read vault placements: %w", err)
	}
	if len(placements) == 0 {
		return nil, nil
	}
	nscs, err := c.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil, fmt.Errorf("read node storage configs: %w", err)
	}
	leaderNodeID := system.LeaderNodeID(placements, nscs)
	if leaderNodeID == "" {
		return nil, nil
	}
	return []string{leaderNodeID}, nil
}

// configIngesterOwner resolves which node(s) run an ingester, from the
// Raft-replicated alive map (system.Runtime). The alive map is written by
// the node that actually started the ingester, so it is the owner of this
// fact — and it is plural by construction: a parallel (non-singleton)
// ingester is alive on every eligible node, which is what ingester HA
// needs the routing layer to be able to express.
type configIngesterOwner struct {
	cfgStore system.Store
}

// ResolveOwners returns the sorted set of nodes currently running the
// ingester.
//
//   - Ingester absent from config → ErrResourceNotFound. Definitive and
//     identical on every node, instead of whichever node received the
//     request reporting "not found" because it merely does not run it.
//   - Configured but not alive anywhere (not started yet, or its alive
//     apply has not landed) → nil, so the request executes locally and the
//     handler reports the runtime state it sees.
func (c *configIngesterOwner) ResolveOwners(ctx context.Context, ingesterID string) ([]string, error) {
	if c.cfgStore == nil {
		return nil, nil
	}
	id, err := glid.ParseUUID(ingesterID)
	if err != nil {
		return nil, nil //nolint:nilerr // not a GLID reference — the handler reports the invalid ID
	}
	cfg, err := c.cfgStore.GetIngester(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read ingester config: %w", err)
	}
	if cfg == nil {
		return nil, routing.ErrResourceNotFound
	}
	alive, err := c.cfgStore.GetIngesterAlive(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read ingester alive state: %w", err)
	}
	owners := make([]string, 0, len(alive))
	for nodeID, isAlive := range alive {
		if isAlive {
			owners = append(owners, nodeID)
		}
	}
	if len(owners) == 0 {
		return nil, nil
	}
	slices.Sort(owners)
	return owners, nil
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
		connect.WithReadMaxBytes(cluster.ForwardRPCMaxResponseBytes),
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
		authInterceptor := auth.NewAuthInterceptor(s.apiVerifier(), s.cfgStore)
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger), authInterceptor}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	case s.trustContextClaims:
		// Identity was established before the request reached this mux, so
		// there is nothing to authenticate here.
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger)}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	default:
		// Built with neither a token service nor NoAuth: there is no way to
		// tell callers apart, so every RPC is refused. Serving instead would
		// publish the whole API to anyone who reaches the listener, and the
		// only signal would be its absence.
		interceptors := []connect.Interceptor{newRPCErrorLogInterceptor(s.logger), &auth.DenyAllInterceptor{}}
		interceptors = append(interceptors, s.routingInterceptor()...)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	}

	queryTimeout, maxFollowDuration, maxResultCount := s.loadQueryConfig()

	lookupRegistry := lookup.Registry{
		"rdns":      lookup.NewRDNS(s.logger),
		"useragent": lookup.NewUserAgent(),
	}

	s.loadInitialLookupConfig(lookupRegistry)

	queryServer := NewQueryServer(s.orch, s.cfgStore, s.remoteSearcher, s.localNodeID, lookupRegistry.Resolve, lookupRegistry.Names(), queryTimeout, maxFollowDuration, maxResultCount, compQuery.Apply(s.logger))
	s.queryServer = queryServer
	vaultServer := NewVaultServer(s.orch, s.cfgStore, s.factories, s.peerVaultStats, s.remoteChunkLister, s.remotePipelineBacklog, s.remoteChunkWatcher, s.remoteIndexer, s.remoteVaultValidator, s.remoteCloudIndexReconciler, s.localNodeID, s.logger)
	configServer := NewSystemServer(SystemServerConfig{
		Logger:              compServer.Apply(s.logger),
		Orch:                s.orch,
		CfgStore:            s.cfgStore,
		Factories:           s.factories,
		CertManager:         s.certManager,
		PeerStats:           s.peerIngesterStats,
		PeerRouteStats:      s.peerRouteStats,
		PeerStorageStats:    s.peerStorageStats,
		RemoteIngesterCheck: s.remoteIngesterCheck,
		ClusterTopology:     s.cluster,
		LocalStats:          s.localStatsFn,
		ClusterRouteRates:   s.clusterRouteRatesFn,
		LocalNodeID:         s.localNodeID,
		AfterConfigApply:    s.afterConfigApply,
		ConfigSignal:        s.configSignal,
		StatsSignal:         s.statsSignal,
		ResolveManagedFile:  s.ResolveManagedFileByID,
		CloudTesters:        s.cloudTesters,
		Tokens:              s.tokens,
		PlacementReconcile:  s.placementReconcile,
		OnTLSConfigChange:   s.reconfigureTLS,
		OnLookupConfigChange: func(cfg system.LookupConfig, mm system.MaxMindConfig) {
			s.applyLookupConfig(cfg, mm, lookupRegistry)
		},
		LogFilter:        s.logFilter,
		EnvironmentLabel: s.environmentLabel,
		EnvironmentColor: s.environmentColor,
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
		lifecycleServer.SetClusterRouteRates(s.clusterRouteRatesFn)
		lifecycleServer.SetPeerPipelineDisk(s.peerPipelineDisk)
	}
	lifecycleServer.SetVaultFuncs(vaultServer.allVaultInfos, func(ctx context.Context) *apiv1.GetStatsResponse {
		resp, _ := vaultServer.GetStats(ctx, connect.NewRequest(&apiv1.GetStatsRequest{}))
		if resp != nil {
			return resp.Msg
		}
		return nil
	})
	lifecycleServer.SetStorageFunc(func(ctx context.Context) []*apiv1.StorageState {
		storages, _ := configServer.allStorageStates(ctx)
		return storages
	})
	authServer := NewAuthServer(s.cfgStore, s.tokens, s.logger, s.noAuth)
	jobServer := NewJobServer(s.orch.Scheduler(), s.localNodeID, s.peerJobs, s.orch.Scheduler().Events())

	mux.Handle(gastrologv1connect.NewQueryServiceHandler(queryServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(vaultServer, handlerOpts...))
	// Hand the node-local ingester check to the cluster layer so peers can ask
	// this node for its own verdict.
	if s.registerIngesterCheck != nil {
		s.registerIngesterCheck(configServer.LocalIngesterCheck)
	}

	mux.Handle(gastrologv1connect.NewSystemServiceHandler(configServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewLifecycleServiceHandler(lifecycleServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(authServer, handlerOpts...))
	mux.Handle(gastrologv1connect.NewJobServiceHandler(jobServer, handlerOpts...))

	s.registerProbes(mux)
	s.registerBootstrapToken(mux)
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

// wrapMiddleware applies the serving middleware chain to a mux:
// tracking → CORS → security headers → rate limit → compression → mux.
// Every listener — HTTP, HTTPS, and the Unix socket — serves through this, so
// the security headers cover the embedded frontend as well as the RPC surface.
func (s *Server) wrapMiddleware(mux http.Handler) http.Handler {
	return s.trackingMiddleware(s.corsMiddleware(securityHeadersMiddleware(rateLimitMiddleware(s.rl)(compressMiddleware(s.logger, mux)))))
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
	s.mu.Lock()
	s.rlCancel = rlCancel
	s.mu.Unlock()
	s.rl.startCleanup(rlCtx, &s.rlWG, 3*time.Minute, 5*time.Minute)

	// Build the core handler once — reused by both HTTP and HTTPS.
	mux := s.buildMux()
	handler := s.wrapMiddleware(mux)
	s.mu.Lock()
	s.handler = handler
	s.mu.Unlock()

	// HTTP adds redirect-to-HTTPS + unencrypted HTTP/2 (h2c) alongside HTTP/1.
	redirectHandler := s.redirectMiddleware(handler)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := newTimedServer(redirectHandler, readHeaderTimeout, readTimeout, idleTimeout)
	// Protocols enables h2c without a separate http2.Server value: Go's
	// native HTTP/2 support reads this http.Server's own IdleTimeout field
	// (falling back to ReadTimeout) for the h2 idle timeout, so the timeouts
	// set above bind h2c connections too.
	srv.Protocols = &protocols
	s.mu.Lock()
	s.server = srv
	s.mu.Unlock()

	// Initial TLS config: start HTTPS if enabled
	s.reconfigureTLS()

	// Start Unix socket for local CLI access (no auth).
	if s.unixSocketConfig != "" {
		if err := s.ListenUnix(s.unixSocketConfig); err != nil {
			s.logger.Warn("unix socket failed, CLI will require --token", "error", err)
		}
	}

	s.logger.Info("server starting", "addr", listener.Addr().String())

	// Block on the local srv rather than s.server: it's the same value we
	// just assigned under s.mu, and reading it back here would mean
	// re-acquiring the lock for no reason.
	err := srv.Serve(listener)
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
		// This reflects the client-supplied Host header into the redirect
		// target unvalidated: an open redirect. A correct fix needs a
		// trusted-host decision (configured hostnames or TLS SANs) that
		// this middleware doesn't have; out of scope here.
		httpsURL := "https://" + host + ":" + port + r.URL.RequestURI()
		http.Redirect(w, r, httpsURL, http.StatusTemporaryRedirect) //nolint:gosec // G710: see comment above
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
	s.mu.Lock()
	rlCancel := s.rlCancel
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

	// Stop rate-limiter cleanup goroutine.
	if rlCancel != nil {
		rlCancel()
		s.rlWG.Wait()
	}

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
//
// It is the same chain the listeners serve, middleware included. A test
// that drove a thinner one could not see a security header that was never
// added, a CORS rule that was reordered, or a rate limit that stopped
// applying.
func (s *Server) Handler() http.Handler {
	return s.wrapMiddleware(s.buildMux())
}
