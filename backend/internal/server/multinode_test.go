package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	"gastrolog/internal/convert"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Multi-node test harness
//
// Creates N in-process nodes with separate orchestrators and vaults.
// The first node ID is the coordinator — its server handles HTTP requests
// and fans out to all other nodes via directRemoteSearcher.
//
// Nodes can optionally be created without a vault (e.g. a coordinator
// that only proxies requests to remote data nodes).
// ---------------------------------------------------------------------------

// multinodeTestNode represents a single in-process cluster node.
type multinodeTestNode struct {
	nodeID  string
	orch    *orchestrator.Orchestrator
	vaultID uuid.UUID    // zero if no vault
	vault   memtest.Vault // zero value if no vault
}

// multiNodeHarness holds the cluster of test nodes and the coordinator's
// server + clients.
type multiNodeHarness struct {
	coordinator        string // nodeID of the coordinator
	nodes              map[string]multinodeTestNode
	cfgStore           config.Store
	srv                *server.Server
	client             gastrologv1connect.QueryServiceClient
	vaultClient        gastrologv1connect.VaultServiceClient
	configClient       gastrologv1connect.ConfigServiceClient
	jobSrv             gastrologv1connect.JobServiceClient
	peerJobs           *mnPeerJobs
	peerRouteStats     *mnPeerRouteStats
	peerIngesterStats  *mnPeerIngesterStats
	peerVaultStats     *mnPeerVaultStats
}

// Node returns the test node by ID, fataling if not found.
func (h *multiNodeHarness) Node(t *testing.T, nodeID string) multinodeTestNode {
	t.Helper()
	n, ok := h.nodes[nodeID]
	if !ok {
		t.Fatalf("unknown node %q; have %v", nodeID, h.nodeIDs())
	}
	return n
}

func (h *multiNodeHarness) nodeIDs() []string {
	ids := make([]string, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	return ids
}

// mnOption configures the multi-node harness setup.
type mnOption func(*mnConfig)

type mnConfig struct {
	// noVault is a set of node IDs that should have no vault.
	noVault map[string]bool
}

// WithoutVault creates a node that has an orchestrator but no vault.
// Useful for testing a coordinator that only fans out to remote nodes.
func WithoutVault(nodeIDs ...string) mnOption {
	return func(c *mnConfig) {
		for _, id := range nodeIDs {
			c.noVault[id] = true
		}
	}
}

// setupMultiNode creates an N-node in-process cluster. The first nodeID
// is the coordinator (runs the server). All other nodes are remote —
// the coordinator's directRemoteSearcher calls into their orchestrators.
func setupMultiNode(t *testing.T, nodeIDs []string, opts ...mnOption) *multiNodeHarness {
	t.Helper()
	if len(nodeIDs) < 2 {
		t.Fatal("setupMultiNode requires at least 2 node IDs")
	}

	cfg := &mnConfig{noVault: make(map[string]bool)}
	for _, opt := range opts {
		opt(cfg)
	}

	coordinatorID := nodeIDs[0]
	nodes := make(map[string]multinodeTestNode, len(nodeIDs))
	cfgStore := cfgmem.NewStore()
	ctx := context.Background()

	// Create all nodes.
	for _, id := range nodeIDs {
		if cfg.noVault[id] {
			nodes[id] = setupMNNodeNoVault(t, id)
		} else {
			node := setupMNNode(t, id)
			// Create a tier assigned to this node (test-only manual assignment; production uses placement manager).
			tierID := uuid.Must(uuid.NewV7())
			_ = cfgStore.PutTier(ctx, config.TierConfig{
				ID:   tierID,
				Name: "tier-" + id,
				Type: config.TierTypeMemory,
				VaultID: node.vaultID, Position: 0,
				Placements: []config.TierPlacement{
					{StorageID: config.SyntheticStorageID(id), Leader: true},
				}, // test-only: placement manager assigns this in production
			})
			_ = cfgStore.PutVault(ctx, config.VaultConfig{
				ID: node.vaultID, Name: "vault-" + id,
			})
			nodes[id] = node
		}
	}

	// Build remote searcher: coordinator can reach all other nodes.
	remoteOrchestrators := make(map[string]*orchestrator.Orchestrator)
	for _, id := range nodeIDs {
		if id == coordinatorID {
			continue
		}
		remoteOrchestrators[id] = nodes[id].orch
	}
	remoteSearcher := &directRemoteSearcher{nodes: remoteOrchestrators}

	peerJobs := &mnPeerJobs{peers: map[string][]*gastrologv1.Job{}}

	// Collect remote orchestrators for peer route stats.
	peerRouteNodes := make(map[string]*orchestrator.Orchestrator)
	for _, id := range nodeIDs {
		if id != coordinatorID {
			peerRouteNodes[id] = nodes[id].orch
		}
	}
	peerRouteStats := &mnPeerRouteStats{nodes: peerRouteNodes}
	peerIngesterStats := &mnPeerIngesterStats{nodes: peerRouteNodes}
	peerVaultStats := &mnPeerVaultStats{nodes: peerRouteNodes}

	routingFwd := newDirectUnaryForwarder(nodes, cfgStore, coordinatorID)

	coordNode := nodes[coordinatorID]
	srv := server.New(coordNode.orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:            coordinatorID,
		RemoteSearcher:    remoteSearcher,
		RoutingForwarder:  routingFwd,
		PeerJobs:          peerJobs,
		PeerRouteStats:    peerRouteStats,
		PeerIngesterStats: peerIngesterStats,
		PeerVaultStats:    peerVaultStats,
	})

	handler := srv.Handler()
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	queryClient := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
	vaultClient := gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded")
	configClient := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")
	jobClient := gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded")

	t.Cleanup(func() {
		for _, n := range nodes {
			n.orch.Stop()
		}
	})

	return &multiNodeHarness{
		coordinator:       coordinatorID,
		nodes:             nodes,
		cfgStore:          cfgStore,
		srv:               srv,
		client:            queryClient,
		vaultClient:       vaultClient,
		configClient:      configClient,
		jobSrv:            jobClient,
		peerJobs:          peerJobs,
		peerRouteStats:    peerRouteStats,
		peerIngesterStats: peerIngesterStats,
		peerVaultStats:    peerVaultStats,
	}
}

func setupMNNode(t *testing.T, nodeID string) multinodeTestNode {
	t.Helper()

	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}

	v := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})

	vaultID := uuid.Must(uuid.NewV7())
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, v.CM, v.IM, v.QE))

	return multinodeTestNode{nodeID: nodeID, orch: orch, vaultID: vaultID, vault: v}
}

func setupMNNodeNoVault(t *testing.T, nodeID string) multinodeTestNode {
	t.Helper()

	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}

	return multinodeTestNode{nodeID: nodeID, orch: orch}
}

// mnPeerJobs provides jobs from simulated peer nodes.
type mnPeerJobs struct {
	peers map[string][]*gastrologv1.Job
}

func (p *mnPeerJobs) GetAll() map[string][]*gastrologv1.Job {
	return p.peers
}

// mnPeerRouteStats simulates aggregated route stats from peer nodes.
// Collects stats from all non-coordinator orchestrators.
type mnPeerRouteStats struct {
	nodes map[string]*orchestrator.Orchestrator // remote node orchs
}

func (p *mnPeerRouteStats) AggregateRouteStats() (ingested, dropped, routed int64, filterActive bool, vaultStats []*gastrologv1.VaultRouteStats, routeStats []*gastrologv1.PerRouteStats) {
	vaultMap := make(map[string]*gastrologv1.VaultRouteStats)
	routeMap := make(map[string]*gastrologv1.PerRouteStats)
	for _, orch := range p.nodes {
		rs := orch.GetRouteStats()
		ingested += rs.Ingested.Load()
		dropped += rs.Dropped.Load()
		routed += rs.Routed.Load()
		if orch.IsFilterSetActive() {
			filterActive = true
		}
		for vaultID, vs := range orch.VaultRouteStatsList() {
			id := vaultID.String()
			existing, ok := vaultMap[id]
			if !ok {
				vaultMap[id] = &gastrologv1.VaultRouteStats{
					VaultId:          id,
					RecordsMatched:   vs.Matched.Load(),
					RecordsForwarded: vs.Forwarded.Load(),
				}
			} else {
				existing.RecordsMatched += vs.Matched.Load()
				existing.RecordsForwarded += vs.Forwarded.Load()
			}
		}
		for routeID, ps := range orch.PerRouteStatsList() {
			id := routeID.String()
			existing, ok := routeMap[id]
			if !ok {
				routeMap[id] = &gastrologv1.PerRouteStats{
					RouteId:          id,
					RecordsMatched:   ps.Matched.Load(),
					RecordsForwarded: ps.Forwarded.Load(),
				}
			} else {
				existing.RecordsMatched += ps.Matched.Load()
				existing.RecordsForwarded += ps.Forwarded.Load()
			}
		}
	}
	for _, vs := range vaultMap {
		vaultStats = append(vaultStats, vs)
	}
	for _, rs := range routeMap {
		routeStats = append(routeStats, rs)
	}
	return
}

// mnPeerIngesterStats implements PeerIngesterStatsProvider by scanning all
// non-coordinator orchestrators for matching ingester stats.
type mnPeerIngesterStats struct {
	nodes map[string]*orchestrator.Orchestrator // remote node orchs
}

func (p *mnPeerIngesterStats) FindIngesterStats(ingesterID string) *gastrologv1.IngesterNodeStats {
	id, err := uuid.Parse(ingesterID)
	if err != nil {
		return nil
	}
	for _, orch := range p.nodes {
		stats := orch.GetIngesterStats(id)
		if stats == nil {
			continue
		}
		return &gastrologv1.IngesterNodeStats{
			Id:               ingesterID,
			Running:          orch.IsIngesterRunning(id),
			MessagesIngested: uint64(stats.MessagesIngested.Load()), //nolint:gosec
			BytesIngested:    uint64(stats.BytesIngested.Load()),    //nolint:gosec
			Errors:           uint64(stats.Errors.Load()),           //nolint:gosec
			Name:             orch.IngesterName(id),
		}
	}
	return nil
}

// mnPeerVaultStats implements PeerVaultStatsProvider by scanning all
// non-coordinator orchestrators for matching vault stats.
type mnPeerVaultStats struct {
	nodes map[string]*orchestrator.Orchestrator // remote node orchs
}

func (p *mnPeerVaultStats) FindVaultStats(vaultID string) *gastrologv1.VaultStats {
	id, err := uuid.Parse(vaultID)
	if err != nil {
		return nil
	}
	for _, orch := range p.nodes {
		for _, vid := range orch.ListVaults() {
			if vid != id {
				continue
			}
			metas, err := orch.ListChunkMetas(vid)
			if err != nil {
				return nil
			}
			stat := &gastrologv1.VaultStats{
				Id:         vaultID,
				ChunkCount: int64(len(metas)),
				Enabled:    orch.IsVaultEnabled(vid),
			}
			for _, meta := range metas {
				if meta.Sealed {
					stat.SealedChunks++
				} else {
					stat.ActiveChunks++
				}
				stat.RecordCount += meta.RecordCount
				stat.DataBytes += meta.Bytes
			}
			return stat
		}
	}
	return nil
}

// directRemoteSearcher calls directly into the target node's orchestrator,
// simulating ForwardSearch/ForwardFollow/ForwardExplain RPCs without gRPC.
type directRemoteSearcher struct {
	nodes map[string]*orchestrator.Orchestrator
}

func (d *directRemoteSearcher) Search(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}

	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, fmt.Errorf("invalid vault_id: %w", err)
	}

	// Match production behavior: only search primary tiers on this node.
	// Production ForwardSearch uses PrimaryTierQueryEngineForVault.
	eng := orch.PrimaryTierQueryEngineForVault(vaultID)
	if eng == nil {
		return &gastrologv1.ForwardSearchResponse{}, nil
	}

	q, pipeline, err := server.ParseExpression(req.GetQuery())
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	// Pipeline query: run locally and return table.
	if pipeline != nil && len(pipeline.Pipes) > 0 && !query.CanStreamPipeline(pipeline) {
		result, err := eng.RunPipeline(ctx, q, pipeline)
		if err != nil {
			return nil, err
		}
		if result.Table != nil {
			return &gastrologv1.ForwardSearchResponse{
				TableResult: server.TableResultToBasicProto(result.Table),
			}, nil
		}
	}

	// Compute histogram for this vault.
	histogram := eng.ComputeHistogram(ctx, q, 50)
	var histProto []*gastrologv1.HistogramBucket
	for _, b := range histogram {
		histProto = append(histProto, &gastrologv1.HistogramBucket{
			TimestampMs: b.TimestampMs,
			Count:       b.Count,
			GroupCounts: b.GroupCounts,
		})
	}

	// Regular search.
	searchIter, _ := eng.Search(ctx, q, nil)

	var records []*gastrologv1.ExportRecord
	for rec, err := range searchIter {
		if err != nil {
			return nil, err
		}
		records = append(records, convert.RecordToExport(rec))
	}
	return &gastrologv1.ForwardSearchResponse{
		Records:   records,
		Histogram: histProto,
	}, nil
}

func (d *directRemoteSearcher) SearchStream(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (
	<-chan []*gastrologv1.ExportRecord,
	[]*gastrologv1.HistogramBucket,
	*gastrologv1.TableResult,
	<-chan error,
	func() []byte,
) {
	recCh := make(chan []*gastrologv1.ExportRecord, 16)
	errCh := make(chan error, 1)

	orch, ok := d.nodes[nodeID]
	if !ok {
		errCh <- fmt.Errorf("unknown node: %s", nodeID)
		close(recCh)
		close(errCh)
		return recCh, nil, nil, errCh, func() []byte { return nil }
	}

	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		errCh <- fmt.Errorf("invalid vault_id: %w", err)
		close(recCh)
		close(errCh)
		return recCh, nil, nil, errCh, func() []byte { return nil }
	}

	// Match production behavior: only search primary tiers on this node.
	eng := orch.PrimaryTierQueryEngineForVault(vaultID)
	if eng == nil {
		close(recCh)
		close(errCh)
		return recCh, nil, nil, errCh, func() []byte { return nil }
	}

	q, pipeline, parseErr := server.ParseExpression(req.GetQuery())
	if parseErr != nil {
		errCh <- fmt.Errorf("parse: %w", parseErr)
		close(recCh)
		close(errCh)
		return recCh, nil, nil, errCh, func() []byte { return nil }
	}

	// Pipeline query: return table result synchronously.
	if pipeline != nil && len(pipeline.Pipes) > 0 && !query.CanStreamPipeline(pipeline) {
		result, runErr := eng.RunPipeline(ctx, q, pipeline)
		if runErr != nil {
			errCh <- runErr
			close(recCh)
			close(errCh)
			return recCh, nil, nil, errCh, func() []byte { return nil }
		}
		if result.Table != nil {
			close(recCh)
			close(errCh)
			return recCh, nil, server.TableResultToBasicProto(result.Table), errCh, func() []byte { return nil }
		}
	}

	// Compute histogram.
	histogram := eng.ComputeHistogram(ctx, q, 50)
	var histProto []*gastrologv1.HistogramBucket
	for _, b := range histogram {
		histProto = append(histProto, &gastrologv1.HistogramBucket{
			TimestampMs: b.TimestampMs,
			Count:       b.Count,
			GroupCounts: b.GroupCounts,
		})
	}

	// Stream records in batches.
	go func() {
		defer close(recCh)
		defer close(errCh)

		searchIter, _ := eng.Search(ctx, q, nil)
		const batchSize = 200
		batch := make([]*gastrologv1.ExportRecord, 0, batchSize)
		for rec, iterErr := range searchIter {
			if iterErr != nil {
				errCh <- iterErr
				return
			}
			batch = append(batch, convert.RecordToExport(rec))
			if len(batch) >= batchSize {
				select {
				case recCh <- batch:
				case <-ctx.Done():
					return
				}
				batch = make([]*gastrologv1.ExportRecord, 0, batchSize)
			}
		}
		if len(batch) > 0 {
			select {
			case recCh <- batch:
			case <-ctx.Done():
			}
		}
	}()

	return recCh, histProto, nil, errCh, func() []byte { return nil }
}

func (d *directRemoteSearcher) GetContext(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}

	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, fmt.Errorf("invalid vault_id: %w", err)
	}
	chunkID, err := chunk.ParseChunkID(req.GetChunkId())
	if err != nil {
		return nil, fmt.Errorf("invalid chunk_id: %w", err)
	}

	eng := orch.MultiVaultQueryEngine()
	result, err := eng.GetContext(ctx, query.ContextRef{
		VaultID: vaultID,
		ChunkID: chunkID,
		Pos:     req.GetPos(),
	}, int(req.GetBefore()), int(req.GetAfter()))
	if err != nil {
		return nil, err
	}

	resp := &gastrologv1.ForwardGetContextResponse{
		Anchor: convert.RecordToExport(result.Anchor),
		Before: make([]*gastrologv1.ExportRecord, len(result.Before)),
		After:  make([]*gastrologv1.ExportRecord, len(result.After)),
	}
	for i, rec := range result.Before {
		resp.Before[i] = convert.RecordToExport(rec)
	}
	for i, rec := range result.After {
		resp.After[i] = convert.RecordToExport(rec)
	}
	return resp, nil
}

func (d *directRemoteSearcher) Explain(ctx context.Context, nodeID string, req *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}

	var allChunks []*gastrologv1.ChunkPlan
	var totalChunks int32

	for _, vaultStr := range req.GetVaultIds() {
		scopedExpr := fmt.Sprintf("vault_id=%s %s", vaultStr, req.GetQuery())
		q, _, err := server.ParseExpression(scopedExpr)
		if err != nil {
			return nil, fmt.Errorf("parse query for vault %s: %w", vaultStr, err)
		}

		eng := orch.MultiVaultQueryEngine()
		plan, err := eng.Explain(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("explain vault %s: %w", vaultStr, err)
		}

		totalChunks += int32(plan.TotalChunks) //nolint:gosec // G115: chunk count fits in int32
		for _, cp := range plan.ChunkPlans {
			chunkPlan := &gastrologv1.ChunkPlan{
				VaultId:          cp.VaultID.String(),
				ChunkId:          cp.ChunkID.String(),
				Sealed:           cp.Sealed,
				RecordCount:      int64(cp.RecordCount),
				ScanMode:         cp.ScanMode,
				EstimatedRecords: int64(cp.EstimatedScan),
				RuntimeFilters:   []string{cp.RuntimeFilter},
				Steps:            server.PipelineStepsToProto(cp.Pipeline),
				SkipReason:       cp.SkipReason,
				NodeId:           nodeID,
			}
			if !cp.WriteStart.IsZero() {
				chunkPlan.WriteStart = timestamppb.New(cp.WriteStart)
			}
			if !cp.WriteEnd.IsZero() {
				chunkPlan.WriteEnd = timestamppb.New(cp.WriteEnd)
			}
			for _, bp := range cp.BranchPlans {
				chunkPlan.BranchPlans = append(chunkPlan.BranchPlans, &gastrologv1.BranchPlan{
					Expression:       bp.BranchExpr,
					Steps:            server.PipelineStepsToProto(bp.Pipeline),
					Skipped:          bp.Skipped,
					SkipReason:       bp.SkipReason,
					EstimatedRecords: int64(bp.EstimatedScan),
				})
			}
			allChunks = append(allChunks, chunkPlan)
		}
	}

	return &gastrologv1.ForwardExplainResponse{
		Chunks:      allChunks,
		TotalChunks: totalChunks,
	}, nil
}

func (d *directRemoteSearcher) Follow(ctx context.Context, nodeID string, req *gastrologv1.ForwardFollowRequest) (<-chan *gastrologv1.ExportRecord, <-chan error) {
	recCh := make(chan *gastrologv1.ExportRecord, 64)
	errCh := make(chan error, 1)

	orch, ok := d.nodes[nodeID]
	if !ok {
		errCh <- fmt.Errorf("unknown node: %s", nodeID)
		close(errCh)
		close(recCh)
		return recCh, errCh
	}

	// Build a scoped query for the requested vaults.
	var scopedExpr string
	for _, vid := range req.GetVaultIds() {
		if scopedExpr != "" {
			scopedExpr += " OR "
		}
		scopedExpr += "vault_id=" + vid
	}
	if req.GetQuery() != "" {
		if len(req.GetVaultIds()) > 1 {
			scopedExpr = "(" + scopedExpr + ") " + req.GetQuery()
		} else {
			scopedExpr += " " + req.GetQuery()
		}
	}

	q, _, err := server.ParseExpression(scopedExpr)
	if err != nil {
		errCh <- fmt.Errorf("parse query: %w", err)
		close(errCh)
		close(recCh)
		return recCh, errCh
	}

	// Use the parent ctx (which already has a timeout from the caller).
	// A short secondary timeout risks closing before records arrive.
	followCtx, cancel := context.WithTimeout(ctx, 2*time.Second)

	eng := orch.MultiVaultQueryEngine()
	followIter := eng.Follow(followCtx, q)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		defer close(recCh)
		for rec, iterErr := range followIter {
			if iterErr != nil {
				errCh <- iterErr
				return
			}
			select {
			case recCh <- convert.RecordToExport(rec):
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	return recCh, errCh
}

func (d *directRemoteSearcher) ExportToVault(_ context.Context, _ string, _ *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error) {
	return &gastrologv1.ForwardExportToVaultResponse{JobId: "test-export-job"}, nil
}

// directUnaryForwarder implements routing.UnaryForwarder for multi-node tests
// by dispatching through in-process Connect muxes on each remote node.
type directUnaryForwarder struct {
	handlers map[string]http.Handler // nodeID → Connect mux handler
}

func newDirectUnaryForwarder(nodes map[string]multinodeTestNode, cfgStore config.Store, coordinatorID string) *directUnaryForwarder {
	handlers := make(map[string]http.Handler)
	for id, node := range nodes {
		if id == coordinatorID {
			continue
		}
		// BuildInternalHandler returns a mux with NoAuthInterceptor and
		// NO routing interceptor — same as the real ForwardRPC dispatch path.
		remoteSrv := server.New(node.orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
			NodeID: id,
			NoAuth: true,
		})
		handlers[id] = remoteSrv.BuildInternalHandler()
	}
	return &directUnaryForwarder{handlers: handlers}
}

func (d *directUnaryForwarder) ForwardUnary(ctx context.Context, nodeID, procedure string, reqPayload []byte) ([]byte, error) {
	handler, ok := d.handlers[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}

	// Build an HTTP request exactly as the real ForwardRPC handler does.
	httpReq, err := http.NewRequestWithContext(ctx, "POST", procedure, bytes.NewReader(reqPayload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/proto")
	httpReq.Header.Set("Connect-Protocol-Version", "1")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httpReq)

	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBuf [4096]byte
		n, _ := resp.Body.Read(errBuf[:])
		return nil, fmt.Errorf("forward to %s: HTTP %d: %s", nodeID, resp.StatusCode, string(errBuf[:n]))
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func addMNRecords(t *testing.T, node multinodeTestNode, prefix string, count int, attrs map[string]string) {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range count {
		ts := t0.Add(time.Duration(i) * time.Second)
		node.vault.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "%s-%d", prefix, i),
			Attrs:    attrs,
		})
	}
}

// searchAll sends a Search RPC and collects all streamed records.
func searchAll(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string) []*gastrologv1.Record {
	t.Helper()
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var records []*gastrologv1.Record
	for stream.Receive() {
		records = append(records, stream.Msg().Records...)
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}
	return records
}

// searchTable sends a Search RPC and returns the table result (for pipeline queries).
func searchTable(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string) *gastrologv1.TableResult {
	t.Helper()
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var table *gastrologv1.TableResult
	for stream.Receive() {
		if stream.Msg().TableResult != nil {
			table = stream.Msg().TableResult
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}
	return table
}

// tableToMap converts a table result into a map keyed by the given column,
// with values from the value column. Useful for asserting stats results.
func tableToMap(t *testing.T, table *gastrologv1.TableResult, keyCol, valCol string) map[string]string {
	t.Helper()
	if table == nil {
		t.Fatal("expected table result, got nil")
	}
	keyIdx, valIdx := -1, -1
	for i, col := range table.Columns {
		if col == keyCol {
			keyIdx = i
		}
		if col == valCol {
			valIdx = i
		}
	}
	if keyIdx < 0 || valIdx < 0 {
		t.Fatalf("expected columns %q and %q, got %v", keyCol, valCol, table.Columns)
	}
	m := make(map[string]string, len(table.Rows))
	for _, row := range table.Rows {
		m[row.Values[keyIdx]] = row.Values[valIdx]
	}
	return m
}

// ---------------------------------------------------------------------------
// Tests — 2-node (coordinator with data + one remote)
// ---------------------------------------------------------------------------

func TestMultiNode_SearchFanOut(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	addMNRecords(t, h.Node(t, "node-A"), "A", 5, nil)
	addMNRecords(t, h.Node(t, "node-B"), "B", 5, nil)

	records := searchAll(t, h.client, "")
	if len(records) != 10 {
		t.Errorf("expected 10 records from both nodes, got %d", len(records))
	}

	var aCount, bCount int
	for _, r := range records {
		raw := string(r.Raw)
		if strings.HasPrefix(raw, "A-") {
			aCount++
		} else if strings.HasPrefix(raw, "B-") {
			bCount++
		}
	}
	if aCount != 5 {
		t.Errorf("expected 5 A-records, got %d", aCount)
	}
	if bCount != 5 {
		t.Errorf("expected 5 B-records, got %d", bCount)
	}
}

func TestMultiNode_PipelineStatsDistributed(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	addMNRecords(t, h.Node(t, "node-A"), "A", 3, map[string]string{"level": "error"})
	addMNRecords(t, h.Node(t, "node-A"), "A-info", 2, map[string]string{"level": "info"})
	addMNRecords(t, h.Node(t, "node-B"), "B", 4, map[string]string{"level": "error"})
	addMNRecords(t, h.Node(t, "node-B"), "B-info", 1, map[string]string{"level": "info"})

	table := searchTable(t, h.client, "| stats count by level")
	counts := tableToMap(t, table, "level", "count")

	if counts["error"] != "7" {
		t.Errorf("expected error count 7, got %q", counts["error"])
	}
	if counts["info"] != "3" {
		t.Errorf("expected info count 3, got %q", counts["info"])
	}
}

func TestMultiNode_NonDistributiveAvgGlobal(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	// Node A: values 10, 20 → per-node avg=15.
	// Node B: values 30 → per-node avg=30.
	// Wrong (distributed sum of avgs): 15+30=45.
	// Correct (global avg): (10+20+30)/3 = 20.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	nodeA := h.Node(t, "node-A")
	nodeB := h.Node(t, "node-B")
	for i, v := range []int{10, 20} {
		ts := t0.Add(time.Duration(i) * time.Second)
		nodeA.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "val=%d", v),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}
	for i, v := range []int{30} {
		ts := t0.Add(time.Duration(i+10) * time.Second)
		nodeB.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "val=%d", v),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}

	table := searchTable(t, h.client, "| stats avg(val)")
	if table == nil {
		t.Fatal("expected table result from avg query")
	}

	for i, col := range table.Columns {
		if strings.Contains(col, "avg") {
			if len(table.Rows) > 0 {
				val := table.Rows[0].Values[i]
				if val != "20" {
					t.Errorf("expected avg(val) = 20 (global), got %q", val)
				}
			}
		}
	}
}

func TestMultiNode_PipelineGlobalHistogram(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	addMNRecords(t, h.Node(t, "node-A"), "A", 3, nil)
	addMNRecords(t, h.Node(t, "node-B"), "B", 4, nil)

	// Pipeline with tail (non-table, goes through searchPipelineGlobal).
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "| tail 10"},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	var histogram []*gastrologv1.HistogramBucket
	var recordCount int
	for stream.Receive() {
		recordCount += len(stream.Msg().Records)
		if len(stream.Msg().Histogram) > 0 {
			histogram = stream.Msg().Histogram
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if recordCount != 7 {
		t.Errorf("expected 7 records, got %d", recordCount)
	}
	if len(histogram) == 0 {
		t.Error("expected histogram in pipeline global response, got none")
	}
}

func TestMultiNode_GetJobCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	mockPeers := &mnPeerJobs{peers: map[string][]*gastrologv1.Job{
		"node-B": {
			{Id: "job-on-B", Name: "compact-B", NodeId: "node-B",
				Kind: gastrologv1.JobKind_JOB_KIND_SCHEDULED},
		},
	}}
	jobSrv := server.NewJobServer(h.Node(t, "node-A").orch.Scheduler(), "node-A", mockPeers)

	resp, err := jobSrv.GetJob(context.Background(), connect.NewRequest(&gastrologv1.GetJobRequest{Id: "job-on-B"}))
	if err != nil {
		t.Fatalf("GetJob for peer job: %v", err)
	}
	if resp.Msg.Job.NodeId != "node-B" {
		t.Errorf("expected NodeId node-B, got %q", resp.Msg.Job.NodeId)
	}
}

// ---------------------------------------------------------------------------
// Tests — 3-node (coordinator without data + two remote data nodes)
// ---------------------------------------------------------------------------

func TestMultiNode_CoordinatorOnlyFanOut(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 4, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 6, nil)

	// Coordinator has no vault — all 10 records come from remote nodes.
	records := searchAll(t, h.client, "")
	if len(records) != 10 {
		t.Errorf("expected 10 records from two remote nodes, got %d", len(records))
	}

	var d1Count, d2Count int
	for _, r := range records {
		raw := string(r.Raw)
		if strings.HasPrefix(raw, "D1-") {
			d1Count++
		} else if strings.HasPrefix(raw, "D2-") {
			d2Count++
		}
	}
	if d1Count != 4 {
		t.Errorf("expected 4 D1-records, got %d", d1Count)
	}
	if d2Count != 6 {
		t.Errorf("expected 6 D2-records, got %d", d2Count)
	}
}

func TestMultiNode_StatsAcrossTwoRemoteNodes(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, map[string]string{"level": "warn"})
	addMNRecords(t, h.Node(t, "data-2"), "D2", 3, map[string]string{"level": "warn"})
	addMNRecords(t, h.Node(t, "data-2"), "D2-err", 2, map[string]string{"level": "error"})

	table := searchTable(t, h.client, "| stats count by level")
	counts := tableToMap(t, table, "level", "count")

	// warn=8 (5+3), error=2.
	if counts["warn"] != "8" {
		t.Errorf("expected warn count 8, got %q", counts["warn"])
	}
	if counts["error"] != "2" {
		t.Errorf("expected error count 2, got %q", counts["error"])
	}
}

func TestMultiNode_AvgAcrossTwoRemoteNodes(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	// data-1: val=10, val=20 → local avg=15
	// data-2: val=30, val=40 → local avg=35
	// Correct global avg: (10+20+30+40)/4 = 25
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	d1 := h.Node(t, "data-1")
	d2 := h.Node(t, "data-2")
	for i, v := range []int{10, 20} {
		ts := t0.Add(time.Duration(i) * time.Second)
		d1.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "val=%d", v),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}
	for i, v := range []int{30, 40} {
		ts := t0.Add(time.Duration(i+10) * time.Second)
		d2.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "val=%d", v),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}

	table := searchTable(t, h.client, "| stats avg(val)")
	if table == nil {
		t.Fatal("expected table result from avg query")
	}
	for i, col := range table.Columns {
		if strings.Contains(col, "avg") && len(table.Rows) > 0 {
			val := table.Rows[0].Values[i]
			if val != "25" {
				t.Errorf("expected avg(val) = 25 (global across 2 remote nodes), got %q", val)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Tests — Vault admin RPC forwarding to remote nodes
// ---------------------------------------------------------------------------

func TestMultiNode_ListChunksLocal(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	// ListChunks is RouteLocal — must call from the node that has the tier.
	dataNode := h.Node(t, "data-1")
	vaultID := dataNode.vaultID.String()
	metas, err := dataNode.orch.ListAllChunkMetas(dataNode.vaultID)
	if err != nil {
		t.Fatalf("ListAllChunkMetas: %v", err)
	}
	if len(metas) == 0 {
		t.Fatalf("expected at least 1 chunk on data-1 for vault %s", vaultID)
	}
}

func TestMultiNode_GetChunkRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	dataNode := h.Node(t, "data-1")
	remoteVaultID := dataNode.vaultID.String()

	// Get chunk ID from the data node's orchestrator (ListChunks is RouteLocal).
	metas, err := dataNode.orch.ListChunkMetas(dataNode.vaultID)
	if err != nil {
		t.Fatalf("ListChunkMetas: %v", err)
	}
	if len(metas) == 0 {
		t.Fatal("no chunks to test GetChunk with")
	}
	chunkID := metas[0].ID.String()

	resp, err := h.vaultClient.GetChunk(context.Background(), connect.NewRequest(&gastrologv1.GetChunkRequest{
		Vault:   remoteVaultID,
		ChunkId: chunkID,
	}))
	if err != nil {
		t.Fatalf("GetChunk on remote vault: %v", err)
	}
	if resp.Msg.Chunk == nil {
		t.Error("expected chunk metadata, got nil")
	}
	if resp.Msg.Chunk.Id != chunkID {
		t.Errorf("expected chunk ID %q, got %q", chunkID, resp.Msg.Chunk.Id)
	}
	if resp.Msg.Chunk.RecordCount != 3 {
		t.Errorf("expected 3 records in chunk, got %d", resp.Msg.Chunk.RecordCount)
	}
}

func TestMultiNode_AnalyzeChunkRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()
	// Memory vaults don't produce full analysis data, but the RPC should
	// succeed — proving the forwarding path works end-to-end.
	_, err := h.vaultClient.AnalyzeChunk(context.Background(), connect.NewRequest(&gastrologv1.AnalyzeChunkRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("AnalyzeChunk on remote vault: %v", err)
	}
}

func TestMultiNode_SealVaultRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()

	// Seal the remote vault's active chunk.
	_, err := h.vaultClient.SealVault(context.Background(), connect.NewRequest(&gastrologv1.SealVaultRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("SealVault on remote vault: %v", err)
	}

	// Verify the chunk is now sealed (query the data node directly since ListChunks is RouteLocal).
	dataNode := h.Node(t, "data-1")
	metas, err := dataNode.orch.ListChunkMetas(dataNode.vaultID)
	if err != nil {
		t.Fatalf("ListChunkMetas after seal: %v", err)
	}
	sealedCount := 0
	for _, m := range metas {
		if m.Sealed {
			sealedCount++
		}
	}
	if sealedCount == 0 {
		t.Error("expected at least 1 sealed chunk after SealVault, got 0")
	}
}

func TestMultiNode_ValidateVaultRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()
	resp, err := h.vaultClient.ValidateVault(context.Background(), connect.NewRequest(&gastrologv1.ValidateVaultRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("ValidateVault on remote vault: %v", err)
	}
	if !resp.Msg.Valid {
		t.Error("expected vault to be valid")
	}
}

func TestMultiNode_GetVaultRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()
	resp, err := h.vaultClient.GetVault(context.Background(), connect.NewRequest(&gastrologv1.GetVaultRequest{
		Id: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("GetVault on remote vault: %v", err)
	}
	if resp.Msg.Vault == nil {
		t.Fatal("expected vault info, got nil")
	}
	if resp.Msg.Vault.Id != remoteVaultID {
		t.Errorf("expected vault ID %q, got %q", remoteVaultID, resp.Msg.Vault.Id)
	}
	if !resp.Msg.Vault.Remote {
		t.Error("expected Remote=true for remote vault")
	}
	if resp.Msg.Vault.Name != "vault-data-1" {
		t.Errorf("expected name %q, got %q", "vault-data-1", resp.Msg.Vault.Name)
	}
}

func TestMultiNode_ReindexVaultRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	remoteVaultID := h.Node(t, "data-1").vaultID.String()
	resp, err := h.vaultClient.ReindexVault(context.Background(), connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("ReindexVault on remote vault: %v", err)
	}
	if resp.Msg.JobId == "" {
		t.Error("expected non-empty job ID from remote reindex")
	}
}

// ---------------------------------------------------------------------------
// Tests — Route stats aggregated across cluster nodes
// ---------------------------------------------------------------------------

func TestMultiNode_RouteStatsAggregated(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	ctx := context.Background()

	// Set up catch-all routes on data nodes so ingest() routes records.
	d1 := h.Node(t, "data-1")
	d2 := h.Node(t, "data-2")

	d1.orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: d1.vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))
	d2.orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: d2.vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))

	// Ingest records on each data node (simulating ingesters on those nodes).
	for range 3 {
		if err := d1.orch.Ingest(chunk.Record{Raw: []byte("from-d1")}); err != nil {
			t.Fatalf("Ingest on data-1: %v", err)
		}
	}
	for range 7 {
		if err := d2.orch.Ingest(chunk.Record{Raw: []byte("from-d2")}); err != nil {
			t.Fatalf("Ingest on data-2: %v", err)
		}
	}

	// Query route stats via the coordinator — should aggregate both data nodes.
	resp, err := h.configClient.GetRouteStats(ctx, connect.NewRequest(&gastrologv1.GetRouteStatsRequest{}))
	if err != nil {
		t.Fatalf("GetRouteStats: %v", err)
	}

	if resp.Msg.TotalIngested != 10 {
		t.Errorf("TotalIngested = %d, want 10 (3+7)", resp.Msg.TotalIngested)
	}
	if resp.Msg.TotalRouted != 10 {
		t.Errorf("TotalRouted = %d, want 10", resp.Msg.TotalRouted)
	}
	if resp.Msg.TotalDropped != 0 {
		t.Errorf("TotalDropped = %d, want 0", resp.Msg.TotalDropped)
	}
	if !resp.Msg.FilterSetActive {
		t.Error("expected FilterSetActive=true")
	}
	if len(resp.Msg.VaultStats) != 2 {
		t.Fatalf("expected 2 vault stats, got %d", len(resp.Msg.VaultStats))
	}

	// Build a map for easier assertions.
	vsMap := make(map[string]int64)
	for _, vs := range resp.Msg.VaultStats {
		vsMap[vs.VaultId] = vs.RecordsMatched
	}
	if vsMap[d1.vaultID.String()] != 3 {
		t.Errorf("data-1 vault matched = %d, want 3", vsMap[d1.vaultID.String()])
	}
	if vsMap[d2.vaultID.String()] != 7 {
		t.Errorf("data-2 vault matched = %d, want 7", vsMap[d2.vaultID.String()])
	}
}

func TestMultiNode_PerRouteStatsAggregated(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	ctx := context.Background()

	d1 := h.Node(t, "data-1")
	d2 := h.Node(t, "data-2")

	// Create two distinct route IDs.
	routeA := uuid.New()
	routeB := uuid.New()

	// data-1: route A catches everything.
	d1.orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: d1.vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*", RouteID: routeA},
	}))
	// data-2: route B catches everything.
	d2.orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: d2.vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*", RouteID: routeB},
	}))

	for range 5 {
		if err := d1.orch.Ingest(chunk.Record{Raw: []byte("from-d1")}); err != nil {
			t.Fatalf("Ingest on data-1: %v", err)
		}
	}
	for range 8 {
		if err := d2.orch.Ingest(chunk.Record{Raw: []byte("from-d2")}); err != nil {
			t.Fatalf("Ingest on data-2: %v", err)
		}
	}

	resp, err := h.configClient.GetRouteStats(ctx, connect.NewRequest(&gastrologv1.GetRouteStatsRequest{}))
	if err != nil {
		t.Fatalf("GetRouteStats: %v", err)
	}

	if resp.Msg.TotalIngested != 13 {
		t.Errorf("TotalIngested = %d, want 13", resp.Msg.TotalIngested)
	}

	// Should have 2 per-route entries.
	if len(resp.Msg.RouteStats) != 2 {
		t.Fatalf("expected 2 route stats, got %d", len(resp.Msg.RouteStats))
	}

	rsMap := make(map[string]int64)
	for _, rs := range resp.Msg.RouteStats {
		rsMap[rs.RouteId] = rs.RecordsMatched
	}
	if rsMap[routeA.String()] != 5 {
		t.Errorf("route A matched = %d, want 5", rsMap[routeA.String()])
	}
	if rsMap[routeB.String()] != 8 {
		t.Errorf("route B matched = %d, want 8", rsMap[routeB.String()])
	}
}

// ---------------------------------------------------------------------------
// Tests — Query fan-out (Explain, GetContext, Follow, GetFields)
// ---------------------------------------------------------------------------

func TestMultiNode_ExplainCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 4, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 6, nil)

	resp, err := h.client.Explain(context.Background(), connect.NewRequest(&gastrologv1.ExplainRequest{
		Query: &gastrologv1.Query{Expression: ""},
	}))
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if resp.Msg.TotalChunks < 2 {
		t.Errorf("expected at least 2 chunks (one per data node), got %d", resp.Msg.TotalChunks)
	}

	// Verify chunks span both nodes.
	nodesSeen := make(map[string]bool)
	for _, cp := range resp.Msg.Chunks {
		nodesSeen[cp.NodeId] = true
	}
	if !nodesSeen["data-1"] {
		t.Error("expected chunk plan from data-1")
	}
	if !nodesSeen["data-2"] {
		t.Error("expected chunk plan from data-2")
	}
}

func TestMultiNode_GetContextCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 10, nil)

	// First, search to get a record ref from the remote node.
	records := searchAll(t, h.client, "")
	if len(records) == 0 {
		t.Fatal("expected records from data-1")
	}

	// Pick a record in the middle for meaningful before/after context.
	mid := records[len(records)/2]
	if mid.Ref == nil {
		t.Fatal("expected record ref")
	}

	resp, err := h.client.GetContext(context.Background(), connect.NewRequest(&gastrologv1.GetContextRequest{
		Ref:    mid.Ref,
		Before: 3,
		After:  3,
	}))
	if err != nil {
		t.Fatalf("GetContext: %v", err)
	}
	if resp.Msg.Anchor == nil {
		t.Fatal("expected anchor record")
	}
	if len(resp.Msg.Before) == 0 && len(resp.Msg.After) == 0 {
		t.Error("expected at least some before/after context records")
	}
}

func TestMultiNode_FollowCrossNode(t *testing.T) {
	t.Parallel()
	// Test the directRemoteSearcher.Follow forwarder directly, since the
	// embedded transport doesn't support long-lived streaming (no http.Flusher).
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	vaultID := h.Node(t, "data-1").vaultID.String()

	// Call the directRemoteSearcher's Follow directly.
	searcher := &directRemoteSearcher{nodes: map[string]*orchestrator.Orchestrator{
		"data-1": h.Node(t, "data-1").orch,
	}}
	followRecCh, followErrCh := searcher.Follow(ctx, "data-1", &gastrologv1.ForwardFollowRequest{
		VaultIds: []string{vaultID},
	})

	// Add records after follow starts.
	go func() {
		time.Sleep(200 * time.Millisecond)
		node := h.Node(t, "data-1")
		t0 := time.Now()
		for i := range 5 {
			ts := t0.Add(time.Duration(i) * time.Millisecond)
			node.vault.CM.Append(chunk.Record{
				IngestTS: ts,
				WriteTS:  ts,
				Raw:      fmt.Appendf(nil, "follow-%d", i),
			})
		}
	}()

	var records []*gastrologv1.ExportRecord
	for rec := range followRecCh {
		records = append(records, rec)
		if len(records) >= 5 {
			break
		}
	}
	// Drain errors.
	for err := range followErrCh {
		if err != nil {
			t.Errorf("follow error: %v", err)
		}
	}

	if len(records) < 5 {
		t.Errorf("expected at least 5 follow records from remote node, got %d", len(records))
	}
}

func TestMultiNode_GetFieldsCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, map[string]string{"source": "syslog"})
	addMNRecords(t, h.Node(t, "data-2"), "D2", 5, map[string]string{"source": "mqtt", "region": "us-east"})

	resp, err := h.client.GetFields(context.Background(), connect.NewRequest(&gastrologv1.GetFieldsRequest{
		Expression: "",
		MaxSamples: 100,
	}))
	if err != nil {
		t.Fatalf("GetFields: %v", err)
	}

	// Check attr fields contain keys from both nodes.
	attrKeys := make(map[string]bool)
	for _, f := range resp.Msg.AttrFields {
		attrKeys[f.Key] = true
	}
	if !attrKeys["source"] {
		t.Error("expected 'source' attr field (present on both nodes)")
	}
	if !attrKeys["region"] {
		t.Error("expected 'region' attr field (only on data-2)")
	}

	// "source" should have values from both nodes.
	for _, f := range resp.Msg.AttrFields {
		if f.Key == "source" && f.Count != 10 {
			t.Errorf("expected source count=10 (5+5), got %d", f.Count)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests — Vault stats and info across cluster nodes
// ---------------------------------------------------------------------------

func TestMultiNode_GetStatsRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 8, nil)

	resp, err := h.vaultClient.GetStats(context.Background(), connect.NewRequest(&gastrologv1.GetStatsRequest{}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalVaults != 2 {
		t.Errorf("TotalVaults = %d, want 2", resp.Msg.TotalVaults)
	}
	if resp.Msg.TotalRecords != 13 {
		t.Errorf("TotalRecords = %d, want 13 (5+8)", resp.Msg.TotalRecords)
	}
	if resp.Msg.TotalChunks < 2 {
		t.Errorf("TotalChunks = %d, want at least 2", resp.Msg.TotalChunks)
	}
	if len(resp.Msg.VaultStats) != 2 {
		t.Errorf("expected 2 vault stats, got %d", len(resp.Msg.VaultStats))
	}
}

func TestMultiNode_GetStatsForSpecificRemoteVault(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 5, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 8, nil)

	// Request stats for just one remote vault.
	remoteVaultID := h.Node(t, "data-2").vaultID.String()
	resp, err := h.vaultClient.GetStats(context.Background(), connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalRecords != 8 {
		t.Errorf("TotalRecords = %d, want 8 (only data-2)", resp.Msg.TotalRecords)
	}
	if len(resp.Msg.VaultStats) != 1 {
		t.Fatalf("expected 1 vault stat, got %d", len(resp.Msg.VaultStats))
	}
	if resp.Msg.VaultStats[0].Id != remoteVaultID {
		t.Errorf("expected vault %s, got %s", remoteVaultID, resp.Msg.VaultStats[0].Id)
	}
}

func TestMultiNode_ListVaultsCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 7, nil)

	resp, err := h.vaultClient.ListVaults(context.Background(), connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}

	if len(resp.Msg.Vaults) != 2 {
		t.Fatalf("expected 2 vaults, got %d", len(resp.Msg.Vaults))
	}

	vaultMap := make(map[string]*gastrologv1.VaultInfo)
	for _, v := range resp.Msg.Vaults {
		vaultMap[v.Id] = v
	}

	d1Vault := vaultMap[h.Node(t, "data-1").vaultID.String()]
	d2Vault := vaultMap[h.Node(t, "data-2").vaultID.String()]

	if d1Vault == nil || d2Vault == nil {
		t.Fatal("expected both data node vaults in listing")
	}
	if !d1Vault.Remote || !d2Vault.Remote {
		t.Error("expected both vaults to be marked Remote=true from coordinator")
	}
	if d1Vault.RecordCount != 3 {
		t.Errorf("data-1 vault records = %d, want 3", d1Vault.RecordCount)
	}
	if d2Vault.RecordCount != 7 {
		t.Errorf("data-2 vault records = %d, want 7", d2Vault.RecordCount)
	}
}

func TestMultiNode_GetIndexesRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	dataNode := h.Node(t, "data-1")
	remoteVaultID := dataNode.vaultID.String()

	// Get chunk ID from the data node directly (ListChunks is RouteLocal).
	metas, err := dataNode.orch.ListChunkMetas(dataNode.vaultID)
	if err != nil {
		t.Fatalf("ListChunkMetas: %v", err)
	}
	if len(metas) == 0 {
		t.Fatal("no chunks to test GetIndexes with")
	}
	chunkID := metas[0].ID.String()

	resp, err := h.vaultClient.GetIndexes(context.Background(), connect.NewRequest(&gastrologv1.GetIndexesRequest{
		Vault:   remoteVaultID,
		ChunkId: chunkID,
	}))
	if err != nil {
		t.Fatalf("GetIndexes on remote vault: %v", err)
	}
	// Memory-backed chunks won't have detailed indexes, but the RPC should succeed.
	_ = resp.Msg.Sealed // proves the response was populated
}

// ---------------------------------------------------------------------------
// Tests — Ingester stats across cluster nodes
// ---------------------------------------------------------------------------

func TestMultiNode_ListIngestersCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	ctx := context.Background()

	// Register an ingester on data-1's orchestrator.
	ingID := uuid.Must(uuid.NewV7())
	h.Node(t, "data-1").orch.RegisterIngester(ingID, "test-ing", "mqtt", nil)

	// Also register it in the config store so ListIngesters can find it.
	_ = h.cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID:     ingID,
		Name:   "test-ing",
		Type:   "mqtt",
		NodeID: "data-1",
	})

	resp, err := h.configClient.ListIngesters(ctx, connect.NewRequest(&gastrologv1.ListIngestersRequest{}))
	if err != nil {
		t.Fatalf("ListIngesters: %v", err)
	}

	if len(resp.Msg.Ingesters) != 1 {
		t.Fatalf("expected 1 ingester, got %d", len(resp.Msg.Ingesters))
	}
	ing := resp.Msg.Ingesters[0]
	if ing.Id != ingID.String() {
		t.Errorf("ingester ID = %q, want %q", ing.Id, ingID.String())
	}
	if ing.NodeId != "data-1" {
		t.Errorf("ingester NodeId = %q, want data-1", ing.NodeId)
	}
	if ing.Name != "test-ing" {
		t.Errorf("ingester Name = %q, want test-ing", ing.Name)
	}
}

func TestMultiNode_GetIngesterStatusCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	ctx := context.Background()

	// Register an ingester on data-1.
	ingID := uuid.Must(uuid.NewV7())
	h.Node(t, "data-1").orch.RegisterIngester(ingID, "test-ing", "mqtt", nil)

	_ = h.cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID:     ingID,
		Name:   "test-ing",
		Type:   "mqtt",
		NodeID: "data-1",
	})

	// Simulate some ingester activity by bumping stats on data-1.
	stats := h.Node(t, "data-1").orch.GetIngesterStats(ingID)
	if stats != nil {
		stats.MessagesIngested.Add(42)
		stats.BytesIngested.Add(1024)
	}

	resp, err := h.configClient.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: ingID.String(),
	}))
	if err != nil {
		t.Fatalf("GetIngesterStatus: %v", err)
	}

	if resp.Msg.Type != "mqtt" {
		t.Errorf("Type = %q, want mqtt", resp.Msg.Type)
	}
	if resp.Msg.MessagesIngested != 42 {
		t.Errorf("MessagesIngested = %d, want 42", resp.Msg.MessagesIngested)
	}
	if resp.Msg.BytesIngested != 1024 {
		t.Errorf("BytesIngested = %d, want 1024", resp.Msg.BytesIngested)
	}
}

// ---------------------------------------------------------------------------
// Tests — Jobs across cluster nodes
// ---------------------------------------------------------------------------

func TestMultiNode_ListJobsCrossNode(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	// Inject a peer job from data-1.
	h.peerJobs.peers["data-1"] = []*gastrologv1.Job{
		{Id: "remote-compact", Name: "compact", Description: "compact data-1 vault", NodeId: "data-1", Kind: gastrologv1.JobKind_JOB_KIND_SCHEDULED},
		{Id: "remote-retain", Name: "retain", Description: "retain data-1 vault", NodeId: "data-1", Kind: gastrologv1.JobKind_JOB_KIND_SCHEDULED},
	}

	resp, err := h.jobSrv.ListJobs(context.Background(), connect.NewRequest(&gastrologv1.ListJobsRequest{}))
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	// Should include both the peer jobs (local scheduler may also have jobs).
	peerJobIDs := make(map[string]bool)
	for _, j := range resp.Msg.Jobs {
		if j.NodeId == "data-1" {
			peerJobIDs[j.Id] = true
		}
	}
	if !peerJobIDs["remote-compact"] {
		t.Error("expected remote-compact from data-1 in ListJobs")
	}
	if !peerJobIDs["remote-retain"] {
		t.Error("expected remote-retain from data-1 in ListJobs")
	}
}

func TestMultiNode_HistogramMatchesRecordCount(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	// Insert same number of records in both vaults — simulates duplicate routing.
	addMNRecords(t, h.Node(t, "data-1"), "D1", 20, nil)
	addMNRecords(t, h.Node(t, "data-2"), "D2", 20, nil)

	// Use a time range that covers all test records (addMNRecords starts at 2025-06-15T10:00:00Z).
	expr := "start=2025-06-15T09:59:00Z end=2025-06-15T10:01:00Z"

	// Collect all records and the histogram from the search.
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	var records []*gastrologv1.Record
	var histogramTotal int64
	for stream.Receive() {
		records = append(records, stream.Msg().Records...)
		for _, b := range stream.Msg().Histogram {
			histogramTotal += b.Count
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if len(records) != 40 {
		t.Errorf("expected 40 records (20 per vault), got %d", len(records))
	}
	if histogramTotal != int64(len(records)) {
		t.Errorf("histogram total %d != record count %d", histogramTotal, len(records))
	}
}


