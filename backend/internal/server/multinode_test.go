package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
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
	coordinator   string // nodeID of the coordinator
	nodes         map[string]multinodeTestNode
	cfgStore      config.Store
	srv           *server.Server
	client        gastrologv1connect.QueryServiceClient
	vaultClient   gastrologv1connect.VaultServiceClient
	configClient  gastrologv1connect.ConfigServiceClient
	jobSrv        gastrologv1connect.JobServiceClient
	peerJobs      *mnPeerJobs
	peerRouteStats *mnPeerRouteStats
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
			_ = cfgStore.PutVault(ctx, config.VaultConfig{
				ID: node.vaultID, Name: "vault-" + id, Type: "memory", NodeID: id,
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
	remoteVaultFwd := &directRemoteVaultForwarder{nodes: remoteOrchestrators}

	peerJobs := &mnPeerJobs{peers: map[string][]*gastrologv1.Job{}}

	// Collect remote orchestrators for peer route stats.
	peerRouteNodes := make(map[string]*orchestrator.Orchestrator)
	for _, id := range nodeIDs {
		if id != coordinatorID {
			peerRouteNodes[id] = nodes[id].orch
		}
	}
	peerRouteStats := &mnPeerRouteStats{nodes: peerRouteNodes}

	coordNode := nodes[coordinatorID]
	srv := server.New(coordNode.orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:               coordinatorID,
		RemoteSearcher:       remoteSearcher,
		RemoteVaultForwarder: remoteVaultFwd,
		PeerJobs:             peerJobs,
		PeerRouteStats:       peerRouteStats,
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
		coordinator:    coordinatorID,
		nodes:          nodes,
		cfgStore:       cfgStore,
		srv:            srv,
		client:         queryClient,
		vaultClient:    vaultClient,
		configClient:   configClient,
		jobSrv:         jobClient,
		peerJobs:       peerJobs,
		peerRouteStats: peerRouteStats,
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
	orch.RegisterVault(orchestrator.NewVault(vaultID, v.CM, v.IM, v.QE))

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

func (p *mnPeerRouteStats) AggregateRouteStats() (ingested, dropped, routed int64, filterActive bool, vaultStats []*gastrologv1.VaultRouteStats) {
	vaultMap := make(map[string]*gastrologv1.VaultRouteStats)
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
	}
	for _, vs := range vaultMap {
		vaultStats = append(vaultStats, vs)
	}
	return
}

// directRemoteSearcher calls directly into the target node's orchestrator,
// simulating ForwardSearch/ForwardFollow/ForwardExplain RPCs without gRPC.
type directRemoteSearcher struct {
	nodes  map[string]*orchestrator.Orchestrator
	called bool
}

func (d *directRemoteSearcher) Search(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	d.called = true
	orch, ok := d.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}

	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, fmt.Errorf("invalid vault_id: %w", err)
	}

	scopedExpr := fmt.Sprintf("vault_id=%s %s", vaultID, req.GetQuery())
	q, pipeline, err := server.ParseExpression(scopedExpr)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	eng := orch.MultiVaultQueryEngine()

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

	// Regular search.
	const maxBatch = 500
	if q.Limit == 0 || q.Limit > maxBatch {
		q.Limit = maxBatch
	}
	searchIter, _ := eng.Search(ctx, q, nil)

	var records []*gastrologv1.ExportRecord
	for rec, err := range searchIter {
		if err != nil {
			return nil, err
		}
		records = append(records, cluster.RecordToExportRecord(rec))
	}
	return &gastrologv1.ForwardSearchResponse{Records: records}, nil
}

func (d *directRemoteSearcher) GetContext(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	return nil, fmt.Errorf("not implemented in test harness")
}

func (d *directRemoteSearcher) Explain(ctx context.Context, nodeID string, req *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	return nil, fmt.Errorf("not implemented in test harness")
}

func (d *directRemoteSearcher) Follow(ctx context.Context, nodeID string, req *gastrologv1.ForwardFollowRequest) (<-chan *gastrologv1.ExportRecord, <-chan error) {
	errCh := make(chan error, 1)
	errCh <- fmt.Errorf("not implemented in test harness")
	close(errCh)
	return nil, errCh
}

// directRemoteVaultForwarder implements RemoteVaultForwarder by calling
// directly into the target node's orchestrator, simulating cluster RPCs.
type directRemoteVaultForwarder struct {
	nodes map[string]*orchestrator.Orchestrator
}

func (d *directRemoteVaultForwarder) orch(nodeID string) (*orchestrator.Orchestrator, error) {
	o, ok := d.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}
	return o, nil
}

func (d *directRemoteVaultForwarder) ListChunks(_ context.Context, nodeID string, req *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	metas, err := o.ListChunkMetas(vaultID)
	if err != nil {
		return nil, err
	}
	chunks := make([]*gastrologv1.ChunkMeta, 0, len(metas))
	for _, m := range metas {
		chunks = append(chunks, server.ChunkMetaToProto(m))
	}
	return &gastrologv1.ForwardListChunksResponse{Chunks: chunks}, nil
}

func (d *directRemoteVaultForwarder) GetChunk(_ context.Context, nodeID string, req *gastrologv1.ForwardGetChunkRequest) (*gastrologv1.ForwardGetChunkResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunkID, err := chunk.ParseChunkID(req.GetChunkId())
	if err != nil {
		return nil, err
	}
	meta, err := o.GetChunkMeta(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	return &gastrologv1.ForwardGetChunkResponse{Chunk: server.ChunkMetaToProto(meta)}, nil
}

func (d *directRemoteVaultForwarder) GetIndexes(_ context.Context, nodeID string, req *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunkID, err := chunk.ParseChunkID(req.GetChunkId())
	if err != nil {
		return nil, err
	}
	report, err := o.ChunkIndexInfos(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	resp := &gastrologv1.ForwardGetIndexesResponse{Sealed: report.Sealed}
	for _, idx := range report.Indexes {
		resp.Indexes = append(resp.Indexes, &gastrologv1.IndexInfo{
			Name: idx.Name, Exists: idx.Exists,
			EntryCount: idx.EntryCount, SizeBytes: idx.SizeBytes,
		})
	}
	return resp, nil
}

func (d *directRemoteVaultForwarder) AnalyzeChunk(_ context.Context, nodeID string, req *gastrologv1.ForwardAnalyzeChunkRequest) (*gastrologv1.ForwardAnalyzeChunkResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	a, err := o.NewAnalyzer(vaultID)
	if err != nil {
		return nil, err
	}
	if req.GetChunkId() != "" {
		chunkID, parseErr := chunk.ParseChunkID(req.GetChunkId())
		if parseErr != nil {
			return nil, parseErr
		}
		analysis, analyzeErr := a.AnalyzeChunk(chunkID)
		if analyzeErr != nil {
			return nil, analyzeErr
		}
		return &gastrologv1.ForwardAnalyzeChunkResponse{
			Analyses: []*gastrologv1.ChunkAnalysis{server.ChunkAnalysisToProto(*analysis)},
		}, nil
	}
	agg, err := a.AnalyzeAll()
	if err != nil {
		return nil, err
	}
	analyses := make([]*gastrologv1.ChunkAnalysis, 0, len(agg.Chunks))
	for _, ca := range agg.Chunks {
		analyses = append(analyses, server.ChunkAnalysisToProto(ca))
	}
	return &gastrologv1.ForwardAnalyzeChunkResponse{Analyses: analyses}, nil
}

func (d *directRemoteVaultForwarder) ValidateVault(_ context.Context, nodeID string, req *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	metas, err := o.ListChunkMetas(vaultID)
	if err != nil {
		return nil, err
	}
	resp := server.ValidateVaultLocal(o, vaultID, metas)
	return &gastrologv1.ForwardValidateVaultResponse{
		Valid: resp.Valid, Chunks: resp.Chunks,
	}, nil
}

func (d *directRemoteVaultForwarder) SealVault(_ context.Context, nodeID string, req *gastrologv1.ForwardSealVaultRequest) (*gastrologv1.ForwardSealVaultResponse, error) {
	o, err := d.orch(nodeID)
	if err != nil {
		return nil, err
	}
	vaultID, err := uuid.Parse(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	if err := o.SealActive(vaultID); err != nil {
		return nil, err
	}
	return &gastrologv1.ForwardSealVaultResponse{}, nil
}

func (d *directRemoteVaultForwarder) ReindexVault(_ context.Context, nodeID string, req *gastrologv1.ForwardReindexVaultRequest) (*gastrologv1.ForwardReindexVaultResponse, error) {
	// In tests we don't have the scheduler wired up, so just return a fake job ID.
	return &gastrologv1.ForwardReindexVaultResponse{JobId: "test-reindex-job"}, nil
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

func TestMultiNode_ListChunksRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()
	resp, err := h.vaultClient.ListChunks(context.Background(), connect.NewRequest(&gastrologv1.ListChunksRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("ListChunks on remote vault: %v", err)
	}
	if len(resp.Msg.Chunks) == 0 {
		t.Error("expected at least 1 chunk from remote vault, got 0")
	}
}

func TestMultiNode_GetChunkRemote(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	addMNRecords(t, h.Node(t, "data-1"), "D1", 3, nil)

	remoteVaultID := h.Node(t, "data-1").vaultID.String()

	// First list chunks to get a chunk ID.
	listResp, err := h.vaultClient.ListChunks(context.Background(), connect.NewRequest(&gastrologv1.ListChunksRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(listResp.Msg.Chunks) == 0 {
		t.Fatal("no chunks to test GetChunk with")
	}
	chunkID := listResp.Msg.Chunks[0].Id

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

	// Verify the chunk is now sealed by listing chunks.
	listResp, err := h.vaultClient.ListChunks(context.Background(), connect.NewRequest(&gastrologv1.ListChunksRequest{
		Vault: remoteVaultID,
	}))
	if err != nil {
		t.Fatalf("ListChunks after seal: %v", err)
	}
	sealedCount := 0
	for _, c := range listResp.Msg.Chunks {
		if c.Sealed {
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
