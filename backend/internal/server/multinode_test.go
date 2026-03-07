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
// Creates two in-process nodes with separate orchestrators and vaults.
// The coordinator node (node-A) has a directRemoteSearcher that calls
// into node-B's orchestrator to simulate cross-node query fan-out.
// ---------------------------------------------------------------------------

type multinodeTestNode struct {
	nodeID  string
	orch    *orchestrator.Orchestrator
	vaultID uuid.UUID
	vault   memtest.Vault
}

type multiNodeHarness struct {
	nodeA    multinodeTestNode
	nodeB    multinodeTestNode
	cfgStore config.Store
	srv      *server.Server
	client   gastrologv1connect.QueryServiceClient
	jobSrv   gastrologv1connect.JobServiceClient
}

func setupMultiNode(t *testing.T) *multiNodeHarness {
	t.Helper()

	nodeA := setupMNNode(t, "node-A")
	nodeB := setupMNNode(t, "node-B")

	// Shared config store that knows about both vaults.
	cfgStore := cfgmem.NewStore()
	ctx := context.Background()
	_ = cfgStore.PutVault(ctx, config.VaultConfig{
		ID: nodeA.vaultID, Name: "vault-a", Type: "memory", NodeID: "node-A",
	})
	_ = cfgStore.PutVault(ctx, config.VaultConfig{
		ID: nodeB.vaultID, Name: "vault-b", Type: "memory", NodeID: "node-B",
	})

	// Create a direct remote searcher: when node-A queries node-B,
	// it calls directly into node-B's query engine.
	remoteSearcher := &directRemoteSearcher{
		nodes: map[string]*orchestrator.Orchestrator{
			"node-B": nodeB.orch,
		},
	}

	// Peer jobs from node-B (simulated broadcast).
	peerJobs := &mnPeerJobs{peers: map[string][]*gastrologv1.Job{}}

	srv := server.New(nodeA.orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:         "node-A",
		RemoteSearcher: remoteSearcher,
		PeerJobs:       peerJobs,
	})

	handler := srv.Handler()
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	queryClient := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
	jobClient := gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded")

	t.Cleanup(func() {
		nodeA.orch.Stop()
		nodeB.orch.Stop()
	})

	return &multiNodeHarness{
		nodeA: nodeA, nodeB: nodeB,
		cfgStore: cfgStore, srv: srv,
		client: queryClient, jobSrv: jobClient,
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

// mnPeerJobs provides jobs from simulated peer nodes.
type mnPeerJobs struct {
	peers map[string][]*gastrologv1.Job
}

func (p *mnPeerJobs) GetAll() map[string][]*gastrologv1.Job {
	return p.peers
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

	scopedExpr := fmt.Sprintf("vault=%s %s", vaultID, req.GetQuery())
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestMultiNode_SearchFanOut(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t)

	// Add records to both nodes.
	addMNRecords(t, h.nodeA, "A", 5, nil)
	addMNRecords(t, h.nodeB, "B", 5, nil)

	// Search via coordinator (node-A) — should return records from both nodes.
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	var allRecords []*gastrologv1.Record
	for stream.Receive() {
		allRecords = append(allRecords, stream.Msg().Records...)
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if len(allRecords) != 10 {
		t.Errorf("expected 10 records from both nodes, got %d", len(allRecords))
	}

	// Verify we have records from both prefixes.
	var aCount, bCount int
	for _, r := range allRecords {
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
	h := setupMultiNode(t)

	// Add records with level attrs to both nodes.
	addMNRecords(t, h.nodeA, "A", 3, map[string]string{"level": "error"})
	addMNRecords(t, h.nodeA, "A-info", 2, map[string]string{"level": "info"})
	addMNRecords(t, h.nodeB, "B", 4, map[string]string{"level": "error"})
	addMNRecords(t, h.nodeB, "B-info", 1, map[string]string{"level": "info"})

	// Run stats count by level — should aggregate across both nodes.
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{
			Expression: "| stats count by level",
		},
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

	if table == nil {
		t.Fatal("expected table result from stats query")
	}

	// Verify aggregated counts: error=7 (3+4), info=3 (2+1).
	levelIdx := -1
	countIdx := -1
	for i, col := range table.Columns {
		if col == "level" {
			levelIdx = i
		}
		if col == "count" {
			countIdx = i
		}
	}
	if levelIdx < 0 || countIdx < 0 {
		t.Fatalf("expected level and count columns, got %v", table.Columns)
	}

	counts := make(map[string]string)
	for _, row := range table.Rows {
		counts[row.Values[levelIdx]] = row.Values[countIdx]
	}
	if counts["error"] != "7" {
		t.Errorf("expected error count 7, got %q", counts["error"])
	}
	if counts["info"] != "3" {
		t.Errorf("expected info count 3, got %q", counts["info"])
	}
}

func TestMultiNode_NonDistributiveAvgGlobal(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t)

	// Node A: values 10, 20 → per-node avg=15.
	// Node B: values 30 → per-node avg=30.
	// Wrong (distributed sum of avgs): 15+30=45.
	// Correct (global avg): (10+20+30)/3 = 20.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i, v := range []int{10, 20} {
		ts := t0.Add(time.Duration(i) * time.Second)
		h.nodeA.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   []byte(fmt.Sprintf("val=%d", v)),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}
	for i, v := range []int{30} {
		ts := t0.Add(time.Duration(i+10) * time.Second)
		h.nodeB.vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   []byte(fmt.Sprintf("val=%d", v)),
			Attrs: map[string]string{"val": fmt.Sprintf("%d", v)},
		})
	}

	// avg(val) should produce 20 (global), not 45 (wrong distributed sum).
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{
			Expression: "| stats avg(val)",
		},
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

	if table == nil {
		t.Fatal("expected table result from avg query")
	}

	// Find avg column value.
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
	h := setupMultiNode(t)

	// Simulate node-B having a job (via peer broadcast).
	peerJobs := h.srv // We set peerJobs in setupMultiNode
	_ = peerJobs

	// Create a job server directly with mock peer jobs.
	mockPeers := &mnPeerJobs{peers: map[string][]*gastrologv1.Job{
		"node-B": {
			{Id: "job-on-B", Name: "compact-B", NodeId: "node-B",
				Kind: gastrologv1.JobKind_JOB_KIND_SCHEDULED},
		},
	}}
	jobSrv := server.NewJobServer(h.nodeA.orch.Scheduler(), "node-A", mockPeers)

	// GetJob for a peer job should succeed.
	resp, err := jobSrv.GetJob(context.Background(), connect.NewRequest(&gastrologv1.GetJobRequest{Id: "job-on-B"}))
	if err != nil {
		t.Fatalf("GetJob for peer job: %v", err)
	}
	if resp.Msg.Job.NodeId != "node-B" {
		t.Errorf("expected NodeId node-B, got %q", resp.Msg.Job.NodeId)
	}
}
