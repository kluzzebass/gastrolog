package server_test

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"os"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
	"net/http"
)

// ---------------------------------------------------------------------------
// Server-level multi-node tier transition tests
//
// These create full Server instances with real HTTP handlers, Connect RPC
// clients, file-backed tiers, and in-process transferrers. Tests exercise
// the complete RPC path — not just orchestrator internals.
// ---------------------------------------------------------------------------

// mnTransferrer implements orchestrator.RemoteTransferrer by calling directly
// into the target orchestrator's exported methods. Same as directTransferrer
// in transition_test.go but usable from server_test package.
type mnTransferrer struct {
	nodes map[string]*orchestrator.Orchestrator
}

func (d *mnTransferrer) StreamToTier(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	return orch.StreamAppendToTier(ctx, vaultID, tierID, next)
}

func (d *mnTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	for _, rec := range records {
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *mnTransferrer) TransferRecords(_ context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
}

func (d *mnTransferrer) WaitVaultReady(_ context.Context, _ string, _ glid.GLID) error {
	return nil
}

// mnSystemLoader wraps a system.Store to implement orchestrator.SystemLoader.
type mnSystemLoader struct {
	store *sysmem.Store
}

func (l *mnSystemLoader) Load(ctx context.Context) (*system.System, error) {
	return l.store.Load(ctx)
}

// mnTierNode holds one node's state in a multi-tier multi-node test.
type mnTierNode struct {
	nodeID  string
	orch    *orchestrator.Orchestrator
	tierDir string
	cm      *chunkfile.Manager
}

// TestMultiNode_TierTransitionSearchFanOut creates a 4-node cluster where:
//   - coord: coordinator with no vault (only fans out)
//   - data-1: owns a 2-tier vault (hot + warm), leader for both tiers
//   - data-2, data-3: followers
//
// Ingests records on data-1, transitions from tier 0 to tier 1, then
// verifies the coordinator can search all records via RPC fan-out.
func TestMultiNode_TierTransitionSearchFanOut(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	tier0ID := glid.New()
	tier1ID := glid.New()

	cfgStore := sysmem.NewStore()
	ctx := context.Background()

	_ = cfgStore.PutTier(ctx, system.TierConfig{
		ID: tier0ID, Name: "hot", Type: system.TierTypeFile,
		VaultID: vaultID, Position: 0,
	})
	_ = cfgStore.SetTierPlacements(ctx, tier0ID, []system.TierPlacement{
		{StorageID: system.SyntheticStorageID("data-1"), Leader: true},
	})
	_ = cfgStore.PutTier(ctx, system.TierConfig{
		ID: tier1ID, Name: "warm", Type: system.TierTypeFile,
		VaultID: vaultID, Position: 1,
	})
	_ = cfgStore.SetTierPlacements(ctx, tier1ID, []system.TierPlacement{
		{StorageID: system.SyntheticStorageID("data-1"), Leader: true},
	})
	_ = cfgStore.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "tiered-vault",
	})

	// Create data-1 with file-backed 2-tier vault.
	dir0 := t.TempDir()
	cm0, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir0, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	im0 := indexfile.NewManager(dir0, nil, nil)

	dir1 := t.TempDir()
	cm1, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir1, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	im1 := indexfile.NewManager(dir1, nil, nil)

	orchData1, err := orchestrator.New(orchestrator.Config{
		LocalNodeID:  "data-1",
		SystemLoader: &mnSystemLoader{store: cfgStore},
	})
	if err != nil {
		t.Fatal(err)
	}

	tierInst0 := &orchestrator.TierInstance{
		TierID: tier0ID, Type: "file", Chunks: cm0, Indexes: im0,
		Query: query.New(cm0, im0, nil),
	}
	tierInst1 := &orchestrator.TierInstance{
		TierID: tier1ID, Type: "file", Chunks: cm1, Indexes: im1,
		Query: query.New(cm1, im1, nil),
	}
	vault := orchestrator.NewVault(vaultID, tierInst0, tierInst1)
	orchData1.RegisterVault(vault)

	// Create coordinator (no vault).
	orchCoord, err := orchestrator.New(orchestrator.Config{LocalNodeID: "coord"})
	if err != nil {
		t.Fatal(err)
	}

	// Wire remote searcher: coordinator → data-1.
	remoteSearcher := &directRemoteSearcher{nodes: map[string]*orchestrator.Orchestrator{
		"data-1": orchData1,
	}}

	// Build server for coordinator.
	srv := server.New(orchCoord, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:         "coord",
		RemoteSearcher: remoteSearcher,
	})
	handler := srv.Handler()
	httpClient := &http.Client{Transport: &embeddedTransport{handler: handler}}
	queryClient := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")

	t.Cleanup(func() {
		orchCoord.Stop()
		orchData1.Stop()
		_ = cm0.Close()
		_ = cm1.Close()
	})

	// Ingest 1000 records on data-1 tier 0.
	const totalRecords = 1000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Second)
		if err := orchData1.AppendToTier(vaultID, tier0ID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw: fmt.Appendf(nil, "tiered-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Search via coordinator BEFORE transition — all records should be findable.
	preRecords := searchAllRecords(t, queryClient, "")
	if len(preRecords) != totalRecords {
		t.Fatalf("pre-transition search: expected %d records, got %d", totalRecords, len(preRecords))
	}

	// Seal and transition tier 0 → tier 1 on data-1.
	if active := cm0.Active(); active != nil && active.RecordCount > 0 {
		_ = cm0.Seal()
	}
	metas, _ := cm0.List()
	t.Logf("data-1 tier 0: %d sealed chunks to transition", len(metas))

	// PostSealProcess each chunk (compress + index).
	for _, m := range metas {
		_ = cm0.PostSealProcess(ctx, m.ID)
	}

	// Transition via orchestrator (uses retentionRunner internally).
	for _, m := range metas {
		orchData1.TransitionChunkForTesting(vaultID, tier0ID, m.ID)
	}

	// Search via coordinator AFTER transition — records should still be findable
	// (now served from tier 1 via the multi-tier query engine).
	postRecords := searchAllRecords(t, queryClient, "")
	if len(postRecords) != totalRecords {
		t.Errorf("post-transition search: expected %d records, got %d (lost %d)",
			totalRecords, len(postRecords), totalRecords-len(postRecords))
	}

	// Verify tier 0 directory is clean on data-1.
	entries, _ := os.ReadDir(dir0)
	var chunkDirs int
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			chunkDirs++
		}
	}
	if chunkDirs > 0 {
		t.Errorf("data-1 tier 0: %d chunk directories still on disk after transition", chunkDirs)
	}
}

// searchAllRecords collects all records from a streaming Search RPC.
func searchAllRecords(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string) []*gastrologv1.Record {
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
