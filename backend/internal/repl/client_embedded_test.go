package repl

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

func TestEmbeddedClient_ListStores(t *testing.T) {
	client := setupEmbeddedClient(t)

	stores := client.ListStores()
	if len(stores) != 1 {
		t.Fatalf("expected 1 store, got %d", len(stores))
	}
	if stores[0] != "default" {
		t.Errorf("expected store 'default', got %q", stores[0])
	}
}

func TestEmbeddedClient_Search(t *testing.T) {
	client, orch, cm := setupEmbeddedClientWithCM(t)

	// Start orchestrator
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	defer orch.Stop()

	// Append some records
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		rec := chunk.Record{
			IngestTS: baseTime.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"service": "api"},
			Raw:      []byte("test log message"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Search via embedded client
	q := query.Query{Limit: 10}
	iter, getToken, err := client.Search(context.Background(), "default", q, nil)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	count := 0
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("iter error: %v", err)
		}
		if string(rec.Raw) != "test log message" {
			t.Errorf("unexpected raw: %s", rec.Raw)
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 records, got %d", count)
	}

	// Token should be available after iteration
	_ = getToken()
}

func TestEmbeddedClient_IsRunning(t *testing.T) {
	client, orch, _ := setupEmbeddedClientWithCM(t)

	// Before start, should report not running (health check fails)
	// Note: The health endpoint returns status based on orchestrator state

	// Start orchestrator
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	defer orch.Stop()

	// After start, should report running
	if !client.IsRunning() {
		t.Error("expected IsRunning() to return true after start")
	}
}

func setupEmbeddedClient(t *testing.T) *GRPCClient {
	client, _, _ := setupEmbeddedClientWithCM(t)
	return client
}

func setupEmbeddedClientWithCM(t *testing.T) (*GRPCClient, *orchestrator.Orchestrator, chunk.ChunkManager) {
	t.Helper()

	// Create memory-based chunk manager
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	// Create memory-based index manager
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

	// Create query engine
	qe := query.New(cm, im, nil)

	// Create orchestrator
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", qe)

	// Create embedded client
	client := NewEmbeddedClient(orch)

	return client, orch, cm
}
