package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
)

func TestQueryServerSearch(t *testing.T) {
	// Create orchestrator with a store
	orch := orchestrator.New(orchestrator.Config{})

	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

	// Add some records
	t0 := time.Now()
	for i := 0; i < 5; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	defaultID := uuid.Must(uuid.NewV7())
	orch.RegisterChunkManager(defaultID, cm)
	orch.RegisterIndexManager(defaultID, im)
	orch.RegisterQueryEngine(defaultID, query.New(cm, im, nil))

	// Create server
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	// Create test server
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Create client
	client := gastrologv1connect.NewQueryServiceClient(
		http.DefaultClient,
		ts.URL,
	)

	// Run search
	t.Log("Running search...")
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{},
	}))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	count := 0
	for stream.Receive() {
		msg := stream.Msg()
		count += len(msg.Records)
		t.Logf("Received batch of %d records", len(msg.Records))
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream error: %v", err)
	}

	t.Logf("Total: %d records", count)
	if count != 5 {
		t.Errorf("expected 5 records, got %d", count)
	}
}
