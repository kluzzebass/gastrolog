package server_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
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
)

func TestDrainWaitsForInFlightRequests(t *testing.T) {
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
	for i := 0; i < 10; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", query.New(cm, im, nil))

	// Create server
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	// Create client with embedded transport
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	queryClient := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	// Start a long-running search in background
	var searchStarted atomic.Bool
	var searchDone atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, err := queryClient.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query: &gastrologv1.Query{},
		}))
		if err != nil {
			t.Errorf("Search failed: %v", err)
			return
		}
		searchStarted.Store(true)
		// Slowly consume results
		for stream.Receive() {
			time.Sleep(50 * time.Millisecond)
		}
		searchDone.Store(true)
	}()

	// Wait for search to start
	for !searchStarted.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	// Issue shutdown with drain=true
	drainDone := make(chan struct{})
	go func() {
		_, err := lifecycleClient.Shutdown(context.Background(), connect.NewRequest(&gastrologv1.ShutdownRequest{
			Drain: true,
		}))
		if err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
		close(drainDone)
	}()

	// Shutdown RPC returns immediately
	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown RPC should return immediately")
	}

	// Wait for search to complete (drain should wait for it)
	wg.Wait()

	if !searchDone.Load() {
		t.Error("Search should have completed")
	}

	// Wait for shutdown channel to be closed (drain happens in background)
	select {
	case <-srv.ShutdownChan():
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown channel should be closed after drain")
	}
}

func TestDrainRejectsNewRequests(t *testing.T) {
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
	for i := 0; i < 10; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", query.New(cm, im, nil))

	// Create server
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	// Create client with embedded transport
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	queryClient := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	// Start a slow search in background that we'll use to hold the drain
	slowSearchDone := make(chan struct{})
	var slowSearchStarted atomic.Bool
	go func() {
		defer close(slowSearchDone)
		stream, err := queryClient.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query: &gastrologv1.Query{},
		}))
		if err != nil {
			return
		}
		slowSearchStarted.Store(true)
		// Read results very slowly to keep the request in-flight
		for stream.Receive() {
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Wait for slow search to start
	for !slowSearchStarted.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	// Issue shutdown with drain=true (this runs in background and starts rejecting)
	_, err := lifecycleClient.Shutdown(context.Background(), connect.NewRequest(&gastrologv1.ShutdownRequest{
		Drain: true,
	}))
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// Give drain time to set the draining flag
	time.Sleep(50 * time.Millisecond)

	// Try to make a new request - should be rejected because draining flag is set
	stream, err := queryClient.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{},
	}))
	if err != nil {
		// Good - rejected at connect time
		return
	}
	// For streaming, the error might come when receiving
	if stream.Receive() {
		t.Fatal("Expected new request to be rejected during drain")
	}
	if stream.Err() == nil {
		t.Fatal("Expected error from rejected request during drain")
	}
}

func TestShutdownWithoutDrain(t *testing.T) {
	// Create orchestrator with a store
	orch := orchestrator.New(orchestrator.Config{})

	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", query.New(cm, im, nil))

	// Create server
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	// Create client with embedded transport
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	// Issue shutdown with drain=false
	_, err := lifecycleClient.Shutdown(context.Background(), connect.NewRequest(&gastrologv1.ShutdownRequest{
		Drain: false,
	}))
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// Shutdown channel should be closed quickly (no drain wait)
	select {
	case <-srv.ShutdownChan():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Shutdown channel should be closed immediately without drain")
	}
}

func TestHealth(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	// Orchestrator is not running, should be unhealthy
	resp, err := lifecycleClient.Health(context.Background(), connect.NewRequest(&gastrologv1.HealthRequest{}))
	if err != nil {
		t.Fatalf("Health failed: %v", err)
	}
	if resp.Msg.Status != gastrologv1.Status_STATUS_UNHEALTHY {
		t.Errorf("expected unhealthy status, got %v", resp.Msg.Status)
	}
	if resp.Msg.Version == "" {
		t.Error("expected version to be set")
	}
}

func TestProbeEndpoints(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	t.Run("healthz always returns 200", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://embedded/healthz", nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("healthz request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("readyz returns 503 when not running", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "http://embedded/readyz", nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("readyz request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", resp.StatusCode)
		}
	})

	t.Run("readyz returns 200 when running", func(t *testing.T) {
		// Start orchestrator
		if err := orch.Start(context.Background()); err != nil {
			t.Fatalf("failed to start orchestrator: %v", err)
		}
		defer orch.Stop()

		req, _ := http.NewRequest("GET", "http://embedded/readyz", nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("readyz request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})
}
