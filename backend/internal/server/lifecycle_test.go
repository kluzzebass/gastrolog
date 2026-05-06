package server_test

import (
	"context"
	"gastrolog/internal/glid"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
)

// mockCluster implements ClusterStatusProvider for testing.
type mockCluster struct {
	leaderAddr string
	leaderID   string
	servers    []cluster.RaftServer
}

func (m *mockCluster) LeaderInfo() (string, string)           { return m.leaderAddr, m.leaderID }
func (m *mockCluster) Servers() ([]cluster.RaftServer, error) { return m.servers, nil }
func (m *mockCluster) LocalStats() map[string]string          { return nil }

func TestDrainWaitsForInFlightRequests(t *testing.T) {
	// Create orchestrator with a vault
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	// Add some records
	t0 := time.Now()
	for i := range 10 {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	defaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// Create server
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
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
	wg.Go(func() {
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
	})

	// Wait for search to start
	for !searchStarted.Load() {
		runtime.Gosched()
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
}

func TestDrainRejectsNewRequests(t *testing.T) {
	// Create orchestrator with a vault
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	// Add some records
	t0 := time.Now()
	for i := range 10 {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	defaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// Create server
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
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
		runtime.Gosched()
	}

	// Issue shutdown with drain=true (this runs in background and starts rejecting)
	_, err = lifecycleClient.Shutdown(context.Background(), connect.NewRequest(&gastrologv1.ShutdownRequest{
		Drain: true,
	}))
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// Poll until new requests are rejected (drain flag is set).
	drainDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(drainDeadline) {
		stream, err := queryClient.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query: &gastrologv1.Query{},
		}))
		if err != nil {
			// Rejected at connect time — drain is active.
			return
		}
		if !stream.Receive() && stream.Err() != nil {
			// Rejected during receive — drain is active.
			return
		}
		runtime.Gosched()
	}
	t.Fatal("Expected new request to be rejected during drain")
}

func TestShutdownWithoutDrain(t *testing.T) {
	// Create orchestrator with a vault
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	defaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// Create server
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	// Create client with embedded transport
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	// Issue shutdown with drain=false
	_, err = lifecycleClient.Shutdown(context.Background(), connect.NewRequest(&gastrologv1.ShutdownRequest{
		Drain: false,
	}))
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestHealth(t *testing.T) {
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
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
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
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

func TestGetClusterStatus_ClusterAddressUsesAdvertised(t *testing.T) {
	t.Parallel()

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	mc := &mockCluster{
		leaderAddr: "node-1:4566",
		leaderID:   "node-1-id",
		servers: []cluster.RaftServer{
			{ID: "node-1-id", Address: "node-1:4566", Suffrage: "Voter"},
			{ID: "node-2-id", Address: "node-2:4566", Suffrage: "Voter"},
			{ID: "node-3-id", Address: "node-3:4566", Suffrage: "Voter"},
		},
	}

	cfgStore := sysmem.NewStore()

	// Server's listen address is port-only (the bug condition).
	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:         "node-2-id",
		ClusterAddress: ":4566",
		Cluster:        mc,
	})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	lifecycleClient := gastrologv1connect.NewLifecycleServiceClient(httpClient, "http://embedded")

	resp, err := lifecycleClient.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}

	// ClusterAddress should be the advertised address (node-2:4566),
	// NOT the listen address (:4566).
	got := resp.Msg.ClusterAddress
	if got == ":4566" {
		t.Errorf("ClusterAddress = %q, want advertised address with hostname", got)
	}
	if got != "node-2:4566" {
		t.Errorf("ClusterAddress = %q, want %q", got, "node-2:4566")
	}

	// The join command in the Nodes settings tab uses ClusterAddress.
	// Verify it's suitable for cross-host joins.
	if !strings.Contains(got, "node-2") {
		t.Errorf("ClusterAddress %q doesn't contain the node hostname", got)
	}
}

func TestReadyz_localVaultReplicationNotReady(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVault(vid, &orchestrator.VaultInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     s.CM,
		Indexes:    s.IM,
		Query:      s.QE,
		IsFSMReady: func() bool { return false },
	}))
	if err := orch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: srv.Handler()},
	}
	req, err := http.NewRequest(http.MethodGet, "http://embedded/readyz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz: want 503 while tier FSM not ready, got %d", resp.StatusCode)
	}
}
