package server_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"github.com/google/uuid"
)

func TestMetricsEndpoint(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	// Add some records so vault has data.
	t0 := time.Now()
	for i := range 5 {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	vaultID := uuid.Must(uuid.NewV7())
	orch.RegisterVault(orchestrator.NewVault(vaultID, s.CM, s.IM, s.QE))

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	req, _ := http.NewRequest("GET", "http://embedded/metrics", nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	text := string(body)

	// Verify key metrics are present.
	for _, want := range []string{
		"gastrolog_info",
		"gastrolog_up",
		"gastrolog_uptime_seconds",
		"gastrolog_ingest_queue_depth",
		"gastrolog_ingest_queue_capacity",
		"gastrolog_store_chunks_total",
		"gastrolog_store_records_total",
		"gastrolog_store_bytes",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing metric %q in output", want)
		}
	}

	// Vault should have records.
	if !strings.Contains(text, "gastrolog_store_records_total") {
		t.Error("missing vault records metric")
	}
}

func TestMetricsWithRunningOrchestrator(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("failed to start orchestrator: %v", err)
	}
	defer orch.Stop()

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	req, _ := http.NewRequest("GET", "http://embedded/metrics", nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	if !strings.Contains(text, "gastrolog_up 1") {
		t.Error("expected gastrolog_up 1 when orchestrator is running")
	}
}
