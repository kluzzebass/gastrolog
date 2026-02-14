package repl

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

func TestEmbeddedClient_ListStores(t *testing.T) {
	client, _, _ := setupTestSystem(t)

	stores := client.ListStores()
	if len(stores) != 1 {
		t.Fatalf("expected 1 store, got %d", len(stores))
	}
	if stores[0].ID != "default" {
		t.Errorf("expected store ID 'default', got %q", stores[0].ID)
	}
}

func TestEmbeddedClient_Search(t *testing.T) {
	client, orch, cm := setupTestSystem(t)

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
	client, orch, _ := setupTestSystem(t)

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
