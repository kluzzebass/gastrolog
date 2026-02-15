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
	// Store ID is now a UUID string, just verify we got one store.
	if stores[0].ID == "" {
		t.Error("expected non-empty store ID")
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
	for i := range 5 {
		rec := chunk.Record{
			IngestTS: baseTime.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"service": "api"},
			Raw:      []byte("test log message"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Search via embedded client — use the store ID from ListStores.
	stores := client.ListStores()
	if len(stores) == 0 {
		t.Fatal("expected at least one store")
	}
	storeID := stores[0].ID

	q := query.Query{Limit: 10}
	iter, getToken, err := client.Search(context.Background(), storeID, q, nil)
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
