package app

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestClearStaleIngesterAlive_RemovesStaleLocalEntry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	const localNode = "node-A"
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: store, LocalNodeID: localNode})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(orch.Close)

	// Ingester X is configured (present in ListIngesters) but this node
	// isn't running it — e.g. previous session crashed before cleanup, or
	// config was edited while down to remove this node from NodeIDs.
	ingID := glid.New()
	if err := store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "x", Type: "chatterbox", Enabled: true,
		NodeIDs: []string{"node-B"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Simulate stale Raft state: this node is marked alive from a previous life.
	if err := store.SetIngesterAlive(ctx, ingID, localNode, true); err != nil {
		t.Fatalf("seed SetIngesterAlive: %v", err)
	}

	// Sweep runs at startup.
	clearStaleIngesterAlive(ctx, store, orch, localNode, silentLogger())

	alive, err := store.GetIngesterAlive(ctx, ingID)
	if err != nil {
		t.Fatalf("GetIngesterAlive: %v", err)
	}
	if _, present := alive[localNode]; present {
		t.Fatalf("expected %s to be cleared from alive map, got %v", localNode, alive)
	}
}

func TestClearStaleIngesterAlive_PreservesOtherNodesEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	const localNode = "node-A"
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: store, LocalNodeID: localNode})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(orch.Close)

	ingID := glid.New()
	if err := store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "x", Type: "chatterbox", Enabled: true,
		NodeIDs: []string{"node-B", "node-C"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// node-B and node-C are legitimately running the ingester — their alive
	// entries must survive node-A's startup sweep.
	if err := store.SetIngesterAlive(ctx, ingID, "node-B", true); err != nil {
		t.Fatalf("seed B: %v", err)
	}
	if err := store.SetIngesterAlive(ctx, ingID, "node-C", true); err != nil {
		t.Fatalf("seed C: %v", err)
	}

	clearStaleIngesterAlive(ctx, store, orch, localNode, silentLogger())

	alive, err := store.GetIngesterAlive(ctx, ingID)
	if err != nil {
		t.Fatalf("GetIngesterAlive: %v", err)
	}
	if !alive["node-B"] || !alive["node-C"] {
		t.Fatalf("sweep destroyed other nodes' alive entries: %v", alive)
	}
}

func TestClearStaleIngesterAlive_LeavesRunningIngesterAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	const localNode = "node-A"
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: store, LocalNodeID: localNode})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(orch.Close)

	ingID := glid.New()
	// Register the ingester so orch.ListIngesters() reports it as running.
	orch.RegisterIngester(ingID, "x", "chatterbox", noopRunner{})

	if err := store.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "x", Type: "chatterbox", Enabled: true,
		NodeIDs: []string{localNode},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	if err := store.SetIngesterAlive(ctx, ingID, localNode, true); err != nil {
		t.Fatalf("seed: %v", err)
	}

	clearStaleIngesterAlive(ctx, store, orch, localNode, silentLogger())

	alive, err := store.GetIngesterAlive(ctx, ingID)
	if err != nil {
		t.Fatalf("GetIngesterAlive: %v", err)
	}
	if !alive[localNode] {
		t.Fatalf("sweep wrongly cleared alive for a running ingester: %v", alive)
	}
}

type noopRunner struct{}

func (noopRunner) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	<-ctx.Done()
	return nil
}
