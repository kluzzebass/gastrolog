package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// mockForwarder records Forward() calls for testing.
type mockForwarder struct {
	mu    sync.Mutex
	calls []forwardCall
}

type forwardCall struct {
	NodeID  string
	VaultID uuid.UUID
	Records []chunk.Record
}

func (m *mockForwarder) Forward(_ context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, forwardCall{
		NodeID:  nodeID,
		VaultID: vaultID,
		Records: records,
	})
	return nil
}

func (m *mockForwarder) getCalls() []forwardCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]forwardCall(nil), m.calls...)
}

// mockConfigLoader returns a fixed config for testing.
type mockConfigLoader struct {
	cfg *config.Config
}

func (m *mockConfigLoader) Load(_ context.Context) (*config.Config, error) {
	return m.cfg, nil
}

func TestIngestForwardsToRemoteVault(t *testing.T) {
	localVaultID := uuid.Must(uuid.NewV7())
	remoteVaultID := uuid.Must(uuid.NewV7())
	remoteNodeID := "node-B"

	fwd := &mockForwarder{}

	o := New(Config{LocalNodeID: "node-A"})
	o.SetRecordForwarder(fwd)

	// Register a local vault with a simple mock chunk manager.
	localVault := &Vault{
		ID:      localVaultID,
		Enabled: true,
		Chunks:  &noopChunkManager{},
	}
	o.vaults[localVaultID] = localVault

	// Build filter set with local + remote targets, both catch-all.
	localFilter, _ := CompileFilter(localVaultID, "*")
	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = remoteNodeID

	o.filterSet = NewFilterSet([]*CompiledFilter{localFilter, remoteFilter})

	rec := chunk.Record{
		Attrs: chunk.Attributes{"env": "prod"},
		Raw:   []byte("hello"),
	}

	if err := o.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify forwarder was called for remote vault.
	calls := fwd.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}
	if calls[0].NodeID != remoteNodeID {
		t.Errorf("nodeID = %q, want %q", calls[0].NodeID, remoteNodeID)
	}
	if calls[0].VaultID != remoteVaultID {
		t.Errorf("vaultID = %s, want %s", calls[0].VaultID, remoteVaultID)
	}
}

func TestIngestNoForwarderSkipsRemote(t *testing.T) {
	localVaultID := uuid.Must(uuid.NewV7())
	remoteVaultID := uuid.Must(uuid.NewV7())

	o := New(Config{LocalNodeID: "node-A"})
	// No forwarder set (single-node mode).

	localVault := &Vault{
		ID:      localVaultID,
		Enabled: true,
		Chunks:  &noopChunkManager{},
	}
	o.vaults[localVaultID] = localVault

	// Filter set with local + remote.
	localFilter, _ := CompileFilter(localVaultID, "*")
	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = "node-B"

	o.filterSet = NewFilterSet([]*CompiledFilter{localFilter, remoteFilter})

	// Test that reloadFiltersFromRoutes correctly skips remote
	// vaults when no forwarder is set.
	o.filterSet = nil
	o.cfgLoader = &mockConfigLoader{cfg: &config.Config{
		Routes: []config.RouteConfig{
			{
				ID:           uuid.Must(uuid.NewV7()),
				Enabled:      true,
				Destinations: []uuid.UUID{localVaultID, remoteVaultID},
			},
		},
		Vaults: []config.VaultConfig{
			{ID: localVaultID, NodeID: "node-A"},
			{ID: remoteVaultID, NodeID: "node-B"},
		},
	}}

	if err := o.reloadFiltersFromRoutes(o.cfgLoader.(*mockConfigLoader).cfg); err != nil {
		t.Fatalf("reloadFiltersFromRoutes failed: %v", err)
	}

	// Only local vault should be in the filter set.
	if o.filterSet == nil {
		t.Fatal("filterSet should not be nil")
	}
	if len(o.filterSet.filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(o.filterSet.filters))
	}
	if o.filterSet.filters[0].VaultID != localVaultID {
		t.Errorf("expected local vault %s, got %s", localVaultID, o.filterSet.filters[0].VaultID)
	}
}

func TestReloadFiltersIncludesRemoteWhenForwarderSet(t *testing.T) {
	localVaultID := uuid.Must(uuid.NewV7())
	remoteVaultID := uuid.Must(uuid.NewV7())

	fwd := &mockForwarder{}
	o := New(Config{LocalNodeID: "node-A"})
	o.SetRecordForwarder(fwd)

	localVault := &Vault{
		ID:      localVaultID,
		Enabled: true,
		Chunks:  &noopChunkManager{},
	}
	o.vaults[localVaultID] = localVault

	cfg := &config.Config{
		Routes: []config.RouteConfig{
			{
				ID:           uuid.Must(uuid.NewV7()),
				Enabled:      true,
				Destinations: []uuid.UUID{localVaultID, remoteVaultID},
			},
		},
		Vaults: []config.VaultConfig{
			{ID: localVaultID, NodeID: "node-A"},
			{ID: remoteVaultID, NodeID: "node-B"},
		},
	}

	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		t.Fatalf("reloadFiltersFromRoutes failed: %v", err)
	}

	if o.filterSet == nil {
		t.Fatal("filterSet should not be nil")
	}
	if len(o.filterSet.filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(o.filterSet.filters))
	}

	// Verify one is local and one is remote.
	var localFound, remoteFound bool
	for _, f := range o.filterSet.filters {
		if f.VaultID == localVaultID && f.NodeID == "" {
			localFound = true
		}
		if f.VaultID == remoteVaultID && f.NodeID == "node-B" {
			remoteFound = true
		}
	}
	if !localFound {
		t.Error("local vault filter not found")
	}
	if !remoteFound {
		t.Error("remote vault filter not found")
	}
}

func TestRebuildFilterSetPreservesRemoteFilters(t *testing.T) {
	localVaultID := uuid.Must(uuid.NewV7())
	remoteVaultID := uuid.Must(uuid.NewV7())
	goneVaultID := uuid.Must(uuid.NewV7())

	o := New(Config{LocalNodeID: "node-A"})

	localVault := &Vault{ID: localVaultID, Enabled: true, Chunks: &noopChunkManager{}}
	o.vaults[localVaultID] = localVault

	localFilter, _ := CompileFilter(localVaultID, "*")
	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = "node-B"
	goneFilter, _ := CompileFilter(goneVaultID, "*")
	// goneFilter has no NodeID — local vault that was removed.

	o.filterSet = NewFilterSet([]*CompiledFilter{localFilter, remoteFilter, goneFilter})

	o.rebuildFilterSetLocked()

	// Remote filter should be preserved. Gone local filter should be removed.
	if o.filterSet == nil {
		t.Fatal("filterSet should not be nil")
	}
	for _, f := range o.filterSet.filters {
		if f.VaultID == goneVaultID {
			t.Error("removed local vault filter should be excluded")
		}
	}
	var remoteFound bool
	for _, f := range o.filterSet.filters {
		if f.VaultID == remoteVaultID && f.NodeID == "node-B" {
			remoteFound = true
		}
	}
	if !remoteFound {
		t.Error("remote vault filter should be preserved")
	}
}

// noopChunkManager satisfies the ChunkManager interface for tests
// that only need the ingest path (no actual storage).
type noopChunkManager struct{}

func (n *noopChunkManager) Append(chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (n *noopChunkManager) AppendPreserved(chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (n *noopChunkManager) Seal() error                                                        { return nil }
func (n *noopChunkManager) Active() *chunk.ChunkMeta                                           { return &chunk.ChunkMeta{} }
func (n *noopChunkManager) Meta(chunk.ChunkID) (chunk.ChunkMeta, error)                        { return chunk.ChunkMeta{}, nil }
func (n *noopChunkManager) List() ([]chunk.ChunkMeta, error)                                   { return nil, nil }
func (n *noopChunkManager) Delete(chunk.ChunkID) error                                         { return nil }
func (n *noopChunkManager) OpenCursor(chunk.ChunkID) (chunk.RecordCursor, error)               { return nil, nil }
func (n *noopChunkManager) FindStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error)   { return 0, false, nil }
func (n *noopChunkManager) ReadWriteTimestamps(chunk.ChunkID, []uint64) ([]time.Time, error)   { return nil, nil }
func (n *noopChunkManager) SetRotationPolicy(chunk.RotationPolicy)                             {}
func (n *noopChunkManager) CheckRotation() *string                                             { return nil }
func (n *noopChunkManager) Close() error                                                       { return nil }
