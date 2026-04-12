package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/system"

	"github.com/google/uuid"
)

// mockForwarder records Forward() and ForwardSync() calls for testing,
// with optional error injection for ForwardSync to exercise the
// ack-gated error propagation path.
type mockForwarder struct {
	mu       sync.Mutex
	calls    []forwardCall
	syncErr  error // if set, ForwardSync returns this error
	syncWait chan struct{}
}

type forwardCall struct {
	NodeID  string
	VaultID uuid.UUID
	Records []chunk.Record
	Sync    bool
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

func (m *mockForwarder) ForwardSync(ctx context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	m.mu.Lock()
	m.calls = append(m.calls, forwardCall{
		NodeID:  nodeID,
		VaultID: vaultID,
		Records: records,
		Sync:    true,
	})
	wait := m.syncWait
	err := m.syncErr
	m.mu.Unlock()

	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (m *mockForwarder) RegisterPressureGate(_ *chanwatch.PressureGate) {
	// Test mock: pressure gate integration is verified in record_forwarder_test.go.
}

func (m *mockForwarder) getCalls() []forwardCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]forwardCall(nil), m.calls...)
}

// mockConfigLoader returns a fixed config for testing.
type mockConfigLoader struct {
	cfg *system.Config
}

func (m *mockConfigLoader) Load(_ context.Context) (*system.Config, error) {
	return m.cfg, nil
}

func TestIngestForwardsToRemoteVault(t *testing.T) {
	localVaultID := uuid.Must(uuid.NewV7())
	remoteVaultID := uuid.Must(uuid.NewV7())
	remoteNodeID := "node-B"

	fwd := &mockForwarder{}

	o, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	o.SetRecordForwarder(fwd)

	// Register a local vault with a simple mock chunk manager.
	localVault := NewVaultFromComponents(localVaultID, &noopChunkManager{}, nil, nil)
	localVault.Enabled = true
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

	o, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	// No forwarder set (single-node mode).

	localVault := NewVaultFromComponents(localVaultID, &noopChunkManager{}, nil, nil)
	localVault.Enabled = true
	o.vaults[localVaultID] = localVault

	// Filter set with local + remote.
	localFilter, _ := CompileFilter(localVaultID, "*")
	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = "node-B"

	o.filterSet = NewFilterSet([]*CompiledFilter{localFilter, remoteFilter})

	// Test that reloadFiltersFromRoutes correctly skips remote
	// vaults when no forwarder is set.
	o.filterSet = nil
	o.cfgLoader = &mockConfigLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{
				ID:           uuid.Must(uuid.NewV7()),
				Enabled:      true,
				Destinations: []uuid.UUID{localVaultID, remoteVaultID},
			},
		},
		Vaults: []system.VaultConfig{
			{ID: localVaultID},
			{ID: remoteVaultID},
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

// TestReloadFiltersIncludesRemoteWhenForwarderSet and
// TestRebuildFilterSetPreservesRemoteFilters were removed: they tested the
// concept of NodeID-based remote vault filters which no longer exists.
// Remote vault routing will be reintroduced via tier primary election.

// noopChunkManager satisfies the ChunkManager interface for tests
// that only need the ingest path (no actual storage).
type noopChunkManager struct{}

func (n *noopChunkManager) Append(chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (n *noopChunkManager) Seal() error                                                        { return nil }
func (n *noopChunkManager) Active() *chunk.ChunkMeta                                           { return &chunk.ChunkMeta{} }
func (n *noopChunkManager) Meta(chunk.ChunkID) (chunk.ChunkMeta, error)                        { return chunk.ChunkMeta{}, nil }
func (n *noopChunkManager) List() ([]chunk.ChunkMeta, error)                                   { return nil, nil }
func (n *noopChunkManager) Delete(chunk.ChunkID) error                                         { return nil }
func (n *noopChunkManager) OpenCursor(chunk.ChunkID) (chunk.RecordCursor, error)               { return nil, nil }
func (n *noopChunkManager) FindStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error)        { return 0, false, nil }
func (n *noopChunkManager) FindIngestStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error) { return 0, false, nil }
func (n *noopChunkManager) FindSourceStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error) { return 0, false, nil }
func (n *noopChunkManager) ReadWriteTimestamps(chunk.ChunkID, []uint64) ([]time.Time, error)       { return nil, nil }
func (n *noopChunkManager) SetRotationPolicy(chunk.RotationPolicy) {
	// No-op: forward_test.go uses noopChunkManager as a stand-in for
	// routing tests that never exercise the rotation policy surface.
}
func (n *noopChunkManager) CheckRotation() *string                                             { return nil }
func (n *noopChunkManager) ImportRecords(chunk.RecordIterator) (chunk.ChunkMeta, error)        { return chunk.ChunkMeta{}, nil }
func (n *noopChunkManager) ScanAttrs(_ chunk.ChunkID, _ uint64, _ func(time.Time, chunk.Attributes) bool) error {
	return nil
}
func (n *noopChunkManager) SetNextChunkID(_ chunk.ChunkID) {
	// No-op: routing tests don't replicate chunks, so SetNextChunkID
	// is never observed.
}
func (n *noopChunkManager) Close() error                   { return nil }

// --- gastrolog-27zvt: ack-gated cross-node forwarding ---

// TestAckGatedRemoteRecordUsesForwardSync verifies the durability fix
// for ack-gated records routed to remote vaults: when rec.WaitForReplica
// is true and the filter match points at another node, ingest() must
// accumulate a forwardTask (not call fire-and-forget forwardRemote), and
// ackAfterReplication must invoke ForwardSync through the RecordForwarder.
//
// Before gastrolog-27zvt, this code path was a silent durability hole —
// the ack fired immediately because ingest() returned no task, and the
// remote forward was fire-and-forget drop-on-full.
func TestAckGatedRemoteRecordUsesForwardSync(t *testing.T) {
	t.Parallel()

	remoteVaultID := uuid.Must(uuid.NewV7())
	remoteNodeID := "node-B"

	fwd := &mockForwarder{}
	o, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	o.SetRecordForwarder(fwd)

	// Single remote filter — no local vault registered.
	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = remoteNodeID
	o.SetFilterSet(NewFilterSet([]*CompiledFilter{remoteFilter}))

	rec := chunk.Record{
		Attrs:          chunk.Attributes{"env": "prod"},
		Raw:            []byte("ack-me-remote"),
		WaitForReplica: true,
	}

	pa, err := o.ingest(rec)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if pa.isEmpty() {
		t.Fatal("expected non-empty pendingAcks for ack-gated remote match")
	}
	if len(pa.forwards) != 1 {
		t.Fatalf("expected 1 forward task, got %d", len(pa.forwards))
	}
	if pa.forwards[0].nodeID != remoteNodeID {
		t.Errorf("forward task nodeID = %q, want %q", pa.forwards[0].nodeID, remoteNodeID)
	}
	if pa.forwards[0].vaultID != remoteVaultID {
		t.Errorf("forward task vaultID = %s, want %s", pa.forwards[0].vaultID, remoteVaultID)
	}

	// Verify that at ingest time, NO forward has happened yet — the
	// task is accumulated, not dispatched. The dispatch is the job of
	// ackAfterReplication, which runs later from the writeLoop's
	// goroutine.
	if got := len(fwd.getCalls()); got != 0 {
		t.Errorf("expected no forwarder calls during ingest (task is accumulated), got %d", got)
	}

	// Drive the ack path and wait for the ack.
	ack := make(chan error, 1)
	o.ackAfterReplication(ack, pa, rec)

	select {
	case err := <-ack:
		if err != nil {
			t.Errorf("expected nil ack, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack did not fire")
	}

	calls := fwd.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d: %+v", len(calls), calls)
	}
	if !calls[0].Sync {
		t.Error("expected ForwardSync, got fire-and-forget Forward")
	}
	if calls[0].NodeID != remoteNodeID {
		t.Errorf("call nodeID = %q, want %q", calls[0].NodeID, remoteNodeID)
	}
}

// TestFireAndForgetRemoteKeepsOldBehavior verifies that when
// WaitForReplica is NOT set, remote forwarding still uses the
// fire-and-forget Forward path. This guards against regressions that
// would push all cross-node forwards through the slower sync path.
func TestFireAndForgetRemoteKeepsOldBehavior(t *testing.T) {
	t.Parallel()

	remoteVaultID := uuid.Must(uuid.NewV7())
	remoteNodeID := "node-B"

	fwd := &mockForwarder{}
	o, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	o.SetRecordForwarder(fwd)

	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = remoteNodeID
	o.SetFilterSet(NewFilterSet([]*CompiledFilter{remoteFilter}))

	rec := chunk.Record{
		Attrs: chunk.Attributes{"env": "prod"},
		Raw:   []byte("fire-and-forget"),
		// WaitForReplica: false
	}

	pa, err := o.ingest(rec)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if !pa.isEmpty() {
		t.Errorf("expected empty pendingAcks for fire-and-forget, got %+v", pa)
	}

	// The fire-and-forget call happens INLINE in ingest, so it should
	// already be recorded.
	calls := fwd.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}
	if calls[0].Sync {
		t.Error("expected fire-and-forget Forward, got ForwardSync")
	}
}

// TestAckGatedForwardPropagatesError verifies that when ForwardSync
// returns an error, the ack channel gets that error — the durability
// guarantee requires the ingester to see the failure.
func TestAckGatedForwardPropagatesError(t *testing.T) {
	t.Parallel()

	remoteVaultID := uuid.Must(uuid.NewV7())
	remoteNodeID := "node-B"

	injected := errors.New("simulated forward failure")
	fwd := &mockForwarder{syncErr: injected}
	o, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	o.SetRecordForwarder(fwd)

	remoteFilter, _ := CompileFilter(remoteVaultID, "*")
	remoteFilter.NodeID = remoteNodeID
	o.SetFilterSet(NewFilterSet([]*CompiledFilter{remoteFilter}))

	rec := chunk.Record{
		Attrs:          chunk.Attributes{"env": "prod"},
		Raw:            []byte("will-fail"),
		WaitForReplica: true,
	}

	pa, err := o.ingest(rec)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	ack := make(chan error, 1)
	o.ackAfterReplication(ack, pa, rec)

	select {
	case err := <-ack:
		if err == nil {
			t.Fatal("expected non-nil ack error")
		}
		if !errors.Is(err, injected) {
			t.Errorf("expected injected error in ack, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack did not fire")
	}
}

