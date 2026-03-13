package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// ejectConfigLoader implements ConfigLoader for eject tests.
type ejectConfigLoader struct {
	cfg *config.Config
}

func (f *ejectConfigLoader) Load(_ context.Context) (*config.Config, error) {
	return f.cfg, nil
}

// ---------- fake cursor ----------

type fakeCursor struct {
	records []chunk.Record
	pos     int
}

func (f *fakeCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if f.pos >= len(f.records) {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	rec := f.records[f.pos]
	f.pos++
	return rec, chunk.RecordRef{}, nil
}

func (f *fakeCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
}

func (f *fakeCursor) Seek(_ chunk.RecordRef) error { return nil }
func (f *fakeCursor) Close() error                 { return nil }

// ---------- eject fake chunk manager ----------

type ejectFakeChunkManager struct {
	retentionFakeChunkManager
	cursorRecords []chunk.Record // records returned by OpenCursor
}

func (f *ejectFakeChunkManager) OpenCursor(_ chunk.ChunkID) (chunk.RecordCursor, error) {
	return &fakeCursor{records: f.cursorRecords}, nil
}

// ---------- eject fake transferrer ----------

type ejectFakeTransferrer struct {
	calls   []ejectTransferCall
	failErr error
}

type ejectTransferCall struct {
	nodeID  string
	vaultID uuid.UUID
	records []chunk.Record
}

func (m *ejectFakeTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil // not used by eject
}

func (m *ejectFakeTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	if m.failErr != nil {
		return m.failErr
	}
	m.calls = append(m.calls, ejectTransferCall{nodeID: nodeID, vaultID: vaultID, records: records})
	return nil
}

func (m *ejectFakeTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}

// ---------- fake appendable orchestrator ----------

// ejectTestOrch wraps an Orchestrator with a recording Append path.
// Since ejectChunk calls r.orch.Append() and r.orch.loadConfig(), we need
// a real Orchestrator with registered vaults and a config loader.

func makeTestRecords(n int, attrs chunk.Attributes) []chunk.Record {
	records := make([]chunk.Record, n)
	for i := range records {
		records[i] = chunk.Record{
			SourceTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			IngestTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			WriteTS:  time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			Raw:      []byte("test-record"),
			Attrs:    attrs,
		}
	}
	return records
}

// ---------- tests ----------

func TestMatchesEjectFilter(t *testing.T) {
	t.Parallel()

	attrs := chunk.Attributes{"level": "error", "env": "prod"}

	t.Run("nil_filter_rejects", func(t *testing.T) {
		if matchesEjectFilter(nil, attrs) {
			t.Error("nil filter should reject")
		}
	})

	t.Run("catch_all", func(t *testing.T) {
		cf, err := CompileFilter(uuid.New(), "*")
		if err != nil {
			t.Fatal(err)
		}
		if !matchesEjectFilter(cf, attrs) {
			t.Error("catch-all should match")
		}
	})

	t.Run("catch_rest", func(t *testing.T) {
		cf, err := CompileFilter(uuid.New(), "+")
		if err != nil {
			t.Fatal(err)
		}
		if !matchesEjectFilter(cf, attrs) {
			t.Error("catch-rest should match in eject context")
		}
	})

	t.Run("matching_expression", func(t *testing.T) {
		cf, err := CompileFilter(uuid.New(), "level=error")
		if err != nil {
			t.Fatal(err)
		}
		if !matchesEjectFilter(cf, attrs) {
			t.Error("expression should match")
		}
	})

	t.Run("non_matching_expression", func(t *testing.T) {
		cf, err := CompileFilter(uuid.New(), "level=info")
		if err != nil {
			t.Fatal(err)
		}
		if matchesEjectFilter(cf, attrs) {
			t.Error("expression should not match")
		}
	})

	t.Run("filter_none", func(t *testing.T) {
		cf := &CompiledFilter{Kind: FilterNone}
		if matchesEjectFilter(cf, attrs) {
			t.Error("FilterNone should reject")
		}
	})
}

func TestEjectChunkLocalDelivery(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := makeTestRecords(5, chunk.Attributes{"level": "error"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	// Create orchestrator with dst vault registered.
	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-A"},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-route", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	dstCM := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{},
	}

	// Register destination vault so Append works.
	orch.RegisterVault(NewVault(dstVaultID, dstCM, &retentionFakeIndexManager{}, nil))

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Source chunk should be expired (deleted).
	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("expected source chunk %s to be deleted, got %v", chunkID, cm.deleted)
	}
}

func TestEjectChunkRemoteDelivery(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := makeTestRecords(3, chunk.Attributes{"level": "info"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"}, // remote
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-remote", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Verify remote transfer was called.
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 TransferRecords call, got %d", len(mock.calls))
	}
	call := mock.calls[0]
	if call.nodeID != "node-B" {
		t.Errorf("nodeID = %q, want %q", call.nodeID, "node-B")
	}
	if call.vaultID != dstVaultID {
		t.Errorf("vaultID = %s, want %s", call.vaultID, dstVaultID)
	}
	if len(call.records) != 3 {
		t.Errorf("expected 3 records, got %d", len(call.records))
	}

	// Source chunk should be deleted after successful delivery.
	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("expected source chunk deleted, got %v", cm.deleted)
	}
}

func TestEjectChunkFilterMatching(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	// Mix of matching and non-matching records.
	records := []chunk.Record{
		{Raw: []byte("match-1"), Attrs: chunk.Attributes{"level": "error"}, IngestTS: time.Now()},
		{Raw: []byte("skip-1"), Attrs: chunk.Attributes{"level": "info"}, IngestTS: time.Now()},
		{Raw: []byte("match-2"), Attrs: chunk.Attributes{"level": "error"}, IngestTS: time.Now()},
		{Raw: []byte("skip-2"), Attrs: chunk.Attributes{"level": "debug"}, IngestTS: time.Now()},
	}

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "level=error"}, // only matches error records
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-filtered", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Only 2 records should be transferred (level=error).
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(mock.calls))
	}
	if len(mock.calls[0].records) != 2 {
		t.Errorf("expected 2 filtered records, got %d", len(mock.calls[0].records))
	}

	// Source chunk deleted regardless (all records processed).
	if len(cm.deleted) != 1 {
		t.Errorf("expected source chunk deleted")
	}
}

func TestEjectChunkMultiRoutesFanOut(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstA := uuid.Must(uuid.NewV7())
	dstB := uuid.Must(uuid.NewV7())
	filterAll := uuid.Must(uuid.NewV7())
	filterErrors := uuid.Must(uuid.NewV7())
	routeA := uuid.Must(uuid.NewV7())
	routeB := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := []chunk.Record{
		{Raw: []byte("error-1"), Attrs: chunk.Attributes{"level": "error"}, IngestTS: time.Now()},
		{Raw: []byte("info-1"), Attrs: chunk.Attributes{"level": "info"}, IngestTS: time.Now()},
	}

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstA, NodeID: "node-B"},
			{ID: dstB, NodeID: "node-C"},
		},
		Filters: []config.FilterConfig{
			{ID: filterAll, Expression: "*"},
			{ID: filterErrors, Expression: "level=error"},
		},
		Routes: []config.RouteConfig{
			{ID: routeA, Name: "route-all", FilterID: &filterAll, Destinations: []uuid.UUID{dstA}, Enabled: true, EjectOnly: true},
			{ID: routeB, Name: "route-errors", FilterID: &filterErrors, Destinations: []uuid.UUID{dstB}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeA, routeB})

	// Route A (catch-all → dstA on node-B): 2 records
	// Route B (level=error → dstB on node-C): 1 record
	if len(mock.calls) != 2 {
		t.Fatalf("expected 2 TransferRecords calls, got %d", len(mock.calls))
	}

	// Build lookup by nodeID for deterministic assertion.
	callsByNode := make(map[string]ejectTransferCall)
	for _, c := range mock.calls {
		callsByNode[c.nodeID] = c
	}

	callA := callsByNode["node-B"]
	if len(callA.records) != 2 {
		t.Errorf("route-all: expected 2 records, got %d", len(callA.records))
	}
	callB := callsByNode["node-C"]
	if len(callB.records) != 1 {
		t.Errorf("route-errors: expected 1 record, got %d", len(callB.records))
	}
}

func TestEjectChunkAbortOnRemoteFailure(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := makeTestRecords(2, chunk.Attributes{"level": "info"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-fail", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{failErr: errTest}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Source chunk must NOT be deleted on failure.
	if len(cm.deleted) != 0 {
		t.Errorf("chunk should not be deleted after transfer failure, got %v", cm.deleted)
	}
}

func TestEjectChunkDisabledRouteSkipped(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := makeTestRecords(2, chunk.Attributes{"level": "info"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "disabled-route", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: false, EjectOnly: true},
		},
	}}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// All routes disabled → no valid routes → chunk not deleted.
	if len(cm.deleted) != 0 {
		t.Errorf("chunk should not be deleted when all routes are disabled")
	}
}

func TestEjectChunkNoFilter(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()

	records := makeTestRecords(3, chunk.Attributes{"level": "info"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: time.Now().Add(-time.Hour), WriteEnd: time.Now()},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"},
		},
		Routes: []config.RouteConfig{
			// Route with no filter ID — matchesEjectFilter returns false for nil.
			{ID: routeID, Name: "no-filter-route", Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Route with no filter → nil CompiledFilter → no records match.
	// But the chunk is still deleted (all records processed, zero delivered).
	if len(mock.calls) != 0 {
		t.Errorf("expected 0 transfers with no filter, got %d", len(mock.calls))
	}
	if len(cm.deleted) != 1 {
		t.Errorf("chunk should still be deleted after eject completes")
	}
}

func TestEjectChunkSweepIntegration(t *testing.T) {
	t.Parallel()

	// Verify the full retention sweep dispatches to ejectChunk correctly.
	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	chunkID := chunk.NewChunkID()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	chunkID2 := chunk.NewChunkID()
	records := makeTestRecords(2, chunk.Attributes{"level": "error"})

	cm := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{
			chunks: []chunk.ChunkMeta{
				{ID: chunkID, Sealed: true, WriteStart: base, WriteEnd: base.Add(30 * time.Minute)},
				{ID: chunkID2, Sealed: true, WriteStart: base.Add(time.Hour), WriteEnd: base.Add(90 * time.Minute)},
			},
		},
		cursorRecords: records,
	}

	loader := &ejectConfigLoader{cfg: &config.Config{
		Vaults: []config.VaultConfig{
			{ID: srcVaultID, NodeID: "node-A"},
			{ID: dstVaultID, NodeID: "node-B"},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-sweep", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch, err := New(Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRemoteTransferrer(mock)

	// Policy: keep 1 chunk → oldest chunk (chunkID) is flagged.
	r := &retentionRunner{
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		rules: []retentionRule{
			{
				policy:        chunk.NewCountRetentionPolicy(1),
				action:        config.RetentionActionEject,
				ejectRouteIDs: []uuid.UUID{routeID},
			},
		},
		orch:   orch,
		now:    time.Now,
		logger: slog.Default(),
	}

	r.sweep()

	// Verify eject was executed via sweep for the oldest chunk.
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 TransferRecords call via sweep, got %d", len(mock.calls))
	}
	if len(mock.calls[0].records) != 2 {
		t.Errorf("expected 2 records, got %d", len(mock.calls[0].records))
	}
	if len(cm.deleted) != 1 {
		t.Errorf("expected 1 source chunk deleted via sweep, got %d", len(cm.deleted))
	}
}

// errTest is a sentinel error for tests.
var errTest = errors.New("test error")
