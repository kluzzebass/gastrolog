package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"fmt"
	"os"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"

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

func (m *ejectFakeTransferrer) ForwardTierAppend(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
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

// ---------- recording chunk manager for destination vaults ----------

// appendRecordingCM records all Append calls for test assertions.
type appendRecordingCM struct {
	noopChunkManager
	appended []chunk.Record
}

func (a *appendRecordingCM) Append(rec chunk.Record) (chunk.ChunkID, uint64, error) {
	a.appended = append(a.appended, rec)
	return chunk.ChunkID{}, uint64(len(a.appended)), nil
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-route", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})

	dstCM := &ejectFakeChunkManager{
		retentionFakeChunkManager: retentionFakeChunkManager{},
	}

	// Register destination vault so Append works.
	orch.RegisterVault(NewVaultFromComponents(dstVaultID, dstCM, &retentionFakeIndexManager{}, nil))

	r := &retentionRunner{
		isLeader: true,
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

func TestEjectChunkDeliveryToSeparateVault(t *testing.T) {
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-separate", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	dstCM := &appendRecordingCM{}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	// Register destination vault locally.
	orch.RegisterVault(NewVaultFromComponents(dstVaultID, dstCM, nil, nil))

	r := &retentionRunner{
		isLeader: true,
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Verify records were appended to destination vault.
	if len(dstCM.appended) != 3 {
		t.Fatalf("expected 3 appended records, got %d", len(dstCM.appended))
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "level=error"}, // only matches error records
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-filtered", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	dstCM := &appendRecordingCM{}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	// Register destination vault locally.
	orch.RegisterVault(NewVaultFromComponents(dstVaultID, dstCM, nil, nil))

	r := &retentionRunner{
		isLeader: true,
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeID})

	// Only 2 records should be appended (level=error).
	if len(dstCM.appended) != 2 {
		t.Fatalf("expected 2 filtered records, got %d", len(dstCM.appended))
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
			{ID: srcVaultID},
			{ID: dstA},
			{ID: dstB},
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

	dstACM := &appendRecordingCM{}
	dstBCM := &appendRecordingCM{}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	// Register destination vaults locally.
	orch.RegisterVault(NewVaultFromComponents(dstA, dstACM, nil, nil))
	orch.RegisterVault(NewVaultFromComponents(dstB, dstBCM, nil, nil))

	r := &retentionRunner{
		isLeader: true,
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.ejectChunk(chunkID, []uuid.UUID{routeA, routeB})

	// Route A (catch-all → dstA): 2 records
	// Route B (level=error → dstB): 1 record
	if len(dstACM.appended) != 2 {
		t.Errorf("route-all: expected 2 records, got %d", len(dstACM.appended))
	}
	if len(dstBCM.appended) != 1 {
		t.Errorf("route-errors: expected 1 record, got %d", len(dstBCM.appended))
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-fail", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{failErr: errTest}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		isLeader: true,
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "disabled-route", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: false, EjectOnly: true},
		},
	}}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})

	r := &retentionRunner{
		isLeader: true,
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Routes: []config.RouteConfig{
			// Route with no filter ID — matchesEjectFilter returns false for nil.
			{ID: routeID, Name: "no-filter-route", Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	mock := &ejectFakeTransferrer{}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	orch.SetRemoteTransferrer(mock)

	r := &retentionRunner{
		isLeader: true,
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
			{ID: srcVaultID},
			{ID: dstVaultID},
		},
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []config.RouteConfig{
			{ID: routeID, Name: "eject-sweep", FilterID: &filterID, Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true},
		},
	}}

	dstCM := &appendRecordingCM{}

	orch := newTestOrch(t, Config{
		ConfigLoader: loader,
		LocalNodeID:  "node-A",
	})
	// Register destination vault locally.
	orch.RegisterVault(NewVaultFromComponents(dstVaultID, dstCM, nil, nil))

	// Policy: keep 1 chunk → oldest chunk (chunkID) is flagged.
	rules := []retentionRule{
		{
			policy:        chunk.NewCountRetentionPolicy(1),
			action:        config.RetentionActionEject,
			ejectRouteIDs: []uuid.UUID{routeID},
		},
	}
	r := &retentionRunner{
		isLeader: true,
		vaultID: srcVaultID,
		cm:      cm,
		im:      &retentionFakeIndexManager{},
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	r.sweep(rules)

	// Verify eject was executed via sweep for the oldest chunk.
	if len(dstCM.appended) != 2 {
		t.Fatalf("expected 2 appended records via sweep, got %d", len(dstCM.appended))
	}
	if len(cm.deleted) != 1 {
		t.Errorf("expected 1 source chunk deleted via sweep, got %d", len(cm.deleted))
	}
}

// errTest is a sentinel error for tests.
var errTest = errors.New("test error")

func (m *ejectFakeTransferrer) ForwardSealTier(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *ejectFakeTransferrer) ForwardDeleteChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *ejectFakeTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (m *ejectFakeTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}

// ==========================================================================
// File-backed eject tests
//
// These use real chunkfile.Manager instances with filesystem directories.
// Verifies records arrive in destination via cursor AND source chunk
// directories are removed from disk.
// ==========================================================================

// TestEjectChunkFileBackedLocalDelivery ejects records from a file-backed
// source vault to a file-backed destination vault on the same node.
// Verifies via cursor reads and filesystem checks.
func TestEjectChunkFileBackedLocalDelivery(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	srcTierID := uuid.Must(uuid.NewV7())
	dstTierID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())
	nodeID := "node-A"

	// Config store with source vault, destination vault, filter, and eject route.
	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: srcVaultID, Name: "src",
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: dstVaultID, Name: "dst",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: srcTierID, Name: "src-hot", Type: config.TierTypeFile,
		Placements: syntheticPlacements(nodeID),
		VaultID: srcVaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: dstTierID, Name: "dst-hot", Type: config.TierTypeFile,
		Placements: syntheticPlacements(nodeID),
		VaultID: dstVaultID, Position: 0,
	})
	_ = store.PutFilter(context.Background(), config.FilterConfig{
		ID: filterID, Name: "catch-all", Expression: "*",
	})
	_ = store.PutRoute(context.Background(), config.RouteConfig{
		ID: routeID, Name: "eject-route", FilterID: &filterID,
		Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true,
	})

	// File-backed source vault.
	srcDir := t.TempDir()
	srcCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            srcDir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(50),
	})
	if err != nil {
		t.Fatal(err)
	}
	srcIM := indexfile.NewManager(srcDir, nil, nil)

	// File-backed destination vault.
	dstDir := t.TempDir()
	dstCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dstDir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	dstIM := indexfile.NewManager(dstDir, nil, nil)

	orch := newTestOrch(t, Config{
		ConfigLoader: &transitionConfigLoader{store: store},
		LocalNodeID:  nodeID,
	})

	srcTier := &TierInstance{TierID: srcTierID, Type: "file", Chunks: srcCM, Indexes: srcIM, Query: query.New(srcCM, srcIM, nil)}
	dstTier := &TierInstance{TierID: dstTierID, Type: "file", Chunks: dstCM, Indexes: dstIM, Query: query.New(dstCM, dstIM, nil)}

	orch.RegisterVault(NewVault(srcVaultID, srcTier))
	orch.RegisterVault(NewVault(dstVaultID, dstTier))

	t.Cleanup(func() {
		orch.Stop()
		_ = srcCM.Close()
		_ = dstCM.Close()
	})

	// Ingest 200 records into source (4 sealed chunks × 50 records).
	const totalRecords = 200
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := orch.AppendToTier(srcVaultID, srcTierID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "eject-%d", i),
			Attrs:    chunk.Attributes{"level": "error"},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := srcCM.Active(); active != nil && active.RecordCount > 0 {
		_ = srcCM.Seal()
	}

	// PostSealProcess source chunks (compress so cursors work on sealed chunks).
	metas, _ := srcCM.List()
	t.Logf("source: %d sealed chunks", len(metas))
	for _, m := range metas {
		if err := srcCM.PostSealProcess(context.Background(), m.ID); err != nil {
			t.Fatalf("PostSealProcess(%s): %v", m.ID, err)
		}
	}

	// Capture source chunk dirs before eject.
	srcEntriesBefore, _ := os.ReadDir(srcDir)
	var srcChunkDirsBefore int
	for _, e := range srcEntriesBefore {
		if e.IsDir() && len(e.Name()) == 26 {
			srcChunkDirsBefore++
		}
	}
	if srcChunkDirsBefore == 0 {
		t.Fatal("expected chunk directories on disk before eject")
	}

	// Eject each sealed chunk.
	runner := &retentionRunner{
		isLeader:        true,
		vaultID:         srcVaultID,
		tierID:          srcTierID,
		cm:              srcCM,
		im:              srcIM,
		orch:            orch,
		now:             time.Now,
		logger:          slog.Default(),
	}
	for _, m := range metas {
		runner.ejectChunk(m.ID, []uuid.UUID{routeID})
	}

	// ---- Verify: destination has all records (cursor-verified) ----
	dstRecords := readAllRecords(t, dstCM)
	if len(dstRecords) != totalRecords {
		t.Errorf("destination: cursor read %d records, expected %d", len(dstRecords), totalRecords)
	}

	// ---- Verify: source has 0 records (cursor-verified) ----
	srcRemaining := cursorCountRecords(t, srcCM)
	if srcRemaining != 0 {
		t.Errorf("source: cursor read %d records after eject (should be 0)", srcRemaining)
	}

	// ---- Verify: source chunk directories removed from disk ----
	srcEntriesAfter, _ := os.ReadDir(srcDir)
	var srcChunkDirsAfter int
	for _, e := range srcEntriesAfter {
		if e.IsDir() && len(e.Name()) == 26 {
			srcChunkDirsAfter++
		}
	}
	if srcChunkDirsAfter > 0 {
		t.Errorf("source: %d chunk directories still on disk after eject", srcChunkDirsAfter)
	}
}

// TestEjectChunkFileBackedRemoteDelivery ejects records from a file-backed
// source vault on node-A to a file-backed destination vault on node-B via
// directTransferrer. Verifies records arrive on the remote node.
func TestEjectChunkFileBackedRemoteDelivery(t *testing.T) {
	t.Parallel()

	srcVaultID := uuid.Must(uuid.NewV7())
	dstVaultID := uuid.Must(uuid.NewV7())
	srcTierID := uuid.Must(uuid.NewV7())
	dstTierID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: srcVaultID, Name: "src",
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: dstVaultID, Name: "dst",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: srcTierID, Name: "src-hot", Type: config.TierTypeFile,
		Placements: syntheticPlacements("node-A"),
		VaultID: srcVaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: dstTierID, Name: "dst-hot", Type: config.TierTypeFile,
		Placements: syntheticPlacements("node-B"),
		VaultID: dstVaultID, Position: 0,
	})
	_ = store.PutFilter(context.Background(), config.FilterConfig{
		ID: filterID, Name: "catch-all", Expression: "*",
	})
	_ = store.PutRoute(context.Background(), config.RouteConfig{
		ID: routeID, Name: "eject-route", FilterID: &filterID,
		Destinations: []uuid.UUID{dstVaultID}, Enabled: true, EjectOnly: true,
	})

	// Node-A (source).
	srcDir := t.TempDir()
	srcCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir: srcDir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(50),
	})
	if err != nil {
		t.Fatal(err)
	}
	srcIM := indexfile.NewManager(srcDir, nil, nil)

	orchA, err := New(Config{
		ConfigLoader: &transitionConfigLoader{store: store},
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	srcTier := &TierInstance{TierID: srcTierID, Type: "file", Chunks: srcCM, Indexes: srcIM, Query: query.New(srcCM, srcIM, nil)}
	orchA.RegisterVault(NewVault(srcVaultID, srcTier))

	// Node-B (destination).
	dstDir := t.TempDir()
	dstCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dstDir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	dstIM := indexfile.NewManager(dstDir, nil, nil)

	orchB, err := New(Config{
		ConfigLoader: &transitionConfigLoader{store: store},
		LocalNodeID:  "node-B",
	})
	if err != nil {
		t.Fatal(err)
	}
	dstTier := &TierInstance{TierID: dstTierID, Type: "file", Chunks: dstCM, Indexes: dstIM, Query: query.New(dstCM, dstIM, nil)}
	orchB.RegisterVault(NewVault(dstVaultID, dstTier))

	// Wire transferrer.
	orchA.SetRemoteTransferrer(&directTransferrer{nodes: map[string]*Orchestrator{"node-B": orchB}})

	t.Cleanup(func() {
		orchA.Stop()
		orchB.Stop()
		_ = srcCM.Close()
		_ = dstCM.Close()
	})

	// Ingest 200 records on node-A.
	const totalRecords = 200
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := orchA.AppendToTier(srcVaultID, srcTierID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "remote-eject-%d", i),
			Attrs: chunk.Attributes{"level": "error"},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := srcCM.Active(); active != nil && active.RecordCount > 0 {
		_ = srcCM.Seal()
	}

	metas, _ := srcCM.List()
	t.Logf("source: %d sealed chunks", len(metas))
	for _, m := range metas {
		_ = srcCM.PostSealProcess(context.Background(), m.ID)
	}

	// Eject to remote node.
	runner := &retentionRunner{
		isLeader: true,
		vaultID:  srcVaultID,
		tierID:   srcTierID,
		cm:       srcCM,
		im:       srcIM,
		orch:     orchA,
		now:      time.Now,
		logger:   slog.Default(),
	}
	for _, m := range metas {
		runner.ejectChunk(m.ID, []uuid.UUID{routeID})
	}

	// ---- Verify: node-B has all records (cursor-verified) ----
	dstCount := cursorCountRecords(t, dstCM)
	if dstCount != totalRecords {
		t.Errorf("node-B: cursor read %d records, expected %d", dstCount, totalRecords)
	}

	// ---- Verify: node-A source chunk dirs removed from disk ----
	entries, _ := os.ReadDir(srcDir)
	var chunkDirs int
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			chunkDirs++
		}
	}
	if chunkDirs > 0 {
		t.Errorf("node-A: %d source chunk directories still on disk", chunkDirs)
	}
}
