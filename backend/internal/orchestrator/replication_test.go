package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// ---------- fake forwarder ----------

type replicationFakeForwarder struct{}

func (f *replicationFakeForwarder) Forward(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (f *replicationFakeForwarder) ForwardToBuffer(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}

// ---------- fake transferrer that records seal commands ----------

type replicationFakeTransferrer struct {
	sealCalls    []sealCall
	sealErr      error
	forwardCalls []transitionTransferCall
}

type sealCall struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	chunkID chunk.ChunkID
}

func (m *replicationFakeTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardAppend(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardTierAppend(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardSealTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
	if m.sealErr != nil {
		return m.sealErr
	}
	m.sealCalls = append(m.sealCalls, sealCall{nodeID: nodeID, vaultID: vaultID, tierID: tierID, chunkID: chunkID})
	return nil
}
func (m *replicationFakeTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}

// ---------- helpers ----------

func newReplicationTier(t *testing.T, tierID uuid.UUID, secondaries []string, isSecondary bool, primaryNodeID string) *TierInstance {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &TierInstance{
		TierID:           tierID,
		Type:             "memory",
		Chunks:           cm,
		Indexes:          im,
		Query:            query.New(cm, im, nil),
		IsSecondary:      isSecondary,
		PrimaryNodeID:    primaryNodeID,
		SecondaryNodeIDs: secondaries,
	}
}

func testRecord(raw string) chunk.Record {
	return chunk.Record{
		SourceTS: time.Now(),
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
	}
}

// ================================================================
// SEAL ACTIVE TIER TESTS
// ================================================================

func TestSealActiveTier(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	vault.Name = "seal-test"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("seal-me")); err != nil {
		t.Fatal(err)
	}

	active := vault.Tiers[0].Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	chunkID := active.ID

	if err := orch.SealActiveTier(vaultID, tierID, chunkID); err != nil {
		t.Fatal(err)
	}

	newActive := vault.Tiers[0].Chunks.Active()
	if newActive != nil && newActive.ID == chunkID {
		t.Error("expected active chunk to change after seal")
	}
}

func TestSealActiveTierMismatchSkipsSeal(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.logger = slog.Default()

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	vault.Name = "mismatch"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// Seal with a wrong chunk ID — should be a no-op (the expected chunk
	// was already rotated by the secondary's own rotation policy).
	wrongID := chunkIDAt(time.Now().Add(-1 * time.Hour))
	if err := orch.SealActiveTier(vaultID, tierID, wrongID); err != nil {
		t.Fatal(err)
	}

	metas, _ := vault.Tiers[0].Chunks.List()
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed != 0 {
		t.Error("expected NO seal when chunk ID doesn't match — seal should be skipped")
	}
}

func TestSealActiveTierTierNotFound(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	wrongTierID := uuid.Must(uuid.NewV7())
	err = orch.SealActiveTier(vaultID, wrongTierID, chunk.ChunkID{})
	if !errors.Is(err, ErrVaultNotFound) {
		t.Errorf("expected ErrVaultNotFound, got %v", err)
	}
}

func TestSealActiveTierNoActiveChunk(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	// No records appended — no active chunk.
	err = orch.SealActiveTier(vaultID, tierID, chunk.ChunkID{})
	if err != nil {
		t.Errorf("expected nil error for no active chunk, got %v", err)
	}
}

// ================================================================
// CATCHUP TESTS
// ================================================================

func TestCatchupSecondaryNoSealedChunks(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.logger = slog.Default()

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	mock := &replicationFakeTransferrer{}
	orch.transferrer = mock

	// No sealed chunks — catchup should be a no-op.
	err = orch.catchupSecondary(context.Background(), vaultID, tierID, "node-2")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCatchupSecondaryOnlyPrimary(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	// This is a secondary — should not initiate catchup.
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, true, "node-2"))
	orch.RegisterVault(vault)

	err = orch.catchupSecondary(context.Background(), vaultID, tierID, "node-3")
	if err != nil {
		t.Fatalf("expected nil (no-op) for secondary, got %v", err)
	}
}

func TestCatchupSecondaryNoTransferrer(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)
	// No transferrer set.

	err = orch.catchupSecondary(context.Background(), vaultID, tierID, "node-2")
	if err == nil {
		t.Fatal("expected error for missing transferrer")
	}
}
