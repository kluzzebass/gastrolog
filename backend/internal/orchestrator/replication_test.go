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

// ---------- fake forwarder that records tier-targeted forwards ----------

type replicationFakeForwarder struct {
	entries []replicationForwardEntry
}

type replicationForwardEntry struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	records []chunk.Record
}

func (f *replicationFakeForwarder) Forward(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}

func (f *replicationFakeForwarder) ForwardToTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, records []chunk.Record) error {
	copied := make([]chunk.Record, len(records))
	for i, r := range records {
		copied[i] = r.Copy()
	}
	f.entries = append(f.entries, replicationForwardEntry{
		nodeID: nodeID, vaultID: vaultID, tierID: tierID, records: copied,
	})
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
// RECORD REPLICATION TESTS
// ================================================================

func TestReplicationRF1NoForwarding(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	vault.Name = "rf1"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("test")); err != nil {
		t.Fatal(err)
	}
	if len(fwd.entries) != 0 {
		t.Errorf("expected 0 forwards with RF=1, got %d", len(fwd.entries))
	}
}

func TestReplicationRF2ForwardsToSecondary(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2"}, false, ""))
	vault.Name = "rf2"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("replicated")); err != nil {
		t.Fatal(err)
	}
	if len(fwd.entries) != 1 {
		t.Fatalf("expected 1 forward, got %d", len(fwd.entries))
	}
	e := fwd.entries[0]
	if e.nodeID != "node-2" {
		t.Errorf("expected node-2, got %q", e.nodeID)
	}
	if e.tierID != tierID {
		t.Errorf("expected tierID %s, got %s", tierID, e.tierID)
	}
	if string(e.records[0].Raw) != "replicated" {
		t.Errorf("expected 'replicated', got %q", string(e.records[0].Raw))
	}
}

func TestReplicationSecondaryDoesNotReForward(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, true, "node-1"))
	vault.Name = "secondary"
	orch.RegisterVault(vault)

	if err := orch.AppendToTier(vaultID, tierID, testRecord("received")); err != nil {
		t.Fatal(err)
	}
	if len(fwd.entries) != 0 {
		t.Errorf("expected 0 forwards from secondary, got %d", len(fwd.entries))
	}
}

func TestReplicationMultipleSecondaries(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2", "node-3"}, false, ""))
	vault.Name = "rf3"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("multi")); err != nil {
		t.Fatal(err)
	}
	if len(fwd.entries) != 2 {
		t.Fatalf("expected 2 forwards, got %d", len(fwd.entries))
	}
	nodes := map[string]bool{}
	for _, e := range fwd.entries {
		nodes[e.nodeID] = true
	}
	if !nodes["node-2"] || !nodes["node-3"] {
		t.Errorf("expected node-2 and node-3, got %v", nodes)
	}
}

func TestReplicationMultipleRecordsSequence(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2"}, false, ""))
	vault.Name = "seq"
	orch.RegisterVault(vault)

	for i := 0; i < 10; i++ {
		if _, _, err := orch.Append(vaultID, testRecord("rec")); err != nil {
			t.Fatal(err)
		}
	}
	if len(fwd.entries) != 10 {
		t.Errorf("expected 10 forwards, got %d", len(fwd.entries))
	}
}

func TestReplicationRecordContentIntegrity(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2"}, false, ""))
	vault.Name = "integrity"
	orch.RegisterVault(vault)

	original := chunk.Record{
		SourceTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IngestTS: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
		Attrs:    chunk.Attributes{"env": "prod", "host": "web-1"},
		Raw:      []byte(`{"level":"info","msg":"hello world"}`),
	}
	if _, _, err := orch.Append(vaultID, original); err != nil {
		t.Fatal(err)
	}

	if len(fwd.entries) != 1 {
		t.Fatal("expected 1 forward")
	}
	forwarded := fwd.entries[0].records[0]
	if string(forwarded.Raw) != string(original.Raw) {
		t.Errorf("Raw mismatch: %q vs %q", forwarded.Raw, original.Raw)
	}
	if forwarded.Attrs["env"] != "prod" || forwarded.Attrs["host"] != "web-1" {
		t.Errorf("Attrs mismatch: %v", forwarded.Attrs)
	}
}

func TestReplicationAppendToTierAlsoReplicates(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tier0 := newReplicationTier(t, uuid.Must(uuid.NewV7()), nil, false, "")
	tier1ID := uuid.Must(uuid.NewV7())
	tier1 := newReplicationTier(t, tier1ID, []string{"node-3"}, false, "")

	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "append-to-tier"
	orch.RegisterVault(vault)

	// AppendToTier targets tier1, which has a secondary.
	if err := orch.AppendToTier(vaultID, tier1ID, testRecord("transition")); err != nil {
		t.Fatal(err)
	}
	if len(fwd.entries) != 1 {
		t.Fatalf("expected 1 forward from AppendToTier, got %d", len(fwd.entries))
	}
	if fwd.entries[0].tierID != tier1ID {
		t.Errorf("expected tierID %s, got %s", tier1ID, fwd.entries[0].tierID)
	}
}

// ================================================================
// SEAL SYNCHRONIZATION TESTS
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

func TestSealActiveTierMismatchStillSeals(t *testing.T) {
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
	if sealed == 0 {
		t.Error("expected sealed chunk despite ID mismatch")
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

func TestSealNotifiesSecondaries(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	mock := &replicationFakeTransferrer{}
	orch.transferrer = mock

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2", "node-3"}, false, ""))
	vault.Name = "seal-notify"
	orch.RegisterVault(vault)

	// Set a rotation policy that seals after 1 record.
	vault.Tiers[0].Chunks.SetRotationPolicy(chunk.NewRecordCountPolicy(1))

	// First append creates the active chunk. Second append triggers rotation → seal.
	if _, _, err := orch.Append(vaultID, testRecord("first")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := orch.Append(vaultID, testRecord("second")); err != nil {
		t.Fatal(err)
	}

	// Check that ForwardSealTier was called for both secondaries.
	if len(mock.sealCalls) != 2 {
		t.Fatalf("expected 2 seal calls, got %d", len(mock.sealCalls))
	}
	sealNodes := map[string]bool{}
	for _, sc := range mock.sealCalls {
		sealNodes[sc.nodeID] = true
		if sc.vaultID != vaultID {
			t.Errorf("expected vaultID %s, got %s", vaultID, sc.vaultID)
		}
		if sc.tierID != tierID {
			t.Errorf("expected tierID %s, got %s", tierID, sc.tierID)
		}
	}
	if !sealNodes["node-2"] || !sealNodes["node-3"] {
		t.Errorf("expected seal calls to node-2 and node-3, got %v", sealNodes)
	}
}

func TestSealSecondaryFailureDoesNotBlockPrimary(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	mock := &replicationFakeTransferrer{sealErr: errors.New("network error")}
	orch.transferrer = mock

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2"}, false, ""))
	vault.Name = "seal-fail"
	orch.RegisterVault(vault)
	orch.logger = slog.Default()

	vault.Tiers[0].Chunks.SetRotationPolicy(chunk.NewRecordCountPolicy(1))

	// This should not panic or error even though seal forwarding fails.
	if _, _, err := orch.Append(vaultID, testRecord("first")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := orch.Append(vaultID, testRecord("second")); err != nil {
		t.Fatal(err)
	}

	// Primary should still have sealed its chunk.
	metas, _ := vault.Tiers[0].Chunks.List()
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Error("expected primary to seal despite secondary failure")
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

// ================================================================
// DISABLED VAULT TESTS
// ================================================================

func TestReplicationDisabledVaultNoForwarding(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	fwd := &replicationFakeForwarder{}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, []string{"node-2"}, false, ""))
	vault.Name = "disabled"
	vault.Enabled = false
	orch.RegisterVault(vault)

	// Append to disabled vault should fail.
	_, _, err = orch.Append(vaultID, testRecord("should-fail"))
	if err == nil {
		t.Fatal("expected error for disabled vault")
	}
	// No forwarding should have happened.
	if len(fwd.entries) != 0 {
		t.Errorf("expected 0 forwards for disabled vault, got %d", len(fwd.entries))
	}
}
