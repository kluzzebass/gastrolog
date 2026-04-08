package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"

	"github.com/google/uuid"
)

// streamErrTransferrer is a minimal RemoteTransferrer that returns a
// configured error from StreamToTier. Other methods are no-ops because
// transitionChunk only calls StreamToTier on the remote path.
type streamErrTransferrer struct {
	err error
}

func (t *streamErrTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return t.err
}

// The other interface methods aren't called by transitionChunk but must
// exist to satisfy RemoteTransferrer.
func (t *streamErrTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func (t *streamErrTransferrer) ForwardAppend(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (t *streamErrTransferrer) ForwardTierAppend(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (t *streamErrTransferrer) ForwardSealTier(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (t *streamErrTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (t *streamErrTransferrer) ForwardDeleteChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (t *streamErrTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}

// setupRemoteTransitionRunner builds a retention runner for a two-tier vault
// where tier1 has a placement on a DIFFERENT node, forcing transitionChunk
// to take the remote (StreamToTier) path rather than the local streamLocal
// path. Returns the runner, the chunk ID to transition, and the tier0
// chunk manager (for asserting the chunk is still present after a failed
// transition).
func setupRemoteTransitionRunner(t *testing.T, transferrer RemoteTransferrer) (*retentionRunner, chunk.ChunkID, chunk.ChunkManager) {
	t.Helper()

	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	localNodeID := "local-node"
	remoteNodeID := "remote-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	tier1 := newMemoryTierInstance(t, tier1ID)

	orch := newTestOrch(t, Config{LocalNodeID: localNodeID})
	orch.SetRemoteTransferrer(transferrer)

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "test-vault"
	orch.RegisterVault(vault)

	// Config store — tier0 lives on local-node, tier1 lives on remote-node.
	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{ID: vaultID, Name: "test-vault"})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory,
		Placements: syntheticPlacements(localNodeID),
		VaultID:    vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeMemory,
		Placements: syntheticPlacements(remoteNodeID),
		VaultID:    vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest and seal a chunk on tier0.
	for i := range 3 {
		if _, _, err := tier0.Chunks.Append(chunk.Record{
			IngestTS: time.Now(),
			WriteTS:  time.Now(),
			Raw:      fmt.Appendf(nil, "rec-%d", i),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, _ := tier0.Chunks.List()
	if len(metas) != 1 {
		t.Fatalf("expected 1 sealed chunk on tier0, got %d", len(metas))
	}
	chunkID := metas[0].ID

	runner := &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   tier0ID,
		cm:       tier0.Chunks,
		im:       tier0.Indexes,
		orch:     orch,
		now:      time.Now,
		logger:   slog.Default(),
	}
	return runner, chunkID, tier0.Chunks
}

// TestTransitionChunkTransientErrorDoesNotMarkUnreadable is the regression
// test for gastrolog-50271. A transient destination-side error from
// StreamToTier (network blip, peer timeout, gRPC Unavailable, forwarder
// per-call timeout) must NOT permanently retire the source chunk via
// markUnreadable — the next retention sweep should be free to retry.
//
// Before the fix, ANY streamErr triggered markUnreadable, which meant a
// single transient network error would mark the chunk unreadable for the
// lifetime of the orchestrator process.
func TestTransitionChunkTransientErrorDoesNotMarkUnreadable(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("network unreachable")
	r, chunkID, _ := setupRemoteTransitionRunner(t, &streamErrTransferrer{err: transientErr})

	r.transitionChunk(chunkID)

	// The chunk must NOT be in the unreadable map — it's a transient
	// destination error, not a source chunk corruption.
	r.mu.Lock()
	marked := r.unreadable[chunkID]
	r.mu.Unlock()
	if marked {
		t.Errorf("transientError: chunk %s marked unreadable, expected retriable", chunkID)
	}
}

// TestTransitionChunkSourceReadErrorMarksUnreadable verifies the other
// branch: when the error IS wrapped with cluster.ErrSourceRead (meaning
// the source cursor's record iterator failed), the chunk SHOULD be marked
// unreadable. This preserves the safety net for genuinely corrupt chunks.
func TestTransitionChunkSourceReadErrorMarksUnreadable(t *testing.T) {
	t.Parallel()

	// Wrap the error with ErrSourceRead the same way chunk_transferrer does.
	sourceErr := fmt.Errorf("%w: transition: %w", cluster.ErrSourceRead, errors.New("corrupt record"))
	r, chunkID, _ := setupRemoteTransitionRunner(t, &streamErrTransferrer{err: sourceErr})

	r.transitionChunk(chunkID)

	// The chunk SHOULD be in the unreadable map — the source iterator
	// failed, meaning the chunk really is corrupt.
	r.mu.Lock()
	marked := r.unreadable[chunkID]
	r.mu.Unlock()
	if !marked {
		t.Errorf("ErrSourceRead: chunk %s should have been marked unreadable", chunkID)
	}
}
