package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// streamErrTransferrer is a minimal RemoteTransferrer that returns a
// configured error from StreamToTier. Other methods are no-ops because
// transitionChunk only calls StreamToTier on the remote path.
type streamErrTransferrer struct {
	err error
}

func (t *streamErrTransferrer) StreamToTier(_ context.Context, _ string, _, _ glid.GLID, _ chunk.RecordIterator) error {
	return t.err
}

// The other interface methods aren't called by transitionChunk but must
// exist to satisfy RemoteTransferrer.
func (t *streamErrTransferrer) TransferRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.RecordIterator) error {
	return nil
}
func (t *streamErrTransferrer) ForwardAppend(_ context.Context, _ string, _ glid.GLID, _ []chunk.Record) error {
	return nil
}
func (t *streamErrTransferrer) WaitVaultReady(_ context.Context, _ string, _ glid.GLID) error {
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

	vaultID := glid.New()
	tier0ID := glid.New()
	tier1ID := glid.New()
	localNodeID := "local-node"
	_ = "remote-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	tier1 := newMemoryTierInstance(t, tier1ID)

	orch := newTestOrch(t, Config{LocalNodeID: localNodeID})
	orch.SetRemoteTransferrer(transferrer)

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "test-vault"
	orch.RegisterVault(vault)

	// Config store — tier0 lives on local-node, tier1 lives on remote-node.
	store := sysmem.NewStore()
	_ = store.PutVault(context.Background(), system.VaultConfig{ID: vaultID, Name: "test-vault"})
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tier0ID, Name: "hot", Type: system.TierTypeMemory,
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tier1ID, Name: "warm", Type: system.TierTypeMemory,
		VaultID: vaultID, Position: 1,
	})
	orch.sysLoader = &transitionSystemLoader{store: store}

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
	entry := r.unreadable[chunkID]
	r.mu.Unlock()
	if entry != nil {
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
	entry := r.unreadable[chunkID]
	r.mu.Unlock()
	if entry == nil {
		t.Errorf("ErrSourceRead: chunk %s should have been marked unreadable", chunkID)
	}
}

// faultingCursor is a minimal RecordCursor that fails Next() with a
// configured error. Used to exercise the local transition path's
// error-classification behavior without needing an actually-corrupted
// chunk on disk.
type faultingCursor struct{ err error }

func (c *faultingCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	return chunk.Record{}, chunk.RecordRef{}, c.err
}
func (c *faultingCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	return chunk.Record{}, chunk.RecordRef{}, c.err
}
func (*faultingCursor) Seek(chunk.RecordRef) error { return nil }
func (*faultingCursor) Close() error               { return nil }

// TestStreamLocalWrapsCursorErrorsAsSourceRead pins gastrolog-3ayz3:
// read-side errors from the source cursor on the LOCAL transition path
// must be wrapped in cluster.ErrSourceRead so transitionChunk's classifier
// calls markUnreadable. Without this, a corrupted chunk's idx.log /
// attr.log error would loop forever through the retention sweep, flooding
// the logs with one ERROR per minute per corrupted chunk.
func TestStreamLocalWrapsCursorErrorsAsSourceRead(t *testing.T) {
	t.Parallel()

	r := &retentionRunner{
		vaultID: glid.New(),
		tierID:  glid.New(),
	}
	raw := errors.New("invalid idx.log entry: attr range [3246:3288] exceeds mmap size 3270")
	err := r.streamLocal(&faultingCursor{err: raw}, glid.New())
	if err == nil {
		t.Fatal("streamLocal: expected error from faulting cursor, got nil")
	}
	if !errors.Is(err, cluster.ErrSourceRead) {
		t.Errorf("streamLocal: expected error to wrap cluster.ErrSourceRead, got: %v", err)
	}
	if !errors.Is(err, raw) {
		t.Errorf("streamLocal: expected error chain to preserve underlying cause, got: %v", err)
	}
}
