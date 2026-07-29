package orchestrator

// schedulePostSeal registers "post-seal:<vault>:<chunk>". Several independent
// paths reach it for one chunk with no coordination — postSealWork from ingest,
// two direct calls in vault_ops, vault_drain, and the reconciler's
// sealLocalActive / sealMetadataOnlyOrphan — and plain RunOnce overwrites the
// registry entry without stopping the job already running, so the work would run
// twice. It is not idempotent: sealToGLCB rebuilds the GLCB from the record
// cursor with no already-built short-circuit, then re-announces and re-enters
// the upload path — the gastrolog-3hwngy shape (gastrolog-3xr5dk).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// postSealCountingManager counts PostSealProcess entries and can hold the first
// one open, so a second schedule lands while the first is genuinely in flight.
type postSealCountingManager struct {
	retentionFakeChunkManager
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (m *postSealCountingManager) PostSealProcess(_ context.Context, _ chunk.ChunkID) error {
	if m.calls.Add(1) == 1 {
		close(m.started)
		<-m.release
	}
	return nil
}

func (m *postSealCountingManager) SetIndexBuilders([]chunk.ChunkIndexBuilder) {}
func (m *postSealCountingManager) HasIndexBuilders() bool                     { return false }

func newPostSealCountingManager() *postSealCountingManager {
	return &postSealCountingManager{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func TestPostSealDoesNotRunTwiceForOneChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	cm := newPostSealCountingManager()
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{VaultID: vaultID, Type: "file", Chunks: cm}))

	orch.schedulePostSeal(vaultID, cm, chunkID)
	select {
	case <-cm.started: // the first job is inside PostSealProcess
	case <-time.After(5 * time.Second):
		t.Fatal("post-seal never ran: the fixture is not being scheduled")
	}

	// A second path post-seals the same chunk while the first is running.
	orch.schedulePostSeal(vaultID, cm, chunkID)

	close(cm.release)
	requireIdle(t, orch.scheduler, 5*time.Second)

	if got := cm.calls.Load(); got != 1 {
		t.Errorf("PostSealProcess ran %d times for one chunk, want 1: the GLCB is rebuilt "+
			"and re-announced on every run", got)
	}
}

// The claim covers in-flight work only. Once the job finishes the name frees,
// and a later post-seal of the same chunk must still run — otherwise a chunk
// re-sealed after a restart would never be processed again.
func TestPostSealRunsAgainAfterTheFirstCompletes(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	cm := newPostSealCountingManager()
	close(cm.release) // never hold
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{VaultID: vaultID, Type: "file", Chunks: cm}))

	orch.schedulePostSeal(vaultID, cm, chunkID)
	requireIdle(t, orch.scheduler, 5*time.Second)
	select {
	case <-cm.started:
	case <-time.After(5 * time.Second):
		t.Fatal("post-seal never ran: the fixture is not being scheduled")
	}

	orch.schedulePostSeal(vaultID, cm, chunkID)
	requireIdle(t, orch.scheduler, 5*time.Second)

	if got := cm.calls.Load(); got != 2 {
		t.Errorf("PostSealProcess ran %d times across two separate seals, want 2: "+
			"the claim must not outlive the job", got)
	}
}
