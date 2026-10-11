package orchestrator

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
)

// blockingIndexManager reports every chunk's indexes as incomplete and holds
// each build until release closes, so a scheduled index rebuild stays pending.
type blockingIndexManager struct {
	index.IndexManager
	release <-chan struct{}
}

func (m blockingIndexManager) IndexesComplete(chunk.ChunkID) (bool, error) { return false, nil }

func (m blockingIndexManager) BuildIndexes(context.Context, chunk.ChunkID) error {
	<-m.release
	return nil
}

// pendingChunkJobs is one vault's set of pending per-chunk jobs.
type pendingChunkJobs struct {
	vaultID     glid.GLID
	postSeal    string
	rebuild     string
	sweep       string
	glcbCatchup string
	cloudUpload string
}

func (p pendingChunkJobs) names() []string {
	return []string{p.postSeal, p.rebuild, p.sweep, p.glcbCatchup, p.cloudUpload}
}

// registerVaultWithPendingJobs registers a vault on o and leaves a post-seal
// job, an index-rebuild job, a home index sweep, a replica catch-up pull and
// a cloud upload pending for it. The index rebuild is scheduled through the
// production scheduling path.
func registerVaultWithPendingJobs(t *testing.T, o *Orchestrator, release <-chan struct{}) pendingChunkJobs {
	t.Helper()
	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	inst.Indexes = blockingIndexManager{IndexManager: inst.Indexes, release: release}
	o.RegisterVault(NewVault(vaultID, inst))

	chunkID := chunk.NewChunkID()
	p := pendingChunkJobs{
		vaultID:     vaultID,
		postSeal:    postSealJobName(vaultID, chunkID),
		rebuild:     indexRebuildJobName(vaultID, chunkID),
		sweep:       homeIndexSweepJobName(vaultID),
		glcbCatchup: glcbCatchupJobName(vaultID, chunkID),
		cloudUpload: cloudUploadJobName(vaultID, chunkID),
	}
	block := func() { <-release }
	for _, name := range []string{p.postSeal, p.sweep, p.glcbCatchup, p.cloudUpload} {
		if err := o.Scheduler().RunOnce(name, block); err != nil {
			t.Fatalf("schedule %s: %v", name, err)
		}
	}
	o.scheduleIndexRebuildIfNeeded(context.Background(), vaultID, inst, chunk.ChunkMeta{ID: chunkID, Sealed: true})
	o.markBackfillFailure(vaultID, chunkID, errors.New("upload failed"), true)

	for _, name := range p.names() {
		if !o.Scheduler().HasJob(name) {
			t.Fatalf("premise: job %q is pending before teardown", name)
		}
	}
	if !o.vaultHasBackfillFailures(vaultID) {
		t.Fatal("premise: cloud-upload backoff state is on record before teardown")
	}
	return p
}

// Every path that closes a vault's managers cancels that vault's pending
// per-chunk jobs and cloud-upload backoff state, and leaves every other
// vault's alone.
func TestVaultTeardownCancelsPendingChunkJobs(t *testing.T) {
	t.Parallel()
	paths := []struct {
		name     string
		teardown func(t *testing.T, o *Orchestrator, vaultID glid.GLID)
	}{
		{"teardownVault", func(_ *testing.T, o *Orchestrator, vaultID glid.GLID) {
			o.teardownVault(vaultID, o.vaults[vaultID])
		}},
		{"UnregisterVault", func(t *testing.T, o *Orchestrator, vaultID glid.GLID) {
			if err := o.UnregisterVault(vaultID); err != nil {
				t.Fatalf("UnregisterVault: %v", err)
			}
		}},
		{"RemoveVaultInstance", func(t *testing.T, o *Orchestrator, vaultID glid.GLID) {
			if !o.RemoveVaultInstance(vaultID) {
				t.Fatal("RemoveVaultInstance removed nothing")
			}
		}},
		{"DeleteVaultInstance", func(t *testing.T, o *Orchestrator, vaultID glid.GLID) {
			if !o.DeleteVaultInstance(vaultID) {
				t.Fatal("DeleteVaultInstance removed nothing")
			}
		}},
		{"finishDrain", func(_ *testing.T, o *Orchestrator, vaultID glid.GLID) {
			o.finishDrain(vaultID)
		}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()
			orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
			// Registered after newTestOrch so the blocked jobs are released
			// before the orchestrator closes.
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			removed := registerVaultWithPendingJobs(t, orch, release)
			kept := registerVaultWithPendingJobs(t, orch, release)

			path.teardown(t, orch, removed.vaultID)

			for _, name := range removed.names() {
				if orch.Scheduler().HasJob(name) {
					t.Errorf("job %q of the torn-down vault is still pending", name)
				}
			}
			for _, name := range kept.names() {
				if !orch.Scheduler().HasJob(name) {
					t.Errorf("job %q of another vault was cancelled", name)
				}
			}
			if orch.vaultHasBackfillFailures(removed.vaultID) {
				t.Error("cloud-upload backoff state of the torn-down vault survived teardown")
			}
			if !orch.vaultHasBackfillFailures(kept.vaultID) {
				t.Error("cloud-upload backoff state of another vault was dropped")
			}
		})
	}
}
