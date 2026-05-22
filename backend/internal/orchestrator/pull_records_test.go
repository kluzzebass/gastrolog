// Unit tests for the pull-by-EventID orchestrator handlers
// (gastrolog-4t3y4 step 5). Covers:
//
//   - PullSelectedRecords: EventID filtering, scheduled/missing counts,
//     self-pull rejection, vault-not-found, and async fill push.
//   - FillSealedRecords: SealedRepairer type-assertion, stub
//     ErrNotImplemented for file.Manager pending step 4b.2, structural
//     dispatch for non-implementing chunk managers (memory).
//
// Wire-level cluster tests live in step 6 (multi-node).

package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
)

// fillRecorder captures the FillRecords + FillComplete calls
// dispatchFillRecords would make to a real puller, so tests can verify
// the async push fired correctly without standing up the cluster wire.
type fillRecorder struct {
	mu          sync.Mutex
	fillBatches [][]chunk.Record
	lastBatchSeen bool
	completeCalls int
	completeRecordsSent uint32
	completeErr   string
}

func (r *fillRecorder) AppendRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (r *fillRecorder) SealVault(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (r *fillRecorder) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (r *fillRecorder) DeleteChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (r *fillRecorder) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}
func (r *fillRecorder) SendFillRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, records []chunk.Record, lastBatch bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	batch := make([]chunk.Record, len(records))
	copy(batch, records)
	r.fillBatches = append(r.fillBatches, batch)
	if lastBatch {
		r.lastBatchSeen = true
	}
	return nil
}
func (r *fillRecorder) SendFillComplete(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, recordsSent uint32, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completeCalls++
	r.completeRecordsSent = recordsSent
	r.completeErr = errMsg
	return nil
}
func (r *fillRecorder) PullRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.EventID, _ string) (uint32, uint32, error) {
	return 0, 0, nil
}

func (r *fillRecorder) totalRecords() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, b := range r.fillBatches {
		n += len(b)
	}
	return n
}

// waitForComplete polls until the FillComplete frame has been sent or the
// timeout elapses. dispatchFillRecords runs in a goroutine; the test must
// wait for it to fire before asserting.
func (r *fillRecorder) waitForComplete(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		done := r.completeCalls > 0
		r.mu.Unlock()
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("FillComplete never fired within %v", timeout)
}

// pullTestSetup builds an orchestrator with a memory-backed vault and a
// fillRecorder replicator wired in. Returns the orchestrator + vault ID +
// recorder. Records the EventIDs of N appended records via the returned
// closure so tests can request specific ones.
func pullTestSetup(t *testing.T, localNodeID string, vaultName string, recordCount int) (*Orchestrator, glid.GLID, []chunk.EventID, *fillRecorder) {
	t.Helper()
	orch := newTestOrch(t, Config{LocalNodeID: localNodeID})
	orch.logger = slog.Default()

	vaultID := glid.New()
	cm, err := chunkmem.NewManager(chunkmem.Config{})
	if err != nil {
		t.Fatalf("memory manager: %v", err)
	}
	vaultInst := &VaultInstance{
		VaultID: vaultID,
		Type:    "memory",
		Chunks:  cm,
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	recorder := &fillRecorder{}
	orch.SetChunkReplicator(recorder)

	eventIDs := make([]chunk.EventID, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		ingesterID := glid.New()
		nodeID := glid.New()
		ts := time.Date(2026, 1, 1, 0, 0, 0, i*1000, time.UTC)
		rec := chunk.Record{
			SourceTS: ts,
			IngestTS: ts,
			EventID: chunk.EventID{
				IngesterID: ingesterID,
				NodeID:     nodeID,
				IngestTS:   ts,
				IngestSeq:  uint32(i),
			},
			Attrs: chunk.Attributes{"i": vaultName},
			Raw:   []byte(vaultName),
		}
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		eventIDs = append(eventIDs, rec.EventID)
	}

	return orch, vaultID, eventIDs, recorder
}

func TestPullSelectedRecords_AllPresent(t *testing.T) {
	t.Parallel()
	orch, vaultID, eventIDs, recorder := pullTestSetup(t, "node-source", "all-present", 5)
	active := orch.vaults[vaultID].Instance.Chunks.Active()

	scheduled, missing, err := orch.PullSelectedRecords(
		context.Background(), vaultID, active.ID, eventIDs, "node-puller",
	)
	if err != nil {
		t.Fatalf("PullSelectedRecords: %v", err)
	}
	if scheduled != 5 || missing != 0 {
		t.Errorf("scheduled=%d missing=%d, want 5/0", scheduled, missing)
	}
	recorder.waitForComplete(t, time.Second)
	if recorder.totalRecords() != 5 {
		t.Errorf("recorder saw %d records pushed, want 5", recorder.totalRecords())
	}
	if !recorder.lastBatchSeen {
		t.Error("last batch flag never set on FillRecords")
	}
	if recorder.completeErr != "" {
		t.Errorf("FillComplete carried unexpected error: %q", recorder.completeErr)
	}
}

func TestPullSelectedRecords_SubsetPresent(t *testing.T) {
	t.Parallel()
	orch, vaultID, eventIDs, recorder := pullTestSetup(t, "node-source", "subset", 3)
	active := orch.vaults[vaultID].Instance.Chunks.Active()

	// Request 2 EventIDs we have + 1 we don't.
	unknown := chunk.EventID{
		IngesterID: glid.New(), NodeID: glid.New(),
		IngestTS: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), IngestSeq: 9999,
	}
	want := []chunk.EventID{eventIDs[0], eventIDs[2], unknown}

	scheduled, missing, err := orch.PullSelectedRecords(
		context.Background(), vaultID, active.ID, want, "node-puller",
	)
	if err != nil {
		t.Fatalf("PullSelectedRecords: %v", err)
	}
	if scheduled != 2 || missing != 1 {
		t.Errorf("scheduled=%d missing=%d, want 2/1", scheduled, missing)
	}
	recorder.waitForComplete(t, time.Second)
	if recorder.totalRecords() != 2 {
		t.Errorf("recorder saw %d records pushed, want 2", recorder.totalRecords())
	}
}

func TestPullSelectedRecords_NonePresent(t *testing.T) {
	t.Parallel()
	orch, vaultID, _, recorder := pullTestSetup(t, "node-source", "none-present", 3)
	active := orch.vaults[vaultID].Instance.Chunks.Active()

	unknown := []chunk.EventID{
		{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), IngestSeq: 1},
		{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: time.Date(2030, 1, 1, 0, 0, 0, 1, time.UTC), IngestSeq: 2},
	}

	scheduled, missing, err := orch.PullSelectedRecords(
		context.Background(), vaultID, active.ID, unknown, "node-puller",
	)
	if err != nil {
		t.Fatalf("PullSelectedRecords: %v", err)
	}
	if scheduled != 0 || missing != 2 {
		t.Errorf("scheduled=%d missing=%d, want 0/2", scheduled, missing)
	}
	// Even with zero records to push, FillComplete must fire so the puller
	// doesn't wait indefinitely.
	recorder.waitForComplete(t, time.Second)
	if recorder.totalRecords() != 0 {
		t.Errorf("recorder saw %d records pushed, want 0", recorder.totalRecords())
	}
}

func TestPullSelectedRecords_SelfPullRejected(t *testing.T) {
	t.Parallel()
	orch, vaultID, eventIDs, _ := pullTestSetup(t, "node-source", "self-pull", 2)
	active := orch.vaults[vaultID].Instance.Chunks.Active()

	_, _, err := orch.PullSelectedRecords(
		context.Background(), vaultID, active.ID, eventIDs, "node-source", // same as local
	)
	if err == nil {
		t.Fatal("expected error on self-pull, got nil")
	}
}

func TestPullSelectedRecords_VaultNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-source"})
	orch.logger = slog.Default()
	recorder := &fillRecorder{}
	orch.SetChunkReplicator(recorder)

	missingVault := glid.New()
	_, _, err := orch.PullSelectedRecords(
		context.Background(), missingVault, chunk.ChunkID{}, nil, "node-puller",
	)
	if err == nil {
		t.Fatal("expected vault-not-found error, got nil")
	}
}

func TestFillSealedRecords_MemoryManagerReturnsNotImplemented(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-receiver"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	cm, err := chunkmem.NewManager(chunkmem.Config{})
	if err != nil {
		t.Fatalf("memory manager: %v", err)
	}
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{
		VaultID: vaultID, Type: "memory", Chunks: cm,
	}))

	// Memory chunk manager doesn't implement SealedRepairer.
	err = orch.FillSealedRecords(context.Background(), vaultID, chunk.ChunkID{}, nil)
	if !errors.Is(err, chunk.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for memory manager, got %v", err)
	}
}

func TestFillSealedRecords_VaultNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-receiver"})
	orch.logger = slog.Default()

	missingVault := glid.New()
	err := orch.FillSealedRecords(context.Background(), missingVault, chunk.ChunkID{}, nil)
	if err == nil {
		t.Fatal("expected vault-not-found error, got nil")
	}
}
