package orchestrator_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// receiveChunkEvent waits for a single event on the chunk bus and unwraps
// the Versioned envelope. Fails the test on timeout.
func receiveChunkEvent(t *testing.T, ch <-chan notify.Versioned[orchestrator.ChunkChangeEvent]) orchestrator.ChunkChangeEvent {
	t.Helper()
	select {
	case msg := <-ch:
		return msg.Event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for chunk event")
		return orchestrator.ChunkChangeEvent{}
	}
}

// TestChunkBusEmitsCreated covers the simplest happy path: a CREATED
// emit propagates through the bus with the right Op, vault ID, chunk
// ID, and full Meta. Subscribers (WatchChunks RPC handler) rely on
// these being filled to patch their local cache without refetching.
func TestChunkBusEmitsCreated(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bus := orch.ChunkBus()
	id, ch, _ := bus.Subscribe()
	defer bus.Unsubscribe(id)

	vault := glid.New()
	chunkID := chunk.NewChunkID()
	meta := chunk.ChunkMeta{ID: chunkID, RecordCount: 0, Sealed: false}
	orch.EmitChunkCreated(vault, meta)

	got := receiveChunkEvent(t, ch)
	if got.Op != orchestrator.ChunkChangeOpCreated {
		t.Errorf("Op = %v, want Created", got.Op)
	}
	if got.VaultID != vault {
		t.Errorf("VaultID = %v, want %v", got.VaultID, vault)
	}
	if got.ChunkID != chunkID {
		t.Errorf("ChunkID = %v, want %v", got.ChunkID, chunkID)
	}
	if got.Meta == nil {
		t.Fatal("Meta is nil; CREATED must carry the post-open snapshot")
	}
	if got.Meta.ID != chunkID {
		t.Errorf("Meta.ID = %v, want %v", got.Meta.ID, chunkID)
	}
}

// TestChunkBusEmitsSealedAndDeleted covers the two ops the inspector
// cares about most: SEALED carries the post-seal Meta so the client can
// flip the sealed flag in place; DELETED carries no Meta (subscriber
// drops the entry from its projection).
func TestChunkBusEmitsSealedAndDeleted(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bus := orch.ChunkBus()
	id, ch, _ := bus.Subscribe()
	defer bus.Unsubscribe(id)

	vault := glid.New()
	chunkID := chunk.NewChunkID()
	sealedMeta := chunk.ChunkMeta{ID: chunkID, RecordCount: 1000, Sealed: true}
	orch.EmitChunkSealed(vault, sealedMeta)

	first := receiveChunkEvent(t, ch)
	if first.Op != orchestrator.ChunkChangeOpSealed {
		t.Errorf("first.Op = %v, want Sealed", first.Op)
	}
	if first.Meta == nil || !first.Meta.Sealed {
		t.Errorf("Sealed Meta missing or sealed=false: %+v", first.Meta)
	}
	if first.Meta.RecordCount != 1000 {
		t.Errorf("Sealed RecordCount = %d, want 1000", first.Meta.RecordCount)
	}

	orch.EmitChunkDeleted(vault, chunkID)
	second := receiveChunkEvent(t, ch)
	if second.Op != orchestrator.ChunkChangeOpDeleted {
		t.Errorf("second.Op = %v, want Deleted", second.Op)
	}
	if second.Meta != nil {
		t.Errorf("Deleted Meta must be nil, got %+v", second.Meta)
	}
	if second.ChunkID != chunkID {
		t.Errorf("Deleted ChunkID = %v, want %v", second.ChunkID, chunkID)
	}
}

// TestChunkBusEmitsProgressWithRecordCount covers the PROGRESS shape:
// no Meta, just a record count. Frontend uses this to update the active
// chunk's count in place without replacing the entire entry. The
// gastrolog-1bgvm dedup fix ensures the count is monotonic across nodes,
// so the client can just write the carried count directly.
func TestChunkBusEmitsProgressWithRecordCount(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bus := orch.ChunkBus()
	id, ch, _ := bus.Subscribe()
	defer bus.Unsubscribe(id)

	vault := glid.New()
	chunkID := chunk.NewChunkID()
	orch.EmitChunkProgress(vault, chunkID, 42)

	got := receiveChunkEvent(t, ch)
	if got.Op != orchestrator.ChunkChangeOpProgress {
		t.Errorf("Op = %v, want Progress", got.Op)
	}
	if got.RecordCount != 42 {
		t.Errorf("RecordCount = %d, want 42", got.RecordCount)
	}
	if got.Meta != nil {
		t.Errorf("Progress Meta must be nil, got %+v", got.Meta)
	}
}

// TestChunkBusMonotonicVersion pins that successive emits produce
// strictly increasing version numbers. This is the contract subscribers
// rely on for drop detection: any gap means the bus dropped events to
// this subscriber and a cold-start resync is required.
func TestChunkBusMonotonicVersion(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bus := orch.ChunkBus()
	id, ch, _ := bus.Subscribe()
	defer bus.Unsubscribe(id)

	vault := glid.New()
	orch.EmitChunkProgress(vault, chunk.NewChunkID(), 1)
	orch.EmitChunkProgress(vault, chunk.NewChunkID(), 2)
	orch.EmitChunkProgress(vault, chunk.NewChunkID(), 3)

	var prev uint64
	for range 3 {
		select {
		case msg := <-ch:
			if msg.Version <= prev {
				t.Errorf("Version did not advance: got %d after %d", msg.Version, prev)
			}
			prev = msg.Version
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

// TestChunkProgressEmitterEmitsOnAdvance pins the contract that the
// progress emitter:
//   - Emits PROGRESS when an active chunk's record count grows since the
//     previous tick.
//   - Does NOT emit when the count is unchanged (idle vault stays quiet).
//
// Exercises emitActiveChunkProgress directly so the test isn't bound to
// a real ticker — gives deterministic per-tick observation.
func TestChunkProgressEmitterEmitsOnAdvance(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NeverRotatePolicy{},
	})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	bus := orch.ChunkBus()
	subID, ch, _ := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	// Append 5 records → first tick should emit PROGRESS{Count: 5}.
	for range 5 {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	last := orchestrator.NewLastSeenMap()
	orch.EmitActiveChunkProgress(last)
	got := receiveChunkEvent(t, ch)
	if got.Op != orchestrator.ChunkChangeOpProgress {
		t.Errorf("first tick: Op = %v, want Progress", got.Op)
	}
	if got.RecordCount != 5 {
		t.Errorf("first tick: RecordCount = %d, want 5", got.RecordCount)
	}

	// Second tick with no new appends → no event.
	orch.EmitActiveChunkProgress(last)
	select {
	case ev := <-ch:
		t.Errorf("unexpected event on idle tick: %+v", ev.Event)
	case <-time.After(100 * time.Millisecond):
		// Good: idle stays quiet.
	}

	// Append 3 more → next tick should emit PROGRESS{Count: 8}.
	for range 3 {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	orch.EmitActiveChunkProgress(last)
	got = receiveChunkEvent(t, ch)
	if got.RecordCount != 8 {
		t.Errorf("third tick: RecordCount = %d, want 8 (5 prior + 3 new)", got.RecordCount)
	}
}
