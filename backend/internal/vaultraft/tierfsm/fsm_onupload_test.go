package tierfsm

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
)

func TestOnUploadCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *ManifestEntry
	fsm.SetOnUpload(func(e ManifestEntry) {
		mu.Lock()
		captured = &e
		mu.Unlock()
	})

	// Create and seal the chunk first.
	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 42, 1024, now, now, now, false)})

	// Upload.
	fsm.Apply(&hraft.Log{Data: MarshalUploadChunk(id, 512, 100, 50, 200, 75, [32]byte{}, glid.GLID{}, 0)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnUpload callback was not called")
	}
	if captured.ID != id {
		t.Errorf("ID = %s, want %s", captured.ID, id)
	}
	if !captured.CloudBacked {
		t.Error("CloudBacked should be true")
	}
	if captured.DiskBytes != 512 {
		t.Errorf("DiskBytes = %d, want 512", captured.DiskBytes)
	}
	if captured.RecordCount != 42 {
		t.Errorf("RecordCount = %d, want 42", captured.RecordCount)
	}
}

func TestOnUploadCallbackNotCalledOnError(t *testing.T) {
	t.Parallel()

	fsm := New()
	called := false
	fsm.SetOnUpload(func(e ManifestEntry) { called = true })

	// Upload for a non-existent chunk — should error, not fire callback.
	id := chunk.NewChunkID()
	fsm.Apply(&hraft.Log{Data: MarshalUploadChunk(id, 512, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0)})

	if called {
		t.Error("OnUpload should not fire when applyUpload fails")
	}
}
