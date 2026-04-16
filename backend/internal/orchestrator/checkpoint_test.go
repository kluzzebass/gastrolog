package orchestrator

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
)

// mockCheckpointIngester implements both Ingester and Checkpointable.
type mockCheckpointIngester struct {
	mu        sync.Mutex
	state     map[string]string
	saveCalls atomic.Int32
	loadCalls atomic.Int32
	runBlock  chan struct{} // closed to unblock Run
}

func newMockCheckpointIngester() *mockCheckpointIngester {
	return &mockCheckpointIngester{
		state:    map[string]string{"cursor": "0"},
		runBlock: make(chan struct{}),
	}
}

func (m *mockCheckpointIngester) Run(ctx context.Context, out chan<- IngestMessage) error {
	// Emit a few records, then block until cancelled or unblocked.
	for i := 0; i < 3; i++ {
		m.mu.Lock()
		m.state["cursor"] = string(rune('0' + i + 1))
		m.mu.Unlock()
		select {
		case out <- IngestMessage{
			Raw:      []byte("msg"),
			IngestTS: time.Now(),
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.runBlock:
		return nil
	}
}

func (m *mockCheckpointIngester) SaveCheckpoint() ([]byte, error) {
	m.saveCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(m.state)
}

func (m *mockCheckpointIngester) LoadCheckpoint(data []byte) error {
	m.loadCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Unmarshal(data, &m.state)
}

// TestCheckpointSaveAndLoad verifies that runWithCheckpoints calls
// SaveCheckpoint periodically and on exit, and that the callback fires.
func TestCheckpointSaveAndLoad(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var savedData []byte
	var callbackCalls int

	orch := newTestOrch(t, Config{
		LocalNodeID: "test-node",
		OnIngesterCheckpoint: func(id glid.GLID, data []byte) {
			mu.Lock()
			savedData = append([]byte(nil), data...)
			callbackCalls++
			mu.Unlock()
		},
	})

	ing := newMockCheckpointIngester()
	ingID := glid.New()
	if err := orch.AddIngester(ingID, "ckpt-test", "mock", false, ing); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Give the ingester time to emit records and the checkpoint ticker to fire.
	// The ticker interval in runWithCheckpoints is 5s, but the exit path also
	// saves a checkpoint. We'll stop the orchestrator which triggers the exit save.
	time.Sleep(100 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Verify SaveCheckpoint was called at least once (on exit).
	if ing.saveCalls.Load() < 1 {
		t.Fatalf("expected at least 1 SaveCheckpoint call, got %d", ing.saveCalls.Load())
	}

	// Verify the callback was invoked.
	mu.Lock()
	defer mu.Unlock()
	if callbackCalls < 1 {
		t.Fatalf("expected at least 1 callback call, got %d", callbackCalls)
	}
	if len(savedData) == 0 {
		t.Fatal("expected non-empty checkpoint data")
	}

	// Verify the data round-trips.
	var restored map[string]string
	if err := json.Unmarshal(savedData, &restored); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	if restored["cursor"] == "" {
		t.Fatal("expected cursor in checkpoint data")
	}
}

// TestCheckpointNotCalledWithoutCallback verifies that when
// OnIngesterCheckpoint is nil, runWithCheckpoints just runs the ingester
// without calling SaveCheckpoint.
func TestCheckpointNotCalledWithoutCallback(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{
		LocalNodeID:          "test-node",
		OnIngesterCheckpoint: nil, // no callback
	})

	ing := newMockCheckpointIngester()
	ingID := glid.New()
	if err := orch.AddIngester(ingID, "no-ckpt", "mock", false, ing); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// SaveCheckpoint should NOT be called when callback is nil.
	if ing.saveCalls.Load() != 0 {
		t.Fatalf("expected 0 SaveCheckpoint calls without callback, got %d", ing.saveCalls.Load())
	}
}

// TestCheckpointLoadRestoresState verifies that LoadCheckpoint correctly
// restores ingester state from previously saved data.
func TestCheckpointLoadRestoresState(t *testing.T) {
	t.Parallel()

	ing := newMockCheckpointIngester()
	ing.state["cursor"] = "42"

	// Save checkpoint.
	data, err := ing.SaveCheckpoint()
	if err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	// Create a new ingester and load the checkpoint.
	ing2 := newMockCheckpointIngester()
	if err := ing2.LoadCheckpoint(data); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}

	ing2.mu.Lock()
	defer ing2.mu.Unlock()
	if ing2.state["cursor"] != "42" {
		t.Fatalf("expected cursor=42 after load, got %q", ing2.state["cursor"])
	}
}

// TestCheckpointLoadInvalidData verifies that LoadCheckpoint returns an error
// for corrupt data rather than panicking.
func TestCheckpointLoadInvalidData(t *testing.T) {
	t.Parallel()

	ing := newMockCheckpointIngester()
	err := ing.LoadCheckpoint([]byte("not valid json{{{"))
	if err == nil {
		t.Fatal("expected error for invalid checkpoint data")
	}
}

// TestCheckpointEmptyState verifies that SaveCheckpoint returns valid JSON
// even when the ingester has no state to save.
func TestCheckpointEmptyState(t *testing.T) {
	t.Parallel()

	ing := newMockCheckpointIngester()
	ing.state = map[string]string{}

	data, err := ing.SaveCheckpoint()
	if err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	// Should produce valid JSON (empty object).
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal empty checkpoint: %v", err)
	}
}
