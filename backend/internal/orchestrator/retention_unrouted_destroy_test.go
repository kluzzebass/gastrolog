package orchestrator

// Coverage for gastrolog-65riw5: fireRetentionEvent must never report
// "completed" when zero records reached a route. The caller destroys the
// chunk on true, so a false positive ejects every record in it unrouted —
// loss of the operator's route disposition, which applyRetentionDisposition-
// ToChunk explicitly promises never to trade for anything. Duplicates at the
// route target are acceptable; loss is not.

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
)

// A vault instance that is absent (not built on this node, torn down
// mid-sweep) means the records cannot be read here at all. Before this fix
// the path returned true and the caller destroyed the chunk, silently — the
// only case in fireRetentionEvent with no log line at all.
func TestFireRetentionEventRetainsWhenVaultInstanceMissing(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	r := &retentionRunner{
		vaultID: glid.New(), // no vault registered under this ID
		orch:    o,
		logger:  slog.Default(),
		idleLog: logging.Throttle{Interval: time.Minute},
	}
	if done := r.fireRetentionEvent(chunk.NewChunkID()); done {
		t.Fatal("fan-out with no vault instance must not report completion; the chunk would be destroyed with zero records routed")
	}
}

type unreadableCursorCM struct {
	chunk.ChunkManager
	err error
}

func (c *unreadableCursorCM) OpenCursor(chunk.ChunkID) (chunk.RecordCursor, error) {
	return nil, c.err
}

// An unreadable cursor may be transient — an I/O error, an unmounted volume.
// It must hand the chunk to the unreadable-retry machinery (backoff +
// chunk-unreadable alarm + the operator's "Retry unreadable" action) rather
// than destroy it. Retention sweeps skip chunks inside their backoff window,
// so retaining cannot wedge the sweep — which is what made destroy-on-
// unreadable look like the only option.
func TestFireRetentionEventRetainsAndFlagsUnreadableCursor(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	// chunk-unreadable carries a catalog DelayOn (transient I/O errors and
	// retry backoff must not chatter); inject the clock and advance past it
	// so the raise annunciates — never wait on wall time.
	now := time.Date(2026, 7, 18, 6, 0, 0, 0, time.UTC)
	alerts := alert.NewWithClock(func() time.Time { return now })
	o.alerts = alerts

	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	inst.Chunks = &unreadableCursorCM{ChunkManager: inst.Chunks, err: errors.New("input/output error")}
	o.RegisterVault(&Vault{ID: vaultID, Instance: inst})

	r := &retentionRunner{
		vaultID: vaultID,
		cm:      inst.Chunks,
		orch:    o,
		logger:  slog.Default(),
		idleLog: logging.Throttle{Interval: time.Minute},
	}
	id := chunk.NewChunkID()

	if done := r.fireRetentionEvent(id); done {
		t.Fatal("fan-out with an unreadable cursor must not report completion; the chunk would be destroyed with zero records routed")
	}

	// The chunk is now tracked for retry, and the operator has an alarm
	// naming it rather than one Warn buried in the sweep's output.
	r.mu.Lock()
	entry := r.unreadable[id]
	r.mu.Unlock()
	if entry == nil {
		t.Fatal("unreadable cursor must flag the chunk for backoff retry, not silently drop it")
	}
	if entry.nextRetry.IsZero() {
		t.Fatal("unreadable entry must schedule a retry")
	}

	wantID := "chunk-unreadable:" + id.String()
	typ, ok := alert.TypeByID("chunk-unreadable")
	if !ok {
		t.Fatal("chunk-unreadable missing from the alarm catalog")
	}
	now = now.Add(typ.DelayOn + time.Second)
	found := false
	for _, a := range alerts.Standing() {
		if a.ID == wantID {
			found = true
		}
	}
	if !found {
		t.Fatalf("unreadable cursor must raise %s (annunciating after its %s delay-on): the operator decides restore vs accept loss", wantID, typ.DelayOn)
	}
}
