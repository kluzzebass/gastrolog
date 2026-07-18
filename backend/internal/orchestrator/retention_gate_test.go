package orchestrator

// gastrolog-5ct2av: per-destination admission rejections must reach the
// retention fan-out as terminal aborts. Before the ack was wired, the
// routing gate's whole-record nack went to a nil ack channel: the record
// vanished, Submit returned nil, and the chunk was destroyed unrouted.

import (
	"errors"
	"log/slog"
	"strings"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// TestFireRetentionEventAbortsOnCappedDestination pins the seam: a
// destination vault size-capped on a (simulated) remote peer must abort
// the fan-out with a single warn and report non-completion, so the caller
// retains the chunk.
func TestFireRetentionEventAbortsOnCappedDestination(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	// The archive destination is capped on some peer node. This is the
	// same lookup the NodeStats broadcast installs in production wiring.
	capped := fx.archiveID
	fx.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })

	logSink := &syncBuffer{}
	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
	}

	if r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fireRetentionEvent must report non-completion when a destination vault is capped")
	}
	logs := logSink.String()
	if got := strings.Count(logs, "fan-out aborted"); got != 1 {
		t.Errorf("want exactly 1 abort warn, got %d\nlogs:\n%s", got, logs)
	}
	if s := fx.orch.GetRouteStats(); s.Matched != 0 {
		t.Errorf("no record may be counted matched past a capped gate; Matched=%d", s.Matched)
	}
}

// TestFireRetentionEventAbortsOnBacklogCappedDestination pins the terminal
// treatment of ErrVaultBacklogBudget alongside the size cap above: a
// destination vault whose pipeline backlog has hit the cluster-global budget
// must abort the whole fan-out with a single warn, not fall into the
// default per-record-drop branch (which would destroy the chunk with every
// record silently dropped at the gate). Unlike the size cap the backlog
// budget needs no peer lookup — vaultAdmissionGate reads the LOCAL guard's
// vaultBacklogCapped state directly (disk_guard.go:735-740) — so this
// flips the real gate via SetVaultGuard + reconcileVaultBacklogCap instead
// of a remote-lookup stub.
func TestFireRetentionEventAbortsOnBacklogCappedDestination(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	// Register the archive (destination) vault with the guard and force its
	// backlog over budget, exactly as evaluateVaults would on a real tick.
	g, _ := newGuardFixture(400*gib, map[string]uint64{})
	g.SetVaultGuard(fx.archiveID, "archive", nil, "", "", 0)
	g.backlogBudget.Store(100)
	g.vaultBacklogBytes = func(id glid.GLID) int64 {
		if id == fx.archiveID {
			return 200
		}
		return 0
	}
	g.evaluateVaults(nil)
	if !g.vaultBacklogCapped(fx.archiveID) {
		t.Fatal("fixture setup: archive vault must be backlog-capped before firing retention")
	}
	fx.orch.diskGuard = g

	logSink := &syncBuffer{}
	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
	}

	if r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fireRetentionEvent must report non-completion when a destination vault is backlog-capped")
	}
	logs := logSink.String()
	if got := strings.Count(logs, "fan-out aborted"); got != 1 {
		t.Errorf("want exactly 1 abort warn, got %d\nlogs:\n%s", got, logs)
	}
	if s := fx.orch.GetRouteStats(); s.Matched != 0 {
		t.Errorf("no record may be counted matched past a backlog-capped gate; Matched=%d", s.Matched)
	}
}

// TestSubmitRetentionRecordReturnsGateError pins the exported seam
// directly: the per-destination gate error surfaces on the submit call.
func TestSubmitRetentionRecordReturnsGateError(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	capped := fx.archiveID
	fx.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })

	rec, err := readOneSealedRecord(t, fx)
	if err != nil {
		t.Fatalf("read seed record: %v", err)
	}
	subErr := fx.orch.SubmitRetentionRecord(t.Context(), fx.sourceID, rec, "")
	if !errorsIsVaultMaxSize(subErr) {
		t.Fatalf("want ErrVaultMaxSize from gated submit, got %v", subErr)
	}
}

// readOneSealedRecord reads the first record from the fixture's sealed
// chunk. chunk.RecordCursor.Next returns (Record, RecordRef, error) — the
// RecordRef is irrelevant here, so it is discarded.
func readOneSealedRecord(t *testing.T, fx dispositionFixture) (chunk.Record, error) {
	t.Helper()
	cur, err := fx.sourceCM.OpenCursor(fx.sealedID)
	if err != nil {
		return chunk.Record{}, err
	}
	defer func() { _ = cur.Close() }()
	rec, _, err := cur.Next()
	return rec, err
}

func errorsIsVaultMaxSize(err error) bool { return errors.Is(err, ErrVaultMaxSize) }

// TestFireRetentionEventRunsUnderAdmissionGate pins the drain-gate
// reclassification: with the node's admission gate engaged (free space in
// the warn band) but the drain gate open, retention fan-out must complete —
// this is exactly the incident's frozen band (gastrolog-5ct2av).
func TestFireRetentionEventRunsUnderAdmissionGate(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	// 400GiB volume, 20GiB free: below the warn band (40GiB) resume bar,
	// above the floor band (12GiB*1.25) — admission gate engaged, drain
	// gate open after evaluate.
	g, sampler := newGuardFixture(400*gib, map[string]uint64{"a": 5 * gib})
	g.evaluate(nil) // floor breach: both gates engage
	sampler.free["a"] = 20 * gib
	g.evaluate(nil) // recovery into the frozen band
	fx.orch.diskGuard = g
	if !fx.orch.diskProtectActive() || fx.orch.diskDeferWrites() {
		t.Fatal("fixture must be in the frozen band: admission engaged, drain open")
	}

	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.Default(),
	}
	if !r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fan-out must complete in the frozen band (admission gate engaged, drain gate open)")
	}
	waitForRouteStats(t, fx.orch, "3 records routed under the admission gate", func(s *RouteStats) bool {
		return s.Matched == 3
	})
}

// TestFireRetentionEventDefersBelowFloor pins the other side: below the
// floor both gates are engaged and the fan-out defers with a single warn.
func TestFireRetentionEventDefersBelowFloor(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	g, _ := newGuardFixture(400*gib, map[string]uint64{"a": 5 * gib})
	g.evaluate(nil) // below floor: drain gate engaged
	fx.orch.diskGuard = g

	logSink := &syncBuffer{}
	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
	}
	if r.fireRetentionEvent(fx.sealedID) {
		t.Fatal("fan-out must defer below the floor")
	}
	if !strings.Contains(logSink.String(), "route fan-out deferred") {
		t.Errorf("deferral must warn; logs:\n%s", logSink.String())
	}
	if s := fx.orch.GetRouteStats(); s.Routed != 0 {
		t.Errorf("no records may enter routing below the floor; Routed=%d", s.Routed)
	}
}
