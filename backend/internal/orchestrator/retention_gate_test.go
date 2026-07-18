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
