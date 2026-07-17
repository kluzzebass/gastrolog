package chunking

// State-based rate limiting for planner index failures and head-purge error
// logging (gastrolog-6wwdos). Wall-clock-gated throttles are a defect in this
// project; the planner logs on failure-state transitions instead.

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

type stubAlertSink struct {
	sets   int
	clears int
}

func (s *stubAlertSink) Raise(string, string, string) { s.sets++ }

func (s *stubAlertSink) RaiseOperator(alert.OperatorAlarm) { s.sets++ }

func (s *stubAlertSink) Clear(string, string) { s.clears++ }

func newLoggedVault(cfg VaultConfig) (*vaultChunking, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	v := &vaultChunking{cfg: cfg}
	v.log = slog.New(slog.NewTextHandler(buf, nil))
	return v, buf
}

func TestNotePlanFailureLogsOncePerFailureState(t *testing.T) {
	t.Parallel()
	v, buf := newLoggedVault(VaultConfig{VaultID: glid.New()})
	id := glid.New()

	openErr := errors.New("corrupt header")
	v.notePlanFailure(id, "open segment index", openErr)
	v.notePlanFailure(id, "open segment index", openErr)
	v.notePlanFailure(id, "open segment index", openErr)
	if got := strings.Count(buf.String(), "segment index unreadable"); got != 1 {
		t.Fatalf("warn lines for repeated identical failure = %d, want 1", got)
	}

	// A different failure is a state change and logs again.
	v.notePlanFailure(id, "read segment index entry", errors.New("short read"))
	if got := strings.Count(buf.String(), "segment index unreadable"); got != 2 {
		t.Fatalf("warn lines after failure-state change = %d, want 2", got)
	}

	// Recovery logs the resumed line once and drops the state.
	v.clearPlanFailure(id)
	v.clearPlanFailure(id)
	if got := strings.Count(buf.String(), "planning resumed"); got != 1 {
		t.Fatalf("resume lines = %d, want 1", got)
	}
	if len(v.planFailures) != 0 {
		t.Fatalf("planFailures = %v, want empty after clear", v.planFailures)
	}
}

func TestUnplannableAlertRaisesOnRepeatAndClearsOnPrune(t *testing.T) {
	t.Parallel()
	sink := &stubAlertSink{}
	v, _ := newLoggedVault(VaultConfig{VaultID: glid.New(), Alerts: sink})
	id := glid.New()
	indexErr := errors.New("corrupt header")

	v.notePlanFailure(id, "open segment index", indexErr)
	if sink.sets != 0 {
		t.Fatalf("alert fired on first failure; sets = %d, want 0", sink.sets)
	}
	v.notePlanFailure(id, "open segment index", indexErr)
	if sink.sets != 1 {
		t.Fatalf("alert after repeated failure; sets = %d, want 1", sink.sets)
	}
	// Repeats do not re-set the alert (transition-based, like the
	// under-replicated alert).
	v.notePlanFailure(id, "open segment index", indexErr)
	if sink.sets != 1 {
		t.Fatalf("alert re-set on repeat; sets = %d, want 1", sink.sets)
	}

	// The segment leaves the eligible set (released, retention give-up
	// expiry): failure state prunes and the alert clears.
	v.prunePlanFailures(map[glid.GLID]struct{}{})
	if sink.clears != 1 {
		t.Fatalf("alert not cleared on prune; clears = %d, want 1", sink.clears)
	}
	if len(v.planFailures) != 0 {
		t.Fatalf("planFailures = %v, want empty after prune", v.planFailures)
	}
}

func TestPurgeReleasedHeadLogsFailures(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory write permissions")
	}
	root := t.TempDir()
	headDir := paths.HeadDir(root)
	if err := os.MkdirAll(headDir, 0o750); err != nil {
		t.Fatal(err)
	}
	id := glid.New()
	if err := os.WriteFile(paths.HeadSegment(root, id), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A read-only head/ makes os.Remove fail without touching the file.
	if err := os.Chmod(headDir, 0o550); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(headDir, 0o750) })

	v, buf := newLoggedVault(VaultConfig{VaultID: glid.New(), VaultRoot: root})
	v.purgeReleasedHead([]glid.GLID{id})
	if !strings.Contains(buf.String(), "head purge failed after registry release") {
		t.Fatalf("purge failure not logged; log = %q", buf.String())
	}
}
