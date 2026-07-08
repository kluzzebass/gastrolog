package app

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"gastrolog/internal/home"
)

// find returns the first captured record whose message contains substr.
// Complements the shared captureHandler (dispatch_test.go) with record access.
func (h *captureHandler) find(msgSubstr string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r.Message, msgSubstr) {
			return r, true
		}
	}
	return slog.Record{}, false
}

func attrValue(r slog.Record, key string) (string, bool) {
	var out string
	var found bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			out = a.Value.String()
			found = true
			return false
		}
		return true
	})
	return out, found
}

// stubCrashOutput swaps the crash-output setter for the test's duration so it
// doesn't mutate the runner's process-wide crash state. Returns a pointer that
// receives the file the runtime would write to.
func stubCrashOutput(t *testing.T) **os.File {
	t.Helper()
	captured := new(*os.File)
	prev := setCrashOutput
	setCrashOutput = func(f *os.File, _ debug.CrashOptions) error {
		*captured = f
		return nil
	}
	t.Cleanup(func() { setCrashOutput = prev })
	return captured
}

// TestArmCrashLogRecoversPreviousTraceback pins the forensics-recovery half:
// a traceback left by the previous run is surfaced to the logger on the next
// start, then the file is re-armed (truncated) for this run.
func TestArmCrashLogRecoversPreviousTraceback(t *testing.T) {
	dir := t.TempDir()
	hd := home.New(dir)
	if err := hd.EnsureExists(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash file left by the previous run — a traceback followed by
	// the untouched tail of the reserved region (NUL padding).
	trace := "panic: failed to save current term: no space left on device\n\ngoroutine 1 [running]:\n"
	blob := append([]byte(trace), make([]byte, 4096)...)
	if err := os.WriteFile(hd.CrashLogPath(), blob, 0o640); err != nil {
		t.Fatal(err)
	}

	stubCrashOutput(t)
	h := &captureHandler{}
	armCrashLog(hd, slog.New(h))

	rec, ok := h.find("previous run terminated by a crash")
	if !ok {
		t.Fatal("a non-empty previous crash log must be surfaced to the logger")
	}
	tb, ok := attrValue(rec, "traceback")
	if !ok || !strings.Contains(tb, "failed to save current term") {
		t.Fatalf("recovered traceback must carry the panic text, got %q", tb)
	}
	if strings.ContainsRune(tb, 0) {
		t.Fatal("recovered traceback must have its reserved NUL tail trimmed")
	}

	// Re-armed: truncated so this run starts clean.
	info, err := os.Stat(hd.CrashLogPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("crash log must be truncated after re-arm, size=%d", info.Size())
	}
}

// TestArmCrashLogQuietWhenNoPriorCrash pins the clean-start path: an absent or
// empty crash log produces no false "previous crash" report.
func TestArmCrashLogQuietWhenNoPriorCrash(t *testing.T) {
	dir := t.TempDir()
	hd := home.New(dir)

	stubCrashOutput(t)
	h := &captureHandler{}
	armCrashLog(hd, slog.New(h)) // no crash.log exists yet

	if _, ok := h.find("previous run terminated"); ok {
		t.Fatal("first start must not report a phantom previous crash")
	}
	if _, err := os.Stat(hd.CrashLogPath()); err != nil {
		t.Fatalf("crash log must be created and armed: %v", err)
	}

	// A second arm over the empty file is still quiet (empty != crash).
	h2 := &captureHandler{}
	armCrashLog(hd, slog.New(h2))
	if _, ok := h2.find("previous run terminated"); ok {
		t.Fatal("an empty crash log must not read as a previous crash")
	}
}

// TestArmCrashLogArmsRuntimeOutput pins that the crash-output setter is handed
// the reserved crash-log file. (armCrashLog closes its own handle afterward —
// SetCrashOutput dups the fd in production — so we assert on the path, which
// survives close, not on the fd.)
func TestArmCrashLogArmsRuntimeOutput(t *testing.T) {
	dir := t.TempDir()
	hd := home.New(dir)

	armed := stubCrashOutput(t)
	armCrashLog(hd, slog.New(&captureHandler{}))

	if *armed == nil {
		t.Fatal("crash-output setter must be given the reserved file")
	}
	if got := (*armed).Name(); got != hd.CrashLogPath() {
		t.Fatalf("armed file = %q, want the reserved crash log %q", got, hd.CrashLogPath())
	}
}
