package app

import (
	"bytes"
	"log/slog"
	"os"
	"runtime/debug"

	"gastrolog/internal/diskreserve"
	"gastrolog/internal/home"
)

// crashLogReserveBytes is the physical space reserved for the crash-forensics
// file — ample for a Go runtime traceback of even a large process, negligible
// on disk.
const crashLogReserveBytes = 1 << 20 // 1 MiB

// setCrashOutput is the seam for runtime crash-output registration; overridden
// in tests so they don't mutate the test runner's process-wide crash state.
var setCrashOutput = debug.SetCrashOutput

// armCrashLog points the Go runtime's panic/fatal traceback at a reserved
// on-disk file under the node home, so a crash on a FULL volume still records
// its stack. The disk-full incident (gastrolog-67gvjo) lost every traceback:
// the only sink was the shared log on the volume that filled, so the two nodes
// that panicked on WAL ENOSPC left nothing to diagnose from.
//
// The file's blocks are reserved (diskreserve) so the crash write cannot
// itself ENOSPC. If the PREVIOUS run left a traceback, it is surfaced to the
// logger before the file is re-armed — turning "the node vanished" into "the
// node crashed here" on the very next start. All failures degrade to a warning:
// crash capture is a diagnostic aid, never a startup gate.
func armCrashLog(hd home.Dir, logger *slog.Logger) {
	if err := hd.EnsureExists(); err != nil {
		logger.Warn("crash-forensics log unavailable: home dir", "error", err)
		return
	}
	path := hd.CrashLogPath()

	// Surface a previous crash before overwriting it.
	if prev, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: path from node home, not untrusted input //ok:os-readfile bounded by crashLogReserveBytes (1 MiB), read once at startup
		if trace := bytes.TrimRight(prev, "\x00\n "); len(trace) > 0 {
			logger.Error("previous run terminated by a crash — traceback recovered from the reserved crash log",
				"path", path, "traceback", string(trace))
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o640) //nolint:gosec // G304: node-home crash log, not secret
	if err != nil {
		logger.Warn("crash-forensics log unavailable: open reserved file", "path", path, "error", err)
		return
	}
	defer func() { _ = f.Close() }() // SetCrashOutput dups the fd; ours is safe to close.

	// Reserve blocks so the crash write survives a full volume. Best-effort:
	// without reservation, capture still works while free space remains —
	// strictly better than the pre-fix state where there was no file at all.
	if err := diskreserve.Blocks(f, crashLogReserveBytes); err != nil {
		logger.Warn("crash-forensics log reserve failed; traceback capture is best-effort until space frees",
			"path", path, "error", err)
	}
	if err := setCrashOutput(f, debug.CrashOptions{}); err != nil {
		logger.Warn("crash-forensics log unavailable: SetCrashOutput", "path", path, "error", err)
	}
}
