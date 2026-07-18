# Retention Route Fan-Out Under Disk Protect — Implementation Plan (gastrolog-5ct2av)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route-disposition retention makes progress under disk pressure (drain-gate governed, not admission-gate governed), can never hang or silently destroy unrouted chunks, and raises one named alarm when deferred repeatedly.

**Architecture:** Five thin changes along the existing seams: (1) retention submits carry the durability ack so per-destination gate rejections become terminal aborts instead of silent drops; (2) a `SubmitDrain` supervisor entry skips only the node-global admission gate, and `fireRetentionEvent`'s pre-check moves from `diskProtectActive()` to `diskDeferWrites()`; (3) a progress watchdog aborts a stalled fan-out; (4) the retention runner tracks consecutive deferred sweeps and raises/clears the `retention-route-deferred` alarm; (5) banned-vocabulary comment rewrite plus doc updates. Spec: `docs/retention-disk-protect-design.md`.

**Tech Stack:** Go 1.26+, existing test fixtures (`newDispositionFixture`, `newGuardFixture`, `setupMultiNode`), `just test` / `just backend test-full`.

## Global Constraints

- The banned storage-level word (see gastrolog-16hruo; the concept ban covers every sense) must not appear in any GastroLog prose, identifiers, comments, or docs. The two disk-guard stages are the **admission gate** (`protect`) and the **drain gate** (`deferWrites`).
- No new operator-facing configuration. New numeric bounds are unexported Go constants.
- No wall-clock in test assertions: watchdog logic is tested through injected ticks and pure functions, never sleeps racing timers.
- Never modify the 5034va ordering: routing runs before the retention-pending flag; an aborted fan-out must not consume the one-shot.
- Commit messages: `type(scope): summary (gastrolog-5ct2av)`. No attribution trailers of any kind.
- All work on branch `fix/gastrolog-5ct2av-retention-drain-gate`.
- Run tests from `backend/`: `go test ./internal/orchestrator/ -run <Name> -v` (or `-short ./...` for the sweep).

---

### Task 1: Vocabulary rewrite + ubiquitous-language entries

**Files:**
- Modify: `backend/internal/orchestrator/disk_guard.go` (comments only: lines 76-85, 103-115, 530-533, 638-660, 677-690)
- Modify: `backend/internal/orchestrator/disk_guard_test.go` (comments only: lines 231-275)
- Modify: `docs/ubiquitous_language.md` (new entries after the **Expire** entry in the retention block, around line 498)

**Interfaces:**
- Consumes: nothing.
- Produces: the vocabulary later tasks' comments and log strings use ("admission gate", "drain gate").

- [ ] **Step 1: Rewrite disk_guard.go comments**

Comment-only edits — no code or string literals change. Replace every banned-word phrase:

*(Executed — the original mapping quoted the banned word per site and is preserved in this file's git history; the result is the comment text now in `disk_guard.go`: admission gate for `protect`, drain gate for `deferWrites`, "two staged gates", "the drain gate releases first / the admission gate releases last".)*

In `disk_guard_test.go`, the comments and t.Fatal strings in `TestDiskGuardStagedReleaseInvariant` (l.231-275) and `TestPrimeDiskGuardClosesBootWindow` (l.294-296) got the same gate-vocabulary rewrite. Fatal-string edits are test-message-only, no assertion logic changes.

- [ ] **Step 2: Verify the word is gone from both files**

Run: `grep -n -i "t[i]er" backend/internal/orchestrator/disk_guard.go backend/internal/orchestrator/disk_guard_test.go`
Expected: no output (exit 1).

- [ ] **Step 3: Add ubiquitous-language entries**

In `docs/ubiquitous_language.md`, directly after the **Expire** entry (the retention vocabulary block), insert:

```markdown
- **Disk guard** — the per-node free-space guard job (`disk_guard.go`):
  samples the node's data volumes every 15s and drives two staged gates
  plus the per-vault caps (max-size budget, backlog budget, per-vault
  floor).

- **Admission gate** (`protect`) — the disk guard's outer gate: suspends
  ingest admission and catch-up pulls. Engages below the free-space
  floor, releases only above the warn band (asymmetric deadband so the
  release burst cannot re-cross the floor).

- **Drain gate** (`deferWrites`) — the disk guard's inner gate: pauses
  chunking builds, collection pulls, and retention route fan-out.
  Engages below the floor, releases just above it — before the
  admission gate, so the paths that free space run while admission is
  still suspended.

- **Retention deferral** — a sweep whose route fan-out could not run
  (drain gate engaged, destination vault gated, or fan-out stalled);
  the chunk is retained for a later sweep. Consecutive deferrals raise
  the `retention-route-deferred` alarm.
```

- [ ] **Step 4: Run the guard tests**

Run: `cd backend && go test ./internal/orchestrator/ -run TestDiskGuard -v`
Expected: PASS (all TestDiskGuard* tests).

- [ ] **Step 5: Commit**

```bash
git add backend/internal/orchestrator/disk_guard.go backend/internal/orchestrator/disk_guard_test.go docs/ubiquitous_language.md
git commit -m "refactor(storage): rename the disk guard's stages to admission gate and drain gate — banned-word sweep plus ubiquitous-language entries (gastrolog-5ct2av)"
```

---

### Task 2: Ack-carrying retention submit — gate rejections become terminal aborts

**Files:**
- Modify: `backend/internal/orchestrator/pipeline.go:79-102` (`SubmitRetentionRecord`)
- Test: `backend/internal/orchestrator/retention_gate_test.go` (create)

**Interfaces:**
- Consumes: `routing.Input.Ack chan<- error` (existing), `(*Supervisor).Submit` (existing).
- Produces: `SubmitRetentionRecord(ctx, sourceVaultID, rec, reason) error` — same signature, but now returns the per-destination gate error (`ErrVaultMaxSize`, `ErrVaultDiskProtect`, `ErrVaultBacklogBudget`) or the durable-commit error, instead of nil-on-queued. Task 3's `SubmitDrain` slots in underneath; Task 4's watchdog counts its returns as progress.

**Why:** the routing gate rejects a whole record via `sendAck(in.Ack, err)` (`routing/manager.go:244-251`). `SubmitRetentionRecord` currently submits with no Ack, so the rejection is silent, `Submit` returns nil, and `fireRetentionEvent` destroys the chunk with zero records delivered — the exact unrouted-destroy failure the terminal-error switch (retention.go:1022-1027) was written to prevent. Wiring the ack makes that switch live, and upgrades `fireRetentionEvent`'s `true` to mean "durably committed at every matched destination" instead of "queued".

- [ ] **Step 1: Write the failing test**

Create `backend/internal/orchestrator/retention_gate_test.go`:

```go
package orchestrator

// gastrolog-5ct2av: per-destination admission rejections must reach the
// retention fan-out as terminal aborts. Before the ack was wired, the
// routing gate's whole-record nack went to a nil ack channel: the record
// vanished, Submit returned nil, and the chunk was destroyed unrouted.

import (
	"log/slog"
	"strings"
	"testing"

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
```

Add the two small helpers at the bottom of the same file:

```go
import ( // merge into the import block above
	"errors"
	"gastrolog/internal/chunk"
)

func readOneSealedRecord(t *testing.T, fx dispositionFixture) (chunk.Record, error) {
	t.Helper()
	cur, err := fx.sourceCM.OpenCursor(fx.sealedID)
	if err != nil {
		return chunk.Record{}, err
	}
	defer func() { _ = cur.Close() }()
	return cur.Next()
}

func errorsIsVaultMaxSize(err error) bool { return errors.Is(err, ErrVaultMaxSize) }
```

(If `RecordCursor`'s read method is not `Next() (chunk.Record, error)`, adapt the helper to the actual `chunk.RecordCursor` interface in `internal/chunk` — the test's substance is the returned error, not cursor mechanics.)

- [ ] **Step 2: Run to verify both fail**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestFireRetentionEventAbortsOnCappedDestination|TestSubmitRetentionRecordReturnsGateError' -v`
Expected: FAIL — fireRetentionEvent returns true (silent gate drop), and SubmitRetentionRecord returns nil.

- [ ] **Step 3: Wire the ack into SubmitRetentionRecord**

In `backend/internal/orchestrator/pipeline.go`, replace the body of `SubmitRetentionRecord` (keep the signature) and update its doc comment:

```go
// SubmitRetentionRecord routes a single record ejected from a vault during a
// retention event (disposition=route) through the pipeline routing stage with a
// RetentionSource context, so routes matching _source="retention" / _vault=<id>
// fan it out to their configured destinations. It carries the durability ack
// and waits for it: a nil return means every matched destination durably
// committed the record (an unmatched record is a counted drop that also
// returns nil). A per-destination admission rejection (ErrVaultMaxSize,
// ErrVaultDiskProtect, ErrVaultBacklogBudget) surfaces here so the caller can
// abort the whole chunk fan-out and retain the chunk — before the ack was
// wired, the routing gate's whole-record nack went to a nil ack and the
// record silently vanished (gastrolog-5ct2av).
func (o *Orchestrator) SubmitRetentionRecord(ctx context.Context, sourceVaultID glid.GLID, rec chunk.Record, reason string) error {
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		// Same terminal condition as a stopped supervisor: callers abort
		// their fan-out on this sentinel (gastrolog-5034va).
		return pipeline.ErrNotRunning
	}
	// Owned conversion: the drain cursor materializes rec.Attrs fresh per
	// record; nothing else holds the map (gastrolog-11y2iv).
	prec := convert.ChunkToRecordOwned(rec)
	ack := make(chan error, 1)
	if err := pl.Submit(ctx, routing.Input{
		Record: &prec,
		Source: routing.RetentionSource(sourceVaultID, reason),
		Ack:    ack,
	}); err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

(`pl.Submit` becomes `pl.SubmitDrain` in Task 3; this task keeps `Submit`.)

- [ ] **Step 4: Run the new tests**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestFireRetentionEventAbortsOnCappedDestination|TestSubmitRetentionRecordReturnsGateError' -v`
Expected: PASS.

- [ ] **Step 5: Run the neighboring retention and pipeline tests**

Run: `cd backend && go test ./internal/orchestrator/ ./internal/pipeline/... -v -run 'Retention|Routing|Disposition'`
Expected: PASS. Watch specifically `TestTryRetainChunkMarksPendingAfterSuccessfulRoute` (fx has no registered local sink for the archive vault, so the join resolves on the zero-target path) and the shutdown tests (ErrNotRunning path unchanged). If a test hangs here, the likely cause is an ack path that never resolves — that is a real bug in the change, not the test; every route() exit resolves the ack (`sendAck` on unmatched/gated/zero-target, `fanOutJoined` children otherwise).

- [ ] **Step 6: Commit**

```bash
git add backend/internal/orchestrator/pipeline.go backend/internal/orchestrator/retention_gate_test.go
git commit -m "fix(retention): carry the durability ack through the fan-out submit — gate rejections abort instead of silently dropping (gastrolog-5ct2av)"
```

---

### Task 3: Drain gate governs the fan-out; SubmitDrain skips node-global admission

**Files:**
- Modify: `backend/internal/orchestrator/pipeline/supervisor.go` (add `SubmitDrain` next to `Submit`, l.506-520)
- Modify: `backend/internal/orchestrator/pipeline.go` (`SubmitRetentionRecord`: `pl.Submit` → `pl.SubmitDrain`)
- Modify: `backend/internal/orchestrator/retention.go:938-957` (`fireRetentionEvent` pre-check)
- Test: `backend/internal/orchestrator/retention_gate_test.go` (extend)

**Interfaces:**
- Consumes: `diskDeferWrites()` (existing accessor), Task 2's ack-carrying `SubmitRetentionRecord`.
- Produces: `(*Supervisor).SubmitDrain(ctx context.Context, in routing.Input) error` — identical contract to `Submit` minus the node-global `admit()` call. Task 4 wraps the same fan-out.

- [ ] **Step 1: Write the failing tests**

Append to `backend/internal/orchestrator/retention_gate_test.go`:

```go
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
```

(`gib`, `newGuardFixture` are package-local test helpers in `disk_guard_test.go`; `waitForRouteStats` is in the retention test helpers.)

- [ ] **Step 2: Run to verify both fail**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestFireRetentionEventRunsUnderAdmissionGate|TestFireRetentionEventDefersBelowFloor' -v`
Expected: `RunsUnderAdmissionGate` FAILS (pre-check aborts on `diskProtectActive`, and `Submit`'s `admit()` would reject anyway). `DefersBelowFloor` may pass already — keep it as a pin.

- [ ] **Step 3: Add SubmitDrain and switch the pre-check**

In `backend/internal/orchestrator/pipeline/supervisor.go`, after `Submit` (l.520):

```go
// SubmitDrain routes a record exactly like Submit but skips the node-global
// admission gate: drain work — retention route fan-out — must run whenever
// the pipeline runs, because it is the mechanism that frees the space the
// admission gate is waiting for (gastrolog-5ct2av). Per-destination
// admission still applies in the routing stage, and the drain gate
// (deferWrites) is enforced by the caller before any record is read.
func (s *Supervisor) SubmitDrain(ctx context.Context, in routing.Input) error {
	if !s.running.Load() {
		return ErrNotRunning
	}
	if !s.sendRouting(ctx, in) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrNotRunning
	}
	return nil
}
```

In `backend/internal/orchestrator/pipeline.go`, change `pl.Submit(ctx, routing.Input{...})` to `pl.SubmitDrain(ctx, routing.Input{...})` inside `SubmitRetentionRecord`.

In `backend/internal/orchestrator/retention.go:945`, replace the pre-check block:

```go
	if r.orch.diskDeferWrites() {
		// The drain gate is engaged: the node is below its free-space floor,
		// where nothing may consume disk — not even the drain itself. Above
		// the floor band the gate releases and this fan-out runs even while
		// the ADMISSION gate still suspends ingest: retention is the only
		// mechanism that frees space on a route-disposition vault, so gating
		// it on admission's resume bar deadlocked the vault permanently
		// (gastrolog-5ct2av; previously this checked diskProtectActive).
		// Fanning out below the floor would still be wrong for the
		// gastrolog-5034va reason: every routed record would be refused and
		// the chunk destroyed unrouted.
		if n, ok := r.idleLog.Allow("disk-protect"); ok {
			r.logger.Warn("retention: route fan-out deferred — drain gate engaged below the disk floor; chunk retained for a later sweep",
				"vault", r.vaultID, "suppressed", n)
		}
		return false
	}
```

- [ ] **Step 4: Run the tests**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestFireRetentionEvent|TestSubmitRetentionRecord|TestTryRetainChunk' -v`
Expected: PASS, including Task 2's tests and the 5034va/65riw5 pins.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/orchestrator/pipeline/supervisor.go backend/internal/orchestrator/pipeline.go backend/internal/orchestrator/retention.go backend/internal/orchestrator/retention_gate_test.go
git commit -m "fix(retention): route fan-out follows the drain gate, not the admission gate — the drain must run while admission waits for it (gastrolog-5ct2av)"
```

---

### Task 4: Fan-out progress watchdog

**Files:**
- Create: `backend/internal/orchestrator/retention_watchdog.go`
- Modify: `backend/internal/orchestrator/retention.go` (`fireRetentionEvent`: start/stop the watchdog; workers bump progress)
- Test: `backend/internal/orchestrator/retention_watchdog_test.go` (create)

**Interfaces:**
- Consumes: the `abort(cause error)` closure inside `fireRetentionEvent` (existing).
- Produces: `type progressWatch struct` with `bump()` and `stalled() bool`; `runStallMonitor(done <-chan struct{}, tick <-chan time.Time, w *progressWatch, abort func(error))`; `var errRetentionFanOutStalled`; `const retentionFanOutStallWindow = 2 * time.Minute`. Task 5 reuses `errRetentionFanOutStalled` for cause naming.

- [ ] **Step 1: Write the failing tests**

Create `backend/internal/orchestrator/retention_watchdog_test.go`:

```go
package orchestrator

// gastrolog-5ct2av: a destination that passes its admission gate but stops
// draining must not hang the retention sweep. The watchdog aborts a fan-out
// that makes no progress for a full stall window. Tested through injected
// ticks and the pure stalled() predicate — never by racing real timers.

import (
	"errors"
	"testing"
	"time"
)

func TestProgressWatchStalledSemantics(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	// No progress since construction: stalled.
	if !w.stalled() {
		t.Fatal("no progress observed: stalled() must be true")
	}
	w.bump()
	if w.stalled() {
		t.Fatal("progress since last check: stalled() must be false")
	}
	// The bump was consumed by the previous check; no new progress.
	if !w.stalled() {
		t.Fatal("no progress since last check: stalled() must be true again")
	}
}

func TestRunStallMonitorAbortsOnStall(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	tick := make(chan time.Time)
	done := make(chan struct{})
	defer close(done)

	aborted := make(chan error, 1)
	go runStallMonitor(done, tick, w, func(cause error) { aborted <- cause })

	// First tick with progress: no abort.
	w.bump()
	tick <- time.Time{}
	select {
	case cause := <-aborted:
		t.Fatalf("progress tick must not abort, got %v", cause)
	default:
	}

	// Second tick without progress: abort with the stall sentinel.
	tick <- time.Time{}
	select {
	case cause := <-aborted:
		if !errors.Is(cause, errRetentionFanOutStalled) {
			t.Fatalf("want errRetentionFanOutStalled, got %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled tick must abort")
	}
}

func TestRunStallMonitorStopsOnDone(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	tick := make(chan time.Time)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		runStallMonitor(done, tick, w, func(error) { t.Error("must not abort after done") })
		close(stopped)
	}()
	close(done)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor must return when done closes")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestProgressWatch|TestRunStallMonitor' -v`
Expected: FAIL — `progressWatch`, `runStallMonitor`, `errRetentionFanOutStalled` undefined.

- [ ] **Step 3: Implement the watchdog**

Create `backend/internal/orchestrator/retention_watchdog.go`:

```go
package orchestrator

import (
	"errors"
	"sync/atomic"
	"time"
)

// retentionFanOutStallWindow is how long a route fan-out may go without a
// single record accepted before it is aborted and the chunk retained for a
// later sweep. Generous on purpose: it exists to stop a sweep from parking
// forever on a jammed pipeline (a destination that passes its admission
// gate but stops draining), not to police throughput (gastrolog-5ct2av).
const retentionFanOutStallWindow = 2 * time.Minute

// errRetentionFanOutStalled is the abort cause when the watchdog fires. The
// abort does not consume the one-shot route flag, so the next sweep retries
// the chunk from scratch (gastrolog-5034va ordering).
var errRetentionFanOutStalled = errors.New(
	"route fan-out made no progress for a full stall window; chunk retained for a later sweep")

// progressWatch is the watchdog's progress ledger: submit workers bump it on
// every completed record submit, the monitor consumes it per tick. stalled()
// is check-and-consume: it reports whether NO bump happened since the
// previous call. Single-consumer (the monitor goroutine); bump is safe from
// any number of workers.
type progressWatch struct {
	progressed atomic.Uint64
	seen       uint64
}

func (w *progressWatch) bump() { w.progressed.Add(1) }

func (w *progressWatch) stalled() bool {
	cur := w.progressed.Load()
	if cur != w.seen {
		w.seen = cur
		return false
	}
	return true
}

// runStallMonitor aborts a fan-out that makes no progress for one full tick
// interval. The tick channel is injected so tests drive it without clocks;
// fireRetentionEvent passes a real time.Ticker channel. Returns when done
// closes or after aborting.
func runStallMonitor(done <-chan struct{}, tick <-chan time.Time, w *progressWatch, abort func(error)) {
	for {
		select {
		case <-done:
			return
		case <-tick:
			if w.stalled() {
				abort(errRetentionFanOutStalled)
				return
			}
		}
	}
}
```

- [ ] **Step 4: Wire it into fireRetentionEvent**

In `backend/internal/orchestrator/retention.go`, inside `fireRetentionEvent` immediately after the `abort` closure is defined (after l.1007) insert:

```go
	// Watchdog: a fan-out that stops making progress — a destination that
	// passes its admission gate but stops draining, a jammed routing input —
	// must abort and retain the chunk instead of parking the sweep forever
	// (gastrolog-5ct2av). Progress is a completed submit: accepted-and-
	// committed, per-record-dropped, or unmatched all count; only a BLOCKED
	// submit does not.
	watch := &progressWatch{}
	watchDone := make(chan struct{})
	defer close(watchDone)
	stallTicker := time.NewTicker(retentionFanOutStallWindow)
	defer stallTicker.Stop()
	go runStallMonitor(watchDone, stallTicker.C, watch, abort)
```

And in the submit worker loop (the `for rec := range jobs` body), after the `switch` on `subErr`, add one line before the loop continues:

```go
				watch.bump()
```

(Placement: last statement of the `for rec := range jobs` body, so every completed `SubmitRetentionRecord` call — success, terminal, or per-record drop — counts as progress; a blocked call never reaches it.)

- [ ] **Step 5: Run the watchdog and retention tests**

Run: `cd backend && go test ./internal/orchestrator/ -run 'TestProgressWatch|TestRunStallMonitor|TestFireRetentionEvent|TestTryRetainChunk' -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/internal/orchestrator/retention_watchdog.go backend/internal/orchestrator/retention_watchdog_test.go backend/internal/orchestrator/retention.go
git commit -m "feat(retention): progress watchdog aborts a stalled route fan-out instead of parking the sweep (gastrolog-5ct2av)"
```

---

### Task 5: Deferral streak and the retention-route-deferred alarm

**Files:**
- Modify: `backend/internal/alert/registry.go` (catalog entry in the High section)
- Modify: `backend/internal/orchestrator/retention.go` (runner fields, deferral notes at the false-return sites, sweep-end evaluation)
- Modify: `docs/alarm-management-design.md` (High table row)
- Test: `backend/internal/orchestrator/retention_deferral_test.go` (create)

**Interfaces:**
- Consumes: `alert.Sink` (`Raise(typeID, instanceKey, detail string)`, `Clear(typeID, instanceKey string)`), `o.alerts` (orchestrator field), Task 4's `errRetentionFanOutStalled`.
- Produces: `const alarmRetentionRouteDeferred = "retention-route-deferred"`, `const retentionDeferralAlarmAfter = 3`, runner methods `noteFanOutDeferral(cause string)`, `noteFanOutProgress()`, `finishSweepDeferralState()`.

- [ ] **Step 1: Write the failing test**

Create `backend/internal/orchestrator/retention_deferral_test.go`:

```go
package orchestrator

// gastrolog-5ct2av: a route-disposition vault whose fan-out is deferred
// sweep after sweep must raise ONE alarm that names the deadlock — the
// incident's operator signal was three unrelated warnings from three
// subsystems. The streak is a pure count of consecutive deferred sweeps;
// nothing persists across restart.

import (
	"strings"
	"sync"
	"testing"

	"gastrolog/internal/glid"
)

// recordingSink is a minimal alert.Sink capturing raises and clears.
type recordingSink struct {
	mu     sync.Mutex
	raises []string // typeID + "|" + instanceKey + "|" + detail
	clears []string // typeID + "|" + instanceKey
}

func (s *recordingSink) Raise(typeID, instanceKey, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raises = append(s.raises, typeID+"|"+instanceKey+"|"+detail)
}

func (s *recordingSink) Clear(typeID, instanceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clears = append(s.clears, typeID+"|"+instanceKey)
}

func TestDeferralStreakRaisesAtThresholdAndClearsOnProgress(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	vaultID := glid.New()
	r := &retentionRunner{
		vaultID:   vaultID,
		vaultName: "first-vault",
		orch:      &Orchestrator{alerts: sink},
	}

	// Two deferred sweeps: below the threshold, no raise.
	for range retentionDeferralAlarmAfter - 1 {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	sink.mu.Lock()
	raised := len(sink.raises)
	sink.mu.Unlock()
	if raised != 0 {
		t.Fatalf("below the threshold no alarm may raise; got %d", raised)
	}

	// Third consecutive deferral: raise, naming vault and cause.
	r.noteFanOutDeferral("destination vault second-vault is at its size budget")
	r.finishSweepDeferralState()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 1 {
		t.Fatalf("want exactly 1 raise at the threshold, got %d", len(sink.raises))
	}
	got := sink.raises[0]
	if !strings.HasPrefix(got, alarmRetentionRouteDeferred+"|"+vaultID.String()+"|") {
		t.Errorf("raise must be typed and instance-keyed by vault: %s", got)
	}
	for _, want := range []string{"first-vault", "size budget", "3 consecutive"} {
		if !strings.Contains(got, want) {
			t.Errorf("alarm detail must contain %q; got: %s", want, got)
		}
	}
}

func TestDeferralStreakResetsOnRoutedChunk(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	r := &retentionRunner{
		vaultID:   glid.New(),
		vaultName: "first-vault",
		orch:      &Orchestrator{alerts: sink},
	}
	for range retentionDeferralAlarmAfter {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	// A sweep that fully routes a chunk clears the alarm and resets.
	r.noteFanOutProgress()
	r.finishSweepDeferralState()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.clears) == 0 {
		t.Fatal("progress must clear the alarm")
	}
	// A single fresh deferral after recovery must not re-raise.
	r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
	r.finishSweepDeferralState()
	if len(sink.raises) != 1 {
		t.Fatalf("streak must reset on progress; raises=%d", len(sink.raises))
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd backend && go test ./internal/orchestrator/ -run TestDeferralStreak -v`
Expected: FAIL — `noteFanOutDeferral`, `finishSweepDeferralState`, `noteFanOutProgress`, `alarmRetentionRouteDeferred`, `vaultName` field usage undefined/absent as needed.

- [ ] **Step 3: Implement the streak and alarm**

In `backend/internal/orchestrator/retention.go`:

Add to the `retentionRunner` struct (near `idleLog`):

```go
	// Deferral streak (gastrolog-5ct2av), guarded by mu: consecutive sweeps
	// whose route fan-out could not run. Pure count, in memory only — a
	// restart starts a fresh streak. sweepDeferred/sweepRouted are the
	// current sweep's scratch flags, folded into the streak by
	// finishSweepDeferralState at sweep end.
	deferralStreak    int
	lastDeferralCause string
	sweepDeferred     bool
	sweepRouted       bool
```

Add the constants next to the other retention constants:

```go
// alarmRetentionRouteDeferred names the deadlock in one alarm: route
// fan-out deferred for consecutive sweeps, so the vault's only drain is
// stopped (gastrolog-5ct2av).
const alarmRetentionRouteDeferred = "retention-route-deferred"

// retentionDeferralAlarmAfter is how many CONSECUTIVE deferred sweeps raise
// the alarm. A count, not a clock: sweeps are the unit of retention time.
const retentionDeferralAlarmAfter = 3
```

Add the three methods:

```go
// noteFanOutDeferral records that this sweep's route fan-out could not run,
// with an operator-readable cause for the alarm detail.
func (r *retentionRunner) noteFanOutDeferral(cause string) {
	r.mu.Lock()
	r.sweepDeferred = true
	r.lastDeferralCause = cause
	r.mu.Unlock()
}

// noteFanOutProgress records that a chunk completed its route fan-out this
// sweep — the deadlock, if one was forming, is not standing.
func (r *retentionRunner) noteFanOutProgress() {
	r.mu.Lock()
	r.sweepRouted = true
	r.mu.Unlock()
}

// finishSweepDeferralState folds the sweep's scratch flags into the streak
// and drives the retention-route-deferred alarm: raise at the threshold,
// clear on progress. Called at the end of every sweep on the runner.
func (r *retentionRunner) finishSweepDeferralState() {
	r.mu.Lock()
	deferred, routed, cause := r.sweepDeferred, r.sweepRouted, r.lastDeferralCause
	r.sweepDeferred, r.sweepRouted = false, false
	switch {
	case routed:
		r.deferralStreak = 0
	case deferred:
		r.deferralStreak++
	}
	streak := r.deferralStreak
	name := r.vaultName
	r.mu.Unlock()

	if r.orch == nil || r.orch.alerts == nil {
		return
	}
	key := r.vaultID.String()
	switch {
	case routed:
		r.orch.alerts.Clear(alarmRetentionRouteDeferred, key)
	case deferred && streak >= retentionDeferralAlarmAfter:
		r.orch.alerts.Raise(alarmRetentionRouteDeferred, key, fmt.Sprintf(
			"Retention route fan-out for vault %s has been deferred for %d consecutive sweeps: %s. "+
				"Expired chunks are retained and any size caps stay engaged until the drain runs. "+
				"Free space on the starved volume, drain or grow the destination vault, or — last resort, "+
				"discards the routed records — set this vault's retention disposition to delete.",
			name, streak, cause))
	}
}
```

Wire the note calls into `fireRetentionEvent`'s false-return sites:

- Drain-gate deferral (Task 3's block): add `r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")` before `return false`.
- Missing vault instance (l.965-969): add `r.noteFanOutDeferral("vault instance unavailable on the sweeping node")` before `return false`.
- The abort path: capture the cause. In the `abort` closure, add `r.noteFanOutDeferral(cause.Error())` inside the `abortOnce.Do`. (Shutdown aborts also count as a deferral note; the sweep-end fold runs on the NEXT sweep after restart with fresh state, so no false alarm.)
- Do NOT note on `r.orch.shuttingDown()` early return (l.942-944) or the unreadable-cursor path (l.971-983 — owned by the chunk-unreadable machinery).

Wire progress: in `tryRetainChunk`, after `applyRetentionDispositionToChunk(id)` returns true AND `r.disposition == system.RetentionDispositionRoute`, call `r.noteFanOutProgress()` (place it immediately after the `if !r.applyRetentionDispositionToChunk(id) { return }` block, guarded on the route disposition so delete-disposition sweeps never touch the streak).

Wire the sweep-end fold: at the end of `sweep()` (after the `if totalMatched == 0 { ... }` block, l.668-670), add:

```go
	r.finishSweepDeferralState()
```

Also add `finishSweepDeferralState()` before the early `return` at the no-candidates exit (l.625-628) — a sweep with nothing eligible neither defers nor progresses, and the fold is what resets the scratch flags:

```go
	if len(sealed) == 0 {
		r.noteIdle("no eligible chunks", len(metas), filtered)
		r.finishSweepDeferralState()
		return
	}
```

In `backend/internal/alert/registry.go`, add to the High section of the catalog (after the `chunk-unreadable` entry):

```go
	{
		IDPrefix: "retention-route-deferred",
		Priority: High,
		Source:   "retention",
		// The consecutive-sweep count at the call site is the condition
		// definition (like chunking-underreplicated's window), so no DelayOn.
		Cause:    "Route-disposition retention on this vault has been unable to fan out for consecutive sweeps — the only mechanism that drains the vault is deferred, so expired chunks accumulate and any size caps stay engaged.",
		Response: "Read the alarm detail for the deferral cause: free space on the starved volume (the drain resumes once free clears the floor band), drain or grow the destination vault, or — last resort, discards the routed records — set the vault's retention disposition to delete.",
	},
```

In `docs/alarm-management-design.md`, add to the High table (after the `vault-max-size-capped` row, keeping column shape):

```markdown
| `retention-route-deferred:<vault>` | retention | Route-disposition fan-out deferred ≥3 consecutive sweeps (the count at the call site is the condition definition) | **Alarm** | High | See detail for the cause: free space on the starved volume, drain/grow the destination vault, or — last resort, discards routed records — set the vault's retention disposition to delete |
```

- [ ] **Step 4: Run the tests**

Run: `cd backend && go test ./internal/orchestrator/ ./internal/alert/ -run 'TestDeferralStreak|TestFireRetentionEvent|Catalog' -v`
Expected: PASS (including any alert-catalog completeness tests).

- [ ] **Step 5: Commit**

```bash
git add backend/internal/orchestrator/retention.go backend/internal/orchestrator/retention_deferral_test.go backend/internal/alert/registry.go docs/alarm-management-design.md
git commit -m "feat(retention): deferral streak raises the retention-route-deferred alarm — one line names the deadlock (gastrolog-5ct2av)"
```

---

### Task 6: Multi-node coverage

**Files:**
- Modify: `backend/internal/server/multinode_test.go` (append tests; reuse `startMNRouteStatsNode`, `submitMNRouteRecords`, `waitForMNRouteStats` at l.2127-2168)

**Interfaces:**
- Consumes: Task 2's gate-error-returning `SubmitRetentionRecord`, `SetRemoteVaultSizeCapped` (exported, `disk_guard.go:755`), harness (`setupMultiNode`, `h.cfgStore.PutRoute`, `h.Node(t, id)`).
- Produces: nothing downstream.

- [ ] **Step 1: Write the test**

Append to `backend/internal/server/multinode_test.go`:

```go
// TestMultiNode_RetentionSubmitDefersOnRemoteCappedDestination pins the
// cross-node deferral seam for gastrolog-5ct2av: a destination vault
// size-capped on a DIFFERENT node must reject the retention submit on the
// sweeping node (via the peer-state lookup the NodeStats broadcast feeds),
// and the same rejection must occur regardless of which node submits.
func TestMultiNode_RetentionSubmitDefersOnRemoteCappedDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline convergence test")
	}
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	ctx := context.Background()

	d1 := h.Node(t, "data-1")
	d2 := h.Node(t, "data-2")

	// Cluster-wide route: retention output of d1's vault lands in d2's vault.
	_ = h.cfgStore.PutRoute(ctx, system.RouteConfig{
		ID:   glid.New(),
		Name: "retain-to-d2",
		Stages: []system.RouteStage{{Match: &system.MatchStage{
			Expression: `_source="retention" AND _vault="` + d1.vaultID.String() + `"`,
		}}},
		Destinations: []glid.GLID{d2.vaultID}, Enabled: true,
	})
	startMNRouteStatsNode(t, d1)
	startMNRouteStatsNode(t, d2)

	// Simulate d2's NodeStats broadcast reporting its vault size-capped.
	// (The broadcast plumbing itself is covered by the peer-state tests;
	// this installs the same lookup production wiring installs.)
	capped := d2.vaultID
	d1.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })
	d2.orch.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == capped })

	rec := chunk.Record{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("expired")}
	// Sweep-node independence: the gate verdict is identical from either node.
	for _, node := range []multinodeTestNode{d1, d2} {
		err := node.orch.SubmitRetentionRecord(ctx, d1.vaultID, rec, "")
		if err == nil || !strings.Contains(err.Error(), "size budget") {
			t.Fatalf("submit on %s: want vault size-budget rejection, got %v", node.nodeID, err)
		}
	}

	// Cap released: the same submit drains and the record is routed.
	d1.orch.SetRemoteVaultSizeCapped(func(glid.GLID) bool { return false })
	d2.orch.SetRemoteVaultSizeCapped(func(glid.GLID) bool { return false })
	submitMNRouteRecords(t, d1, "k", "v", "expired", 3)
	waitForMNRouteStats(t, h.configClient, func(m *gastrologv1.GetRouteStatsResponse) bool {
		return m.TotalMatched >= 3
	})
}
```

(If `GetRouteStatsResponse` has no `TotalMatched` field, assert on `TotalRouted >= 3` as `TestMultiNode_PerRouteStatsAggregated` does. `multinodeTestNode`, `strings` import: match the file's existing imports/types.)

- [ ] **Step 2: Run it (fails before Task 2's change is present, passes after)**

Run: `cd backend && go test ./internal/server/ -run TestMultiNode_RetentionSubmitDefersOnRemoteCappedDestination -v`
Expected: PASS (Tasks 2-3 are already merged on this branch). If it fails with a nil error on the capped submit, the ack wiring regressed.

- [ ] **Step 3: Commit**

```bash
git add backend/internal/server/multinode_test.go
git commit -m "test(retention): multi-node pin — remote-capped destination rejects the retention submit from every node (gastrolog-5ct2av)"
```

---

### Task 7: Full acceptance gate

**Files:** none new.

- [ ] **Step 1: Fast loop**

Run: `just test`
Expected: PASS.

- [ ] **Step 2: Full gate (mandatory before handoff)**

Run: `just backend test-full`
Expected: PASS. Multi-second convergence tests included.

- [ ] **Step 3: Banned-word sweep over the whole diff**

Run: `git diff main --unified=0 | grep -i "t[i]er" ; echo "exit: $?"`
Expected: `exit: 1` (no occurrences anywhere in the diff).

- [ ] **Step 4: Set the issue in_review**

```bash
dcat update --status in_review gastrolog-5ct2av
git add .dogcats/issues.jsonl
git commit -m "chore(dcat): gastrolog-5ct2av to in_review — drain-gate retention fan-out implemented (gastrolog-5ct2av)"
```

Then ask the user to test; do NOT close, merge, or push without explicit instruction.

---

## Self-review notes

- **Spec coverage:** drain-gate reclassification (Task 3), drain submit path (Task 3), gate rejections terminal (Task 2 — spec's "already terminal-abort cleanly" claim was wrong in code; this task makes it true), watchdog (Task 4), alarm + lever text (Task 5), vocabulary + UL (Task 1), recovery-path documentation (spec §Recovery + alarm Response text, Task 5), multi-node + sweep-node independence (Task 6), test gates (Task 7). Restart-mid-deferral acceptance: covered by design (in-memory streak) and pinned implicitly by `TestDeferralStreakResetsOnRoutedChunk`'s fresh-runner construction.
- **Type consistency:** `progressWatch.bump/stalled`, `runStallMonitor`, `errRetentionFanOutStalled` (Task 4) match Task 4's wiring; `noteFanOutDeferral/noteFanOutProgress/finishSweepDeferralState` (Task 5) match the test; `SubmitDrain` (Task 3) matches Task 2's caller swap.
- **Known adaptation points (not placeholders):** cursor-read helper in Task 2 and stats-field name in Task 6 are marked with the concrete fallback to use.
