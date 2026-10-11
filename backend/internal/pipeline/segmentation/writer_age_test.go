package segmentation

// Age completion: a segment that holds records completes once MaxAge has
// elapsed since it was opened, even when no further input arrives. The age
// deadline runs on a manual timer and a manual clock, so every test drives
// time explicitly instead of waiting it out.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/waittest"
)

const testMaxAge = time.Minute

// manualAgeTimer is a deadlineTimer the test fires by hand. Every Reset is
// reported on resets, so a test can observe the writer arming the deadline
// and the duration it asked for.
type manualAgeTimer struct {
	c      chan time.Time
	resets chan time.Duration
	stops  atomic.Int32
}

func newManualAgeTimer() *manualAgeTimer {
	return &manualAgeTimer{
		c:      make(chan time.Time, 1),
		resets: make(chan time.Duration, 64),
	}
}

func (m *manualAgeTimer) C() <-chan time.Time { return m.c }

func (m *manualAgeTimer) Reset(d time.Duration) {
	m.drain()
	m.resets <- d
}

func (m *manualAgeTimer) Stop() {
	m.stops.Add(1)
	m.drain()
}

func (m *manualAgeTimer) drain() {
	select {
	case <-m.c:
	default:
	}
}

func (m *manualAgeTimer) fire() { m.c <- time.Time{} }

// manualClock is the writer's injectable clock.
type manualClock struct{ ns atomic.Int64 }

func newManualClock() *manualClock {
	c := &manualClock{}
	c.ns.Store(time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *manualClock) now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *manualClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type ageHarness struct {
	mgr       *Manager
	in        chan<- Input
	completed <-chan CompletedSegment
	root      string
	vaultID   glid.GLID
	timer     *manualAgeTimer
	clock     *manualClock
}

// startAgeHarness runs one vault writer with MaxAge set and the age deadline
// on a manual timer. cfg's clock and timer hooks are overwritten.
func startAgeHarness(t *testing.T, cfg Config) *ageHarness {
	t.Helper()
	h := &ageHarness{
		root:    t.TempDir(),
		vaultID: glid.New(),
		timer:   newManualAgeTimer(),
		clock:   newManualClock(),
	}
	if cfg.CompletePolicy.MaxAge == 0 {
		cfg.CompletePolicy.MaxAge = testMaxAge
	}
	if cfg.CompletedCap == 0 {
		cfg.CompletedCap = 8
	}
	cfg.Now = h.clock.now
	cfg.newAgeTimer = func() deadlineTimer { return h.timer }
	h.mgr, h.completed = New(cfg)
	in, err := h.mgr.RegisterVault(h.vaultID, h.root, VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	h.in = in
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.mgr.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return h
}

func (h *ageHarness) progress() string {
	return fmt.Sprintf("%s resets_pending=%d", writerProgress(h.mgr), len(h.timer.resets))
}

// awaitArmed returns the duration of the next age-deadline arm.
func (h *ageHarness) awaitArmed(t *testing.T) time.Duration {
	t.Helper()
	return waittest.Recv(t, "age deadline armed", h.timer.resets, h.progress)
}

// barrier returns once the record loop has handled every earlier event: a nil
// record with an ack is rejected at encode, never appended, so it neither
// arms the deadline nor touches the segment.
func (h *ageHarness) barrier(t *testing.T) {
	t.Helper()
	if err := sendAck(t, h.mgr, h.in, nil); err == nil {
		t.Fatal("barrier record encoded; want an encode failure")
	}
}

// awaitCompleted returns the next completed segment once the writer has also
// opened its successor: the completion is announced before the successor
// opens, and a test that moves the clock in between would shift the
// successor's open time.
func (h *ageHarness) awaitCompleted(t *testing.T) CompletedSegment {
	t.Helper()
	seg := waittest.Recv(t, "age-completed segment", h.completed, h.progress)
	h.barrier(t)
	return seg
}

func (h *ageHarness) assertNoCompletion(t *testing.T, when string) {
	t.Helper()
	select {
	case seg := <-h.completed:
		t.Fatalf("%s: segment %s completed with %d records, want none", when, seg.SegmentID, seg.Header.RecordCount)
	default:
	}
}

func (h *ageHarness) assertNoArm(t *testing.T, when string) {
	t.Helper()
	select {
	case d := <-h.timer.resets:
		t.Fatalf("%s: age deadline armed for %v, want no arm", when, d)
	default:
	}
}

// assertPublished checks the completed segment is finalized in completed/
// with the expected record count and no longer in working/.
func (h *ageHarness) assertPublished(t *testing.T, seg CompletedSegment, records uint64) {
	t.Helper()
	if seg.VaultID != h.vaultID {
		t.Fatalf("completed vault = %s, want %s", seg.VaultID, h.vaultID)
	}
	if got := uint64(seg.Header.RecordCount); got != records {
		t.Fatalf("completed segment records = %d, want %d", got, records)
	}
	if seg.Header.Flags&segment.FlagComplete == 0 {
		t.Fatal("completed segment lacks FlagComplete")
	}
	if seg.Path != paths.CompletedSegment(h.root, seg.SegmentID) {
		t.Fatalf("completed path = %s, want %s", seg.Path, paths.CompletedSegment(h.root, seg.SegmentID))
	}
	if _, err := os.Stat(seg.Path); err != nil {
		t.Fatalf("completed segment file: %v", err)
	}
	if _, err := os.Stat(paths.WorkingSegment(h.root, seg.SegmentID)); !os.IsNotExist(err) {
		t.Fatalf("working copy still present: %v", err)
	}
}

// TestQuietSegmentCompletesOnAge: records arrive, then input stops. Once the
// segment's MaxAge expires the writer completes and publishes it with no
// further record to trigger a commit. The next segment arms its own
// deadline, measured from its own open, and completes the same way.
func TestQuietSegmentCompletesOnAge(t *testing.T) {
	t.Parallel()
	h := startAgeHarness(t, Config{SyncBatchSize: 1, SyncBatchWindow: time.Hour})

	for range 3 {
		if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if d := h.awaitArmed(t); d != testMaxAge {
		t.Fatalf("first segment deadline = %v, want %v from open", d, testMaxAge)
	}
	h.assertNoArm(t, "later records into an armed segment")
	h.assertNoCompletion(t, "before the deadline")

	h.clock.advance(testMaxAge)
	h.timer.fire()
	h.assertPublished(t, h.awaitCompleted(t), 3)

	// The successor segment arms for its own age, not the first one's.
	h.clock.advance(testMaxAge / 2)
	if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
		t.Fatalf("append to successor: %v", err)
	}
	if d := h.awaitArmed(t); d != testMaxAge/2 {
		t.Fatalf("successor deadline = %v, want %v (opened at the first segment's completion)", d, testMaxAge/2)
	}
	h.clock.advance(testMaxAge / 2)
	h.timer.fire()
	h.assertPublished(t, h.awaitCompleted(t), 1)
}

// TestEmptySegmentNeverCompletesOnAge: a working segment that never receives a
// record never arms the deadline, and even a spurious wake long past MaxAge
// does not complete it — an idle vault does not churn empty segments.
func TestEmptySegmentNeverCompletesOnAge(t *testing.T) {
	t.Parallel()
	h := startAgeHarness(t, Config{SyncBatchSize: 1, SyncBatchWindow: time.Hour})

	if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
		t.Fatalf("append: %v", err)
	}
	h.awaitArmed(t)
	h.clock.advance(testMaxAge)
	h.timer.fire()
	h.assertPublished(t, h.awaitCompleted(t), 1)

	// The successor is empty: hours pass, nothing arms, nothing completes.
	h.clock.advance(10 * testMaxAge)
	h.barrier(t)
	h.assertNoArm(t, "empty successor segment")

	h.timer.fire()
	h.barrier(t)
	h.assertNoCompletion(t, "spurious wake on an empty segment")
	h.assertNoArm(t, "spurious wake on an empty segment")
	if got := h.mgr.AppendStats()[0].SegmentsCompleted; got != 1 {
		t.Fatalf("SegmentsCompleted = %d, want 1", got)
	}
}

// TestSizeCompletionWithAgeDeadline: size completion still fires on the
// commit that crosses MaxBytes while an age deadline is armed. The deadline
// armed for the size-completed segment is then stale: its wake neither
// completes nor arms the successor, and a wake before the successor's own
// age re-arms for the remainder instead of completing.
func TestSizeCompletionWithAgeDeadline(t *testing.T) {
	t.Parallel()
	h := startAgeHarness(t, Config{
		CompletePolicy:  CompletePolicy{MaxBytes: 256},
		SyncBatchSize:   1,
		SyncBatchWindow: time.Hour,
	})

	var sizeCompleted CompletedSegment
	waittest.Progress(t, "size completion", func() (string, bool) {
		if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
			t.Fatalf("append: %v", err)
		}
		select {
		case sizeCompleted = <-h.completed:
			return "", true
		default:
			return h.progress(), false
		}
	})
	if sizeCompleted.Header.RecordCount == 0 {
		t.Fatal("size-completed segment is empty")
	}
	if d := h.awaitArmed(t); d != testMaxAge {
		t.Fatalf("deadline = %v, want %v", d, testMaxAge)
	}
	h.assertNoArm(t, "records within the size-completed segment")

	// The size-completed segment's deadline fires: it belongs to a segment
	// that is gone, so it neither completes nor arms the empty successor.
	h.clock.advance(testMaxAge / 4)
	h.timer.fire()
	h.barrier(t)
	h.assertNoArm(t, "stale wake")
	h.assertNoCompletion(t, "stale wake")

	// One record into the successor arms its own deadline, measured from the
	// successor's open at the size completion.
	if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
		t.Fatalf("append to successor: %v", err)
	}
	if d := h.awaitArmed(t); d != testMaxAge*3/4 {
		t.Fatalf("successor deadline = %v, want %v", d, testMaxAge*3/4)
	}

	// A wake before the successor's age: no completion, re-armed for the rest.
	h.timer.fire()
	if d := h.awaitArmed(t); d != testMaxAge*3/4 {
		t.Fatalf("early wake re-armed for %v, want %v", d, testMaxAge*3/4)
	}
	h.barrier(t)
	h.assertNoCompletion(t, "early wake")

	h.clock.advance(testMaxAge)
	h.timer.fire()
	h.assertPublished(t, h.awaitCompleted(t), 1)
}

// TestAgeCompletionCommitsParkedAcksAfterFsync: an ack-bearing record parked
// in a commit batch (long MaxCommitDelay) when the age deadline fires is
// committed into the aged segment, and its ack releases only after that
// commit's fsync.
func TestAgeCompletionCommitsParkedAcksAfterFsync(t *testing.T) {
	t.Parallel()
	var syncs atomic.Uint32
	h := startAgeHarness(t, Config{
		SyncBatchSize:   1000,
		SyncBatchWindow: time.Hour,
		MaxCommitDelay:  time.Hour,
		OnSync:          func() { syncs.Add(1) },
	})

	ack := make(chan error, 1)
	h.in <- Input{Record: testRecord(t), Ack: ack}
	h.awaitArmed(t) // the record is in the batch
	select {
	case err := <-ack:
		t.Fatalf("ack released before any commit: %v", err)
	default:
	}

	h.clock.advance(testMaxAge)
	h.timer.fire()
	if err := waittest.Recv(t, "parked ack", ack, h.progress); err != nil {
		t.Fatalf("parked ack = %v, want nil", err)
	}
	if syncs.Load() == 0 {
		t.Fatal("ack released without an fsync")
	}
	h.assertPublished(t, h.awaitCompleted(t), 1)
}

// TestAgeDeadlineAfterCommitFailure: a commit failure abandons the armed
// segment for crash recovery and the reopen fails (disk full), so the writer
// is degraded. The abandoned segment's deadline firing must not complete
// anything nor re-enter degradation, and the reopen backoff — which runs on
// the commit batch's timer — still restores the writer once the disk recovers.
func TestAgeDeadlineAfterCommitFailure(t *testing.T) {
	t.Parallel()
	alerts := &recordingAlerts{}
	var created atomic.Int32
	var diskFull atomic.Bool
	h := startAgeHarness(t, Config{
		Alerts: alerts,
		newSegmentFile: func(path string, meta segment.Meta) (segmentFile, error) {
			if diskFull.Load() {
				return nil, errInjectedCreate
			}
			sf, err := segment.Create(path, meta)
			if err != nil {
				return nil, err
			}
			if created.Add(1) == 1 {
				return &failingSyncSegment{segmentFile: sf}, nil
			}
			return sf, nil
		},
	})

	diskFull.Store(true)
	if err := sendAck(t, h.mgr, h.in, testRecord(t)); !errors.Is(err, errInjectedSync) {
		t.Fatalf("first ack = %v, want injected sync failure", err)
	}
	h.awaitArmed(t)

	h.clock.advance(testMaxAge)
	h.timer.fire()
	if err := sendAck(t, h.mgr, h.in, testRecord(t)); !errors.Is(err, errWriterDegraded) {
		t.Fatalf("ack while degraded = %v, want errWriterDegraded", err)
	}
	h.assertNoCompletion(t, "abandoned segment's deadline")
	if sets, _ := alerts.counts(); sets != 1 {
		t.Fatalf("degraded alarms raised = %d, want 1 (the stale deadline must not re-enter degradation)", sets)
	}

	diskFull.Store(false)
	waittest.Progress(t, "writer recovers after disk came back", func() (string, bool) {
		if err := sendAck(t, h.mgr, h.in, testRecord(t)); err == nil {
			return "", true
		}
		return h.progress(), false
	})
	if _, clears := alerts.counts(); clears == 0 {
		t.Fatal("alarm never cleared after recovery")
	}

	// The recovered writer's fresh segment ages and completes normally.
	h.awaitArmed(t)
	h.clock.advance(testMaxAge)
	h.timer.fire()
	h.assertPublished(t, h.awaitCompleted(t), 1)
}

// TestAgeDeadlineStoppedOnShutdown: the record loop stops the age timer when
// the writer stops.
func TestAgeDeadlineStoppedOnShutdown(t *testing.T) {
	t.Parallel()
	h := startAgeHarness(t, Config{SyncBatchSize: 1, SyncBatchWindow: time.Hour})

	if err := sendAck(t, h.mgr, h.in, testRecord(t)); err != nil {
		t.Fatalf("append: %v", err)
	}
	h.awaitArmed(t)
	if got := h.timer.stops.Load(); got != 0 {
		t.Fatalf("timer stops before shutdown = %d, want 0", got)
	}
	h.mgr.UnregisterVault(h.vaultID)
	if got := h.timer.stops.Load(); got != 1 {
		t.Fatalf("timer stops after shutdown = %d, want 1", got)
	}
}
