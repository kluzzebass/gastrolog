package segmentation

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

var errWriterClosed = errors.New("segmentation writer closed")

// errWriterDegraded nacks ack-bearing inputs while the writer has no usable
// working segment (commit failed and reopen has not yet succeeded).
var errWriterDegraded = errors.New("segmentation writer degraded: no usable working segment")

// segmentFile is the subset of *segment.File the writer drives. It exists so
// fault-injection tests can wrap Sync/Append/rotation failures.
type segmentFile interface {
	AppendFrames(frames []segment.Frame) error
	Sync() error
	DataSize() int64
	Header() segment.Header
	Finalize() error
	Close() error
}

func defaultCreateSegmentFile(path string, meta segment.Meta) (segmentFile, error) {
	return segment.Create(path, meta)
}

// reopenBaseDelay/reopenMaxDelay bound the degraded-writer reopen backoff:
// quick first retry for transient errors, 5s steady-state while the disk
// stays sick.
const (
	reopenBaseDelay = 100 * time.Millisecond
	reopenMaxDelay  = 5 * time.Second
)

type encodedWork struct {
	rec     *record.Record
	writeTS time.Time
	body    []byte
	ack     chan<- error
}

type vaultWriter struct {
	vaultID glid.GLID
	root    string
	cfg     Config
	log     *slog.Logger
	alerts  AlertSink
	dropped *atomic.Uint64 // manager-wide dropped-records counter

	// Cumulative throughput counters for the stats broadcast; the collector's
	// rolling windows turn them into per-second rates (gastrolog-4eh5ns).
	recordsAppended atomic.Uint64 // frames appended to the working segment
	bytesAppended   atomic.Uint64 // frame body bytes appended
	recordsDurable  atomic.Uint64 // records released by a successful group commit

	// Resolved per-vault commit/fsync tuning (Config default + VaultConfig override).
	syncEvery    int
	syncWindow   time.Duration
	commitDelay  time.Duration
	disableFsync bool

	in                 chan Input
	completed          chan<- CompletedSegment
	onSync             func()
	onCompletedDropped func()
	openedAt           time.Time
	segmentID          glid.GLID
	seg                segmentFile
	workingPath        string

	// Degraded-writer state, owned by the recordLoop goroutine.
	degraded      bool
	reopenDelay   time.Duration
	degradedDrops uint64 // nil-ack records dropped in the current episode
	lastFailure   error

	mu      sync.Mutex
	closed  bool
	started atomic.Bool
	done    chan struct{} // closed when run() returns
}

func newVaultWriter(vaultID glid.GLID, root string, cfg Config, vc VaultConfig, completed chan<- CompletedSegment, dropped *atomic.Uint64) (*vaultWriter, error) {
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		return nil, err
	}
	if err := recoverWorkingSegments(root, vaultID, completed, cfg.OnCompletedDropped,
		compSegmentation.Apply(logging.Default(cfg.Logger))); err != nil {
		return nil, err
	}
	queueCap := cfg.EncodeQueueCap
	if queueCap <= 0 {
		queueCap = 8192
	}
	syncEvery := vc.SyncBatchSize
	if syncEvery <= 0 {
		syncEvery = cfg.syncBatchSize()
	}
	syncWindow := vc.SyncBatchWindow
	if syncWindow <= 0 {
		syncWindow = cfg.syncBatchWindow()
	}
	commitDelay := vc.MaxCommitDelay
	if commitDelay <= 0 {
		commitDelay = cfg.MaxCommitDelay
	}
	w := &vaultWriter{
		vaultID:            vaultID,
		root:               root,
		cfg:                cfg,
		log:                compSegmentation.Apply(logging.Default(cfg.Logger)).With("vault", vaultID),
		alerts:             cfg.Alerts,
		dropped:            dropped,
		syncEvery:          syncEvery,
		syncWindow:         syncWindow,
		commitDelay:        commitDelay,
		disableFsync:       vc.DisableFsync || cfg.DisableFsync,
		in:                 make(chan Input, queueCap),
		completed:          completed,
		onSync:             cfg.OnSync,
		onCompletedDropped: cfg.OnCompletedDropped,
		done:               make(chan struct{}),
	}
	if err := w.openNewSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

// recoverWorkingSegments finalizes working/ segments a previous process left
// behind (crash or kill before the complete policy fired). Records in them were
// fsynced and ACKED — dropping them is post-accept loss, a cardinal-rule
// violation. Non-empty orphans are finalized (segment.Open reconciles a torn
// tail down to the synced prefix) and moved to completed/, where the
// completed channel — or distribution's stranded rescan when the channel is
// full — publishes them like any freshly-completed segment. Empty orphans are
// deleted. Files that fail to open or finalize are left in place and logged:
// recovery must never destroy bytes it cannot prove empty.
func recoverWorkingSegments(root string, vaultID glid.GLID, completed chan<- CompletedSegment, onCompletedDropped func(), log *slog.Logger) error {
	ids, err := paths.ListSegmentIDs(paths.WorkingDir(root))
	if err != nil {
		return err
	}
	for id := range ids {
		workingPath := paths.WorkingSegment(root, id)
		sf, err := segment.Open(workingPath)
		if err != nil {
			log.Warn("orphaned working segment unreadable; left in place",
				"vault", vaultID, "segment", id, "error", err)
			continue
		}
		if sf.Header().RecordCount == 0 {
			_ = sf.Close()
			_ = os.Remove(workingPath)
			continue
		}
		if err := sf.Finalize(); err != nil {
			_ = sf.Close()
			log.Warn("orphaned working segment finalize failed; left in place",
				"vault", vaultID, "segment", id, "error", err)
			continue
		}
		hdr := sf.Header()
		if err := sf.Close(); err != nil {
			log.Warn("orphaned working segment close failed; left in place",
				"vault", vaultID, "segment", id, "error", err)
			continue
		}
		completedPath := paths.CompletedSegment(root, id)
		if err := os.Rename(workingPath, completedPath); err != nil {
			log.Warn("orphaned working segment rename failed; left in place",
				"vault", vaultID, "segment", id, "error", err)
			continue
		}
		log.Info("recovered orphaned working segment",
			"vault", vaultID, "segment", id, "records", hdr.RecordCount)
		if completed == nil {
			continue
		}
		select {
		case completed <- CompletedSegment{
			VaultID:   vaultID,
			SegmentID: id,
			Path:      completedPath,
			Header:    hdr,
		}:
		default:
			if onCompletedDropped != nil {
				onCompletedDropped()
			}
		}
	}
	return nil
}

func (w *vaultWriter) input() chan<- Input {
	return w.in
}

func (w *vaultWriter) run(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	defer close(w.done)
	w.recordLoop(ctx)
}

func (w *vaultWriter) stop() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	close(w.in)
	w.mu.Unlock()
	// Claim the started flag: if run() never launched, this makes a late run() a
	// no-op and there is no goroutine to wait on (avoids WaitGroup.Add racing
	// Wait when register/unregister flap). Otherwise run() owns the flag and we
	// wait for it to drain the closed input and exit.
	if w.started.CompareAndSwap(false, true) {
		w.flushAndCompleteSegment()
		return
	}
	<-w.done
	w.flushAndCompleteSegment()
}

func (w *vaultWriter) recordLoop(ctx context.Context) {
	b := newCommitBatch(w)
	defer b.timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.shutdownCommit(b)
			return
		case in, ok := <-w.in:
			if !ok {
				w.shutdownCommit(b)
				return
			}
			w.handleInput(b, in)
		case <-b.timer.C:
			b.timerArmed = false
			if w.degraded {
				w.tryReopen(b)
			} else if err := b.commit(); err != nil {
				w.enterDegraded(b, err)
			}
		}
	}
}

// shutdownCommit flushes the final batch on loop exit. A failure here cannot
// wedge anything (the writer is going away) but must not be silent: the
// unsynced tail stays in working/ for crash recovery at next registration.
func (w *vaultWriter) shutdownCommit(b *commitBatch) {
	if err := b.commit(); err != nil {
		w.log.Error("final commit failed on writer shutdown; unsynced records remain in the working segment for crash recovery",
			"segment", w.segmentID, "error", err)
	}
}

// handleInput encodes and appends one input under the active commit policy,
// downgrading the writer instead of killing the loop on disk errors: a dead
// writer with a still-open bounded input queue wedges routing — and then the
// node — once the queue fills (gastrolog-1c9f5l).
func (w *vaultWriter) handleInput(b *commitBatch, in Input) {
	if w.degraded {
		w.rejectDegraded(in)
		return
	}
	writeTS := w.cfg.now()
	body, err := segment.EncodeFrame(in.Record, writeTS)
	if err != nil {
		w.rejectEncodeFailure(in, err)
		return
	}
	b.append(encodedWork{rec: in.Record, writeTS: writeTS, body: body, ack: in.Ack})
	w.afterAppend(b)
}

// afterAppend applies the commit policy once a frame has been added to the batch.
func (w *vaultWriter) afterAppend(b *commitBatch) {
	switch {
	case b.pendingSync >= w.syncEvery:
		// Size cap: hard flush regardless of regime.
		if err := b.commit(); err != nil {
			w.enterDegraded(b, err)
		}
	case !b.hasAck:
		// Fire-and-forget: arm the lazy window once per batch.
		if !b.timerArmed {
			b.armTimer(w.syncWindow)
		}
	case w.commitDelay > 0:
		// Coalesce ack records within the delay window.
		if !b.timerArmed {
			b.armTimer(w.commitDelay)
		}
	default:
		// Pure group commit: drain whatever is queued, then fsync once.
		w.drainAvailable(b)
		if err := b.commit(); err != nil {
			w.enterDegraded(b, err)
		}
	}
}

// rejectEncodeFailure handles a record whose frame cannot be encoded (nil
// record, unencodable attrs). This is a per-record data problem, not a disk
// problem: the writer stays healthy, but the record must not vanish silently
// — an accepted, minted record dropped without a trace is post-accept loss.
func (w *vaultWriter) rejectEncodeFailure(in Input, err error) {
	if in.Ack != nil {
		in.Ack <- err
		return
	}
	w.dropped.Add(1)
	var eventID record.EventID
	if in.Record != nil {
		eventID = in.Record.EventID
	}
	w.log.Error("dropping record: frame encode failed", "event_id", eventID, "error", err)
}

// rejectDegraded fails an input while the writer has no working segment.
// Ack-bearing producers get an immediate nack (ack-after-durable upstream
// holds the record); fire-and-forget records are counted and logged sampled
// so a sick disk cannot flood the log at ingest rate.
func (w *vaultWriter) rejectDegraded(in Input) {
	if in.Ack != nil {
		in.Ack <- errWriterDegraded
		return
	}
	w.dropped.Add(1)
	w.degradedDrops++
	if w.degradedDrops == 1 {
		w.log.Error("dropping fire-and-forget records while writer is degraded; drop total reported on reopen attempts",
			"error", w.lastFailure)
	}
}

func (w *vaultWriter) alertID() string {
	return "segmentation-writer:" + w.vaultID.String()
}

// enterDegraded abandons the suspect working segment and rotates to a fresh
// one. The failed batch's parked acks were already nacked by commit/append.
// The abandoned file keeps its fsynced prefix and is reconciled by
// working-segment crash recovery at the next registration — after an fsync
// failure the kernel may mark dirty pages clean, so re-syncing the same fd
// can falsely succeed; never trust that segment again.
func (w *vaultWriter) enterDegraded(b *commitBatch, cause error) {
	b.disarmTimer()
	b.frames = b.frames[:0]
	b.frameBytes = 0
	b.pendingSync = 0
	b.hasAck = false
	w.lastFailure = cause
	w.degraded = true
	w.degradedDrops = 0

	w.mu.Lock()
	segID, path := w.segmentID, w.workingPath
	if w.seg != nil {
		_ = w.seg.Close()
		w.seg = nil
	}
	w.mu.Unlock()

	w.log.Error("segment commit failed; abandoning working segment for crash recovery and rotating",
		"segment", segID, "path", path, "error", cause)
	if w.alerts != nil {
		w.alerts.Set(w.alertID(), alert.Error, "segmentation",
			"vault "+w.vaultID.String()+" segment commit failed: "+cause.Error())
	}
	w.tryReopen(b)
}

// tryReopen attempts to open a fresh working segment. On failure the writer
// stays degraded and re-arms the batch timer as a reopen backoff (the timer
// is otherwise idle while degraded — no batch can exist without a segment).
func (w *vaultWriter) tryReopen(b *commitBatch) {
	if err := w.openNewSegment(); err != nil {
		switch {
		case w.reopenDelay == 0:
			w.reopenDelay = reopenBaseDelay
		case w.reopenDelay < reopenMaxDelay:
			w.reopenDelay *= 2
		}
		w.log.Error("reopen failed; writer degraded — nacking ack records, dropping fire-and-forget records",
			"error", err, "retry_in", w.reopenDelay, "dropped_this_episode", w.degradedDrops)
		b.armTimer(w.reopenDelay)
		return
	}
	w.degraded = false
	w.reopenDelay = 0
	w.lastFailure = nil
	w.log.Info("segmentation writer recovered: fresh working segment opened",
		"segment", w.segmentID, "dropped_while_degraded", w.degradedDrops)
	w.degradedDrops = 0
	if w.alerts != nil {
		w.alerts.Clear(w.alertID())
	}
}

// commitBatch accumulates encoded frames and their parked acks between
// fsyncs. Frames stay in memory until commit flushes them with ONE data
// write + ONE header rewrite (gastrolog-1ojsm6) — the previous per-record
// 3-pwrite shape capped a writer at ~25K rec/s. The loss window is
// unchanged: acks release only after the commit fsync, and un-acked
// records were never durable before their fsync either.
type commitBatch struct {
	w           *vaultWriter
	timer       *time.Timer
	timerArmed  bool
	frames      []segment.Frame
	frameBytes  uint64
	parked      []chan<- error
	pendingSync int
	hasAck      bool
}

// syncTimerPark is the arbitrary far-future duration the commit batch's sync
// timer is constructed with: time.NewTimer requires some duration, and the
// timer is stopped (channel drained) immediately after creation, so it never
// fires at this value. armTimer gives it its real deadline per batch.
const syncTimerPark = time.Hour

// syncTimerArmFloor clamps non-positive arm requests (an already-due or zero
// commit window) to a minimal real duration, so a due batch still flows
// through the timer channel in the select loop instead of needing a separate
// fire-immediately branch.
const syncTimerArmFloor = time.Millisecond

func newCommitBatch(w *vaultWriter) *commitBatch {
	timer := time.NewTimer(syncTimerPark)
	if !timer.Stop() {
		<-timer.C
	}
	return &commitBatch{w: w, timer: timer}
}

func (b *commitBatch) armTimer(d time.Duration) {
	if d <= 0 {
		d = syncTimerArmFloor
	}
	resetSyncTimer(b.timer, d)
	b.timerArmed = true
}

func (b *commitBatch) disarmTimer() {
	if !b.timerArmed {
		return
	}
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
	b.timerArmed = false
}

// append stashes an encoded frame in the batch and parks its ack. Disk I/O
// happens at commit (flush + fsync); a stash cannot fail.
func (b *commitBatch) append(work encodedWork) {
	b.frames = append(b.frames, segment.Frame{Rec: work.rec, Body: work.body})
	b.frameBytes += uint64(len(work.body))
	b.pendingSync++
	if work.ack != nil {
		b.parked = append(b.parked, work.ack)
		b.hasAck = true
	}
}

// commit flushes the stashed frames (one data write + one header rewrite),
// syncs (unless fsync is disabled), and releases the parked acks with the
// result. A non-nil return means the batch's segment is suspect and the
// writer must rotate.
func (b *commitBatch) commit() error {
	if b.pendingSync == 0 {
		b.disarmTimer()
		return nil
	}
	err := b.w.appendFrames(b.frames)
	if err == nil {
		b.w.recordsAppended.Add(uint64(b.pendingSync)) //nolint:gosec // pendingSync >= 0
		b.w.bytesAppended.Add(b.frameBytes)
		if b.w.disableFsync {
			err = b.w.maybeCompleteNoSync()
		} else {
			err = b.w.syncAndMaybeComplete()
		}
	}
	b.releaseParked(err)
	if err == nil {
		b.w.recordsDurable.Add(uint64(b.pendingSync)) //nolint:gosec // pendingSync >= 0
	}
	b.frames = b.frames[:0]
	b.frameBytes = 0
	b.pendingSync = 0
	b.hasAck = false
	b.disarmTimer()
	return err
}

func (b *commitBatch) releaseParked(err error) {
	for _, ack := range b.parked {
		ack <- err
	}
	b.parked = b.parked[:0]
}

// drainAvailable stashes every input currently buffered in the in queue
// without blocking, forming the group-commit batch. Encode failures reject
// that record and keep draining; disk errors surface at commit.
func (w *vaultWriter) drainAvailable(b *commitBatch) {
	for {
		select {
		case in, ok := <-w.in:
			if !ok {
				return
			}
			writeTS := w.cfg.now()
			body, err := segment.EncodeFrame(in.Record, writeTS)
			if err != nil {
				w.rejectEncodeFailure(in, err)
				continue
			}
			b.append(encodedWork{rec: in.Record, writeTS: writeTS, body: body, ack: in.Ack})
		default:
			return
		}
	}
}

func resetSyncTimer(timer *time.Timer, window time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(window)
}

func (w *vaultWriter) appendFrames(frames []segment.Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return errWriterClosed
	}
	return w.seg.AppendFrames(frames)
}

// syncAndMaybeComplete fsyncs the open segment, notifies OnSync, and rotates the
// segment if the complete policy is met.
func (w *vaultWriter) syncAndMaybeComplete() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return errWriterClosed
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if w.onSync != nil {
		w.onSync()
	}
	return w.maybeCompleteLocked()
}

// maybeCompleteNoSync rotates the segment on the complete policy without any fsync
// (DisableFsync vaults).
func (w *vaultWriter) maybeCompleteNoSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return errWriterClosed
	}
	return w.maybeCompleteLocked()
}

func (w *vaultWriter) maybeCompleteLocked() error {
	// In-memory append anchor: the per-commit Stat syscall bought nothing
	// (gastrolog-1ojsm6).
	if w.shouldComplete(w.seg.DataSize()) {
		return w.rotateSegmentLocked()
	}
	return nil
}

func (w *vaultWriter) shouldComplete(size int64) bool {
	policy := w.cfg.CompletePolicy
	if policy.MaxBytes > 0 && size >= 0 && uint64(size) >= policy.MaxBytes {
		return true
	}
	if policy.MaxAge > 0 && w.cfg.now().Sub(w.openedAt) >= policy.MaxAge {
		return true
	}
	return false
}

func (w *vaultWriter) rotateSegmentLocked() error {
	if w.seg == nil {
		return nil
	}
	if w.seg.Header().RecordCount == 0 {
		return nil
	}
	if err := w.completeWorkingSegmentLocked(); err != nil {
		return err
	}
	// Hand the finalized segment's batch buffer to its successor: a fresh
	// multi-MB buffer per rotation was the top allocation site under pour
	// load (gastrolog-11y2iv). Optional interface — test fakes don't carry
	// scratch.
	var scratch []byte
	if sc, ok := w.seg.(scratchCarrier); ok {
		scratch = sc.TakeScratch()
	}
	if err := w.openNewSegmentLocked(); err != nil {
		return err
	}
	if sc, ok := w.seg.(scratchCarrier); ok && scratch != nil {
		sc.GiveScratch(scratch)
	}
	return nil
}

// scratchCarrier is the optional segmentFile capability for batch-buffer
// handoff across rotation (implemented by *segment.File).
type scratchCarrier interface {
	TakeScratch() []byte
	GiveScratch([]byte)
}

// completeWorkingSegmentLocked finalizes the open segment and moves it to completed/.
// The caller must hold w.mu and ensure RecordCount > 0.
func (w *vaultWriter) completeWorkingSegmentLocked() error {
	if err := w.seg.Finalize(); err != nil {
		return err
	}
	// Durability barrier AFTER Finalize: it truncates, writes the index
	// tails, sorts via writable mmap, and rewrites the header. Syncing
	// before it (as this used to) left all of that unsynced at publish —
	// after a crash the registry-referenced segment could fail
	// segment.Open on the origin and collectors got non-retryable
	// ErrCorruptSegment (gastrolog-4mqy06). DisableFsync vaults opt out
	// of durability wholesale (dev/load testing only).
	if !w.disableFsync {
		if err := w.seg.Sync(); err != nil {
			return err
		}
	}
	hdr := w.seg.Header()
	working := w.workingPath
	completed := paths.CompletedSegment(w.root, w.segmentID)
	if err := w.seg.Close(); err != nil {
		return err
	}
	w.seg = nil
	if err := os.Rename(working, completed); err != nil {
		return err
	}
	if !w.disableFsync {
		if err := paths.SyncDir(paths.CompletedDir(w.root)); err != nil {
			return err
		}
	}
	if w.completed != nil {
		select {
		case w.completed <- CompletedSegment{
			VaultID:   w.vaultID,
			SegmentID: w.segmentID,
			Path:      completed,
			Header:    hdr,
		}:
		default:
			if w.onCompletedDropped != nil {
				w.onCompletedDropped()
			}
		}
	}
	return nil
}

// discardWorkingSegmentLocked closes and deletes an empty in-progress segment.
// The caller must hold w.mu.
func (w *vaultWriter) discardWorkingSegmentLocked() {
	path := w.workingPath
	_ = w.seg.Close()
	w.seg = nil
	_ = os.Remove(path)
}

func (w *vaultWriter) openNewSegment() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.openNewSegmentLocked()
}

func (w *vaultWriter) openNewSegmentLocked() error {
	w.segmentID = glid.New()
	w.workingPath = paths.WorkingSegment(w.root, w.segmentID)
	w.openedAt = w.cfg.now()
	create := w.cfg.newSegmentFile
	if create == nil {
		create = defaultCreateSegmentFile
	}
	sf, err := create(w.workingPath, segment.Meta{
		ID:      w.segmentID,
		VaultID: w.vaultID,
	})
	if err != nil {
		return err
	}
	w.seg = sf
	return nil
}

func (w *vaultWriter) flushAndCompleteSegment() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return
	}
	if w.seg.Header().RecordCount == 0 {
		w.discardWorkingSegmentLocked()
		return
	}
	if err := w.completeWorkingSegmentLocked(); err != nil {
		w.log.Error("final segment flush failed on writer close; segment left in working/ for crash recovery",
			"segment", w.segmentID, "path", w.workingPath, "error", err)
	}
}
