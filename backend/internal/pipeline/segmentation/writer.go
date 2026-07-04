package segmentation

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

var errWriterClosed = errors.New("segmentation writer closed")

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
	seg                *segment.File
	workingPath        string

	mu      sync.Mutex
	closed  bool
	started atomic.Bool
	done    chan struct{} // closed when run() returns
}

func newVaultWriter(vaultID glid.GLID, root string, cfg Config, vc VaultConfig, completed chan<- CompletedSegment) (*vaultWriter, error) {
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		return nil, err
	}
	queueCap := cfg.EncodeQueueCap
	if queueCap <= 0 {
		queueCap = 64
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
		w.flushAndCloseSegment()
		return
	}
	<-w.done
	w.flushAndCloseSegment()
}

func (w *vaultWriter) recordLoop(ctx context.Context) {
	b := newCommitBatch(w)
	defer b.timer.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = b.commit()
			return
		case in, ok := <-w.in:
			if !ok {
				_ = b.commit()
				return
			}
			if !w.appendInput(b, in) || !w.afterAppend(b) {
				return
			}
		case <-b.timer.C:
			b.timerArmed = false
			if !b.commit() {
				return
			}
		}
	}
}

func (w *vaultWriter) appendInput(b *commitBatch, in Input) bool {
	writeTS := w.cfg.now()
	body, err := segment.EncodeFrame(in.Record, writeTS)
	if err != nil {
		if in.Ack != nil {
			in.Ack <- err
		}
		return true
	}
	return b.append(encodedWork{rec: in.Record, writeTS: writeTS, body: body, ack: in.Ack})
}

// afterAppend applies the commit policy once a frame has been added to the batch.
// Returns false if the writer should stop (a commit failed).
func (w *vaultWriter) afterAppend(b *commitBatch) bool {
	switch {
	case b.pendingSync >= w.syncEvery:
		// Size cap: hard flush regardless of regime.
		return b.commit()
	case !b.hasAck:
		// Fire-and-forget: arm the lazy window once per batch.
		if !b.timerArmed {
			b.armTimer(w.syncWindow)
		}
		return true
	case w.commitDelay > 0:
		// Coalesce ack records within the delay window.
		if !b.timerArmed {
			b.armTimer(w.commitDelay)
		}
		return true
	default:
		// Pure group commit: drain whatever is queued, then fsync once.
		return w.drainAvailable(b) && b.commit()
	}
}

// commitBatch accumulates appended frames and their parked acks between fsyncs.
type commitBatch struct {
	w           *vaultWriter
	timer       *time.Timer
	timerArmed  bool
	parked      []chan<- error
	pendingSync int
	hasAck      bool
}

func newCommitBatch(w *vaultWriter) *commitBatch {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	return &commitBatch{w: w, timer: timer}
}

func (b *commitBatch) armTimer(d time.Duration) {
	if d <= 0 {
		d = time.Millisecond
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

// append appends a frame and parks its ack. Returns false on append error, after
// nacking the offending and any already-parked acks so the loop can stop.
func (b *commitBatch) append(work encodedWork) bool {
	if err := b.w.appendFrame(work); err != nil {
		if work.ack != nil {
			work.ack <- err
		}
		b.releaseParked(err)
		return false
	}
	b.pendingSync++
	if work.ack != nil {
		b.parked = append(b.parked, work.ack)
		b.hasAck = true
	}
	return true
}

// commit syncs the pending batch (unless fsync is disabled) and releases the
// parked acks with the result. Returns false if the writer should stop.
func (b *commitBatch) commit() bool {
	if b.pendingSync == 0 {
		b.disarmTimer()
		return true
	}
	var err error
	if b.w.disableFsync {
		err = b.w.maybeCloseNoSync()
	} else {
		err = b.w.syncAndMaybeClose()
	}
	b.releaseParked(err)
	b.pendingSync = 0
	b.hasAck = false
	b.disarmTimer()
	return err == nil
}

func (b *commitBatch) releaseParked(err error) {
	for _, ack := range b.parked {
		ack <- err
	}
	b.parked = b.parked[:0]
}

// drainAvailable appends every input currently buffered in the in queue without
// blocking, forming the group-commit batch. Returns false on append error.
func (w *vaultWriter) drainAvailable(b *commitBatch) bool {
	for {
		select {
		case in, ok := <-w.in:
			if !ok {
				return true
			}
			if !w.appendInput(b, in) {
				return false
			}
		default:
			return true
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

func (w *vaultWriter) appendFrame(work encodedWork) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return errWriterClosed
	}
	return w.seg.AppendFrame(work.rec, work.writeTS, work.body)
}

// syncAndMaybeClose fsyncs the open segment, notifies OnSync, and rotates the
// segment if the close policy is met.
func (w *vaultWriter) syncAndMaybeClose() error {
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
	return w.maybeCloseLocked()
}

// maybeCloseNoSync rotates the segment on the close policy without any fsync
// (DisableFsync vaults).
func (w *vaultWriter) maybeCloseNoSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return errWriterClosed
	}
	return w.maybeCloseLocked()
}

func (w *vaultWriter) maybeCloseLocked() error {
	size, err := w.seg.Size()
	if err != nil {
		return err
	}
	if w.shouldClose(size) {
		return w.closeSegmentLocked()
	}
	return nil
}

func (w *vaultWriter) shouldClose(size int64) bool {
	policy := w.cfg.ClosePolicy
	if policy.MaxBytes > 0 && size >= 0 && uint64(size) >= policy.MaxBytes {
		return true
	}
	if policy.MaxAge > 0 && w.cfg.now().Sub(w.openedAt) >= policy.MaxAge {
		return true
	}
	return false
}

func (w *vaultWriter) closeSegmentLocked() error {
	if w.seg == nil {
		return nil
	}
	if w.seg.Header().RecordCount == 0 {
		return nil
	}
	if err := w.completeWorkingSegmentLocked(); err != nil {
		return err
	}
	return w.openNewSegmentLocked()
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
	meta := segment.Meta{ID: w.segmentID, VaultID: w.vaultID}
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
			VaultID: w.vaultID,
			Meta:    meta,
			Path:    completed,
			Header:  hdr,
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
	sf, err := segment.Create(w.workingPath, segment.Meta{
		ID:      w.segmentID,
		VaultID: w.vaultID,
	})
	if err != nil {
		return err
	}
	w.seg = sf
	return nil
}

func (w *vaultWriter) flushAndCloseSegment() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg == nil {
		return
	}
	if w.seg.Header().RecordCount == 0 {
		w.discardWorkingSegmentLocked()
		return
	}
	_ = w.completeWorkingSegmentLocked()
}
