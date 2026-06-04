package segmentation

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
)

var errWriterClosed = errors.New("segmentation writer closed")

type encodedWork struct {
	rec     *record.Record
	writeTS time.Time
	body    []byte
}

type vaultWriter struct {
	vaultID glid.GLID
	root    string
	cfg     Config

	in          chan *record.Record
	encoded     chan encodedWork
	completed   chan<- CompletedSegment
	onSync      func()
	openedAt    time.Time
	segmentID   glid.GLID
	seg         *segment.File
	workingPath string

	mu      sync.Mutex
	closed  bool
	started atomic.Bool
	wg      sync.WaitGroup
}

func newVaultWriter(vaultID glid.GLID, root string, cfg Config, completed chan<- CompletedSegment) (*vaultWriter, error) {
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		return nil, err
	}
	queueCap := cfg.EncodeQueueCap
	if queueCap <= 0 {
		queueCap = 64
	}
	w := &vaultWriter{
		vaultID:   vaultID,
		root:      root,
		cfg:       cfg,
		in:        make(chan *record.Record, queueCap),
		encoded:   make(chan encodedWork, queueCap),
		completed: completed,
		onSync:    cfg.OnSync,
	}
	if err := w.openNewSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *vaultWriter) input() chan<- *record.Record {
	return w.in
}

func (w *vaultWriter) run(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	w.wg.Add(2)
	go w.encodeLoop(ctx)
	go w.appendLoop(ctx)
	w.wg.Wait()
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
	w.wg.Wait()
	w.flushAndCloseSegment()
}

func (w *vaultWriter) encodeLoop(ctx context.Context) {
	defer w.wg.Done()
	defer close(w.encoded)

	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-w.in:
			if !ok {
				return
			}
			writeTS := w.cfg.now()
			body, err := segment.EncodeFrame(rec, writeTS)
			if err != nil {
				continue
			}
			work := encodedWork{rec: rec, writeTS: writeTS, body: body}
			select {
			case w.encoded <- work:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (w *vaultWriter) appendLoop(ctx context.Context) {
	defer w.wg.Done()

	syncEvery := w.cfg.syncBatchSize()
	window := w.cfg.syncBatchWindow()
	timer := time.NewTimer(window)
	defer timer.Stop()

	var pendingSync int
	syncNow := func() error {
		if pendingSync == 0 {
			return nil
		}
		pendingSync = 0
		resetSyncTimer(timer, window)
		return w.fsync()
	}

	for {
		select {
		case <-ctx.Done():
			_ = syncNow()
			return
		case work, ok := <-w.encoded:
			if !ok {
				_ = syncNow()
				return
			}
			if err := w.appendFrame(work); err != nil {
				return
			}
			pendingSync++
			if pendingSync >= syncEvery {
				if err := syncNow(); err != nil {
					return
				}
				continue
			}
			if pendingSync == 1 {
				resetSyncTimer(timer, window)
			}
		case <-timer.C:
			if err := syncNow(); err != nil {
				return
			}
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

func (w *vaultWriter) fsync() error {
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
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if err := w.seg.MarkComplete(); err != nil {
		return err
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
	if w.completed != nil {
		select {
		case w.completed <- CompletedSegment{
			VaultID: w.vaultID,
			Meta:    meta,
			Path:    completed,
			Header:  hdr,
		}:
		default:
		}
	}
	return w.openNewSegmentLocked()
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
	_ = w.seg.Sync()
	_ = w.seg.Close()
	w.seg = nil
}
