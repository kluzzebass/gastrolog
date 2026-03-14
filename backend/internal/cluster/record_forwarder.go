package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// forwardChanCap is the per-node channel capacity. When full, new
	// records are dropped to bound memory and prevent backpressure.
	forwardChanCap = 1000

	// forwardBatchSize is the max records per flush.
	forwardBatchSize = 100

	// forwardFlushInterval is the max time between flushes.
	forwardFlushInterval = 100 * time.Millisecond
)

// forwardEntry is a single record queued for forwarding.
type forwardEntry struct {
	vaultID uuid.UUID
	record  chunk.Record
}

// nodeForwarder manages a per-node channel and flush goroutine.
type nodeForwarder struct {
	ch   chan forwardEntry
	done chan struct{}

	// Backoff state — only accessed from the flushLoop goroutine.
	failures int           // consecutive send failures
	backoff  time.Duration // current backoff duration
	everSent bool          // true after first successful batch
}

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second
)

// RecordForwarder implements orchestrator.RecordForwarder with per-node
// buffered channels and batched unary RPCs.
type RecordForwarder struct {
	peers  *PeerConns
	logger *slog.Logger
	cw     *chanwatch.Watcher

	sent atomic.Int64 // records successfully sent via ForwardRecords RPCs

	mu       sync.Mutex
	nodes    map[string]*nodeForwarder // keyed by node ID
	wg       sync.WaitGroup
	closed   bool
	stop     chan struct{}          // closed on Close() to unblock backoff sleeps
	cwCancel context.CancelFunc    // cancels the chanwatch goroutine
}

// NewRecordForwarder creates a RecordForwarder using the shared PeerConns pool.
func NewRecordForwarder(peers *PeerConns, logger *slog.Logger) *RecordForwarder {
	rf := &RecordForwarder{
		peers:  peers,
		logger: logger,
		cw:     chanwatch.New(logger, 1*time.Second),
		nodes:  make(map[string]*nodeForwarder),
		stop:   make(chan struct{}),
	}
	// Run the channel pressure watcher until Close().
	ctx, cancel := context.WithCancel(context.Background())
	rf.cwCancel = cancel
	rf.wg.Go(func() {
		rf.cw.Run(ctx)
	})
	return rf
}

// Forward enqueues records for delivery to the given node. Non-blocking:
// if the per-node buffer is full, records are dropped with a warning.
func (rf *RecordForwarder) Forward(_ context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	rf.mu.Lock()
	if rf.closed {
		rf.mu.Unlock()
		return errors.New("forwarder closed")
	}
	nf, ok := rf.nodes[nodeID]
	if !ok {
		nf = rf.startNode(nodeID)
	}
	rf.mu.Unlock()

	for _, rec := range records {
		select {
		case nf.ch <- forwardEntry{vaultID: vaultID, record: rec}:
		default:
			if !rf.stopping() {
				rf.logger.Info("forward buffer full, dropping record",
					"node", nodeID, "vault", vaultID)
			}
		}
	}
	return nil
}

// startNode creates and starts a per-node forwarder goroutine.
// Must be called with rf.mu held.
func (rf *RecordForwarder) startNode(nodeID string) *nodeForwarder {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf
	rf.cw.Watch("forward:"+nodeID, func() (int, int) {
		return len(nf.ch), cap(nf.ch)
	}, 0.9)
	rf.wg.Add(1)
	go rf.flushLoop(nodeID, nf)
	return nf
}

// flushLoop drains the per-node channel in batches.
func (rf *RecordForwarder) flushLoop(nodeID string, nf *nodeForwarder) {
	defer rf.wg.Done()
	defer close(nf.done)

	batch := make([]forwardEntry, 0, forwardBatchSize)
	timer := time.NewTimer(forwardFlushInterval)
	defer timer.Stop()

	for {
		// If backing off, wait for the backoff duration instead of the
		// normal flush interval. Records keep accumulating in the channel
		// and will be sent in the next successful batch.
		if nf.backoff > 0 {
			// During backoff, DON'T drain the channel. Let it fill up so
			// Forward() sees the backpressure and logs overflow drops.
			timer.Reset(nf.backoff)
			select {
			case <-timer.C:
			case <-rf.stop:
				return
			}
			// Backoff expired — drain channel and retry.
			batch = rf.drainChannel(nf, batch)
			if len(batch) > 0 {
				rf.sendBatchWithBackoff(nodeID, nf, batch)
				batch = batch[:0]
			}
			continue
		}

		// Wait for first entry or channel close.
		select {
		case entry, ok := <-nf.ch:
			if !ok {
				// Channel closed — flush remaining and exit.
				if len(batch) > 0 {
					rf.sendBatchWithBackoff(nodeID, nf, batch)
				}
				return
			}
			batch = append(batch, entry)
		case <-timer.C:
			if len(batch) > 0 {
				rf.sendBatchWithBackoff(nodeID, nf, batch)
				batch = batch[:0]
			}
			timer.Reset(forwardFlushInterval)
			continue
		}

		// Drain up to batch size.
		batch = rf.drainChannel(nf, batch)

		if len(batch) >= forwardBatchSize {
			rf.sendBatchWithBackoff(nodeID, nf, batch)
			batch = batch[:0]
			timer.Reset(forwardFlushInterval)
		}
	}
}

// drainChannel reads available entries from the channel up to forwardBatchSize.
func (rf *RecordForwarder) drainChannel(nf *nodeForwarder, batch []forwardEntry) []forwardEntry {
	for len(batch) < forwardBatchSize {
		select {
		case entry, ok := <-nf.ch:
			if !ok {
				return batch
			}
			batch = append(batch, entry)
		default:
			return batch
		}
	}
	return batch
}

// sendBatchWithBackoff wraps sendBatch with backoff tracking.
func (rf *RecordForwarder) sendBatchWithBackoff(nodeID string, nf *nodeForwarder, entries []forwardEntry) {
	if rf.sendBatch(nodeID, nf, entries) {
		rf.sent.Add(int64(len(entries)))
		// Success — reset backoff and log recovery if we were failing.
		if nf.failures > 0 {
			rf.logger.Info("forward: connection restored",
				"node", nodeID, "after_failures", nf.failures)
		} else if nf.failures == 0 && nf.backoff == 0 && !nf.everSent {
			rf.logger.Info("forward: first batch sent",
				"node", nodeID, "records", len(entries))
			nf.everSent = true
		}
		nf.failures = 0
		nf.backoff = 0
		return
	}

	// Failure — bump backoff. The batch is dropped.
	if !rf.stopping() {
		rf.logger.Warn("forward: batch dropped",
			"node", nodeID, "records", len(entries),
			"failures", nf.failures+1)
	}
	nf.failures++
	if nf.failures == 1 {
		nf.backoff = backoffMin
	} else {
		nf.backoff = min(nf.backoff*2, backoffMax)
	}
}

// sendBatch groups entries by vault and sends one ForwardRecords RPC per vault.
// Returns true if all RPCs succeeded, false on any failure.
func (rf *RecordForwarder) sendBatch(nodeID string, nf *nodeForwarder, entries []forwardEntry) bool {
	// Group by vault ID.
	byVault := make(map[uuid.UUID][]*gastrologv1.ExportRecord)
	for _, e := range entries {
		rec := &gastrologv1.ExportRecord{
			Raw:        e.record.Raw,
			Attrs:      e.record.Attrs,
			IngestSeq:  e.record.EventID.IngestSeq,
			IngesterId: e.record.EventID.IngesterID[:],
		}
		if !e.record.SourceTS.IsZero() {
			rec.SourceTs = timestamppb.New(e.record.SourceTS)
		}
		if !e.record.IngestTS.IsZero() {
			rec.IngestTs = timestamppb.New(e.record.IngestTS)
		}
		byVault[e.vaultID] = append(byVault[e.vaultID], rec)
	}

	conn, err := rf.peers.Conn(nodeID)
	if err != nil {
		rf.logFailure(nodeID, nf, "dial failed", err)
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for vaultID, records := range byVault {
		req := &gastrologv1.ForwardRecordsRequest{
			VaultId: vaultID.String(),
			Records: records,
		}
		resp := &gastrologv1.ForwardRecordsResponse{}
		err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardRecords", req, resp)
		if err != nil {
			rf.logFailure(nodeID, nf, "RPC failed", err)
			rf.peers.Invalidate(nodeID)
			return false
		}
		ack := resp.GetRecordsWritten()
		if ack != int64(len(records)) {
			rf.logger.Warn("forward: partial write",
				"node", nodeID, "vault", vaultID,
				"sent", len(records), "written", ack)
		} else {
			rf.logger.Debug("forwarded records",
				"node", nodeID, "vault", vaultID,
				"sent", len(records), "written", ack)
		}
	}
	return true
}

// logFailure logs forwarding failures at INFO level. Every failure is logged
// because silent drops during backoff are extremely hard to diagnose.
//
// nf is passed directly by the caller (always within the flushLoop goroutine)
// to avoid a racy map lookup → read of nf.failures across goroutines.
func (rf *RecordForwarder) logFailure(nodeID string, nf *nodeForwarder, msg string, err error) {
	if rf.stopping() {
		return // suppress noise during shutdown
	}
	rf.logger.Info("forward: "+msg,
		"node", nodeID, "error", err,
		"consecutive_failures", nf.failures,
		"backoff", nf.backoff)
}

// stopping returns true if the forwarder is shutting down.
func (rf *RecordForwarder) stopping() bool {
	select {
	case <-rf.stop:
		return true
	default:
		return false
	}
}

// Sent returns the total number of records successfully sent via forwarding.
func (rf *RecordForwarder) Sent() int64 {
	return rf.sent.Load()
}

// Close shuts down all per-node forwarders. Connection cleanup is handled
// by PeerConns.
func (rf *RecordForwarder) Close() error {
	rf.mu.Lock()
	rf.closed = true
	close(rf.stop) // unblock any backoff sleeps
	rf.cwCancel()  // stop the channel pressure watcher
	for _, nf := range rf.nodes {
		close(nf.ch)
	}
	rf.mu.Unlock()

	rf.wg.Wait()
	return nil
}
