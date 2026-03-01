package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// forwardChanCap is the per-node channel capacity. When full, new
	// records are dropped to bound memory and prevent backpressure.
	forwardChanCap = 10_000

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

	mu     sync.Mutex
	nodes  map[string]*nodeForwarder // keyed by node ID
	wg     sync.WaitGroup
	closed bool
}

// NewRecordForwarder creates a RecordForwarder using the shared PeerConns pool.
func NewRecordForwarder(peers *PeerConns, logger *slog.Logger) *RecordForwarder {
	return &RecordForwarder{
		peers:  peers,
		logger: logger,
		nodes:  make(map[string]*nodeForwarder),
	}
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
			rf.logger.Info("forward buffer full, dropping record",
				"node", nodeID, "vault", vaultID)
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
			timer.Reset(nf.backoff)
			select {
			case <-timer.C:
				// Backoff expired — drain channel and attempt send.
				batch = rf.drainChannel(nf, batch)
				if len(batch) > 0 {
					rf.sendBatchWithBackoff(nodeID, nf, batch)
					batch = batch[:0]
				}
				continue
			case entry, ok := <-nf.ch:
				if !ok {
					return
				}
				// Record arrived during backoff — accumulate it but
				// keep waiting for the backoff timer.
				batch = append(batch, entry)
				continue
			}
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
	if rf.sendBatch(nodeID, entries) {
		// Success — reset backoff and log recovery if we were failing.
		if nf.failures > 0 {
			rf.logger.Info("forward: connection restored",
				"node", nodeID, "after_failures", nf.failures)
		}
		nf.failures = 0
		nf.backoff = 0
		return
	}

	// Failure — bump backoff.
	nf.failures++
	if nf.failures == 1 {
		nf.backoff = backoffMin
	} else {
		nf.backoff = min(nf.backoff*2, backoffMax)
	}
}

// sendBatch groups entries by vault and sends one ForwardRecords RPC per vault.
// Returns true if all RPCs succeeded, false on any failure.
func (rf *RecordForwarder) sendBatch(nodeID string, entries []forwardEntry) bool {
	// Group by vault ID.
	byVault := make(map[uuid.UUID][]*gastrologv1.ExportRecord)
	for _, e := range entries {
		rec := &gastrologv1.ExportRecord{
			Raw:   e.record.Raw,
			Attrs: e.record.Attrs,
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
		rf.logFailure(nodeID, "dial failed", err)
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
			rf.logFailure(nodeID, "RPC failed", err)
			rf.peers.Invalidate(nodeID)
			return false
		}
		rf.logger.Debug("forwarded records",
			"node", nodeID, "vault", vaultID,
			"sent", len(records), "written", resp.GetRecordsWritten())
	}
	return true
}

// logFailure logs a forwarding failure at INFO on the first occurrence,
// then at DEBUG for subsequent failures. Peer unavailability is normal
// in a cluster, so this is not a warning.
func (rf *RecordForwarder) logFailure(nodeID string, msg string, err error) {
	rf.mu.Lock()
	nf := rf.nodes[nodeID]
	rf.mu.Unlock()

	if nf == nil || nf.failures == 0 {
		rf.logger.Info("forward: "+msg,
			"node", nodeID, "error", err)
	} else {
		rf.logger.Debug("forward: "+msg,
			"node", nodeID, "error", err, "consecutive_failures", nf.failures)
	}
}

// Close shuts down all per-node forwarders. Connection cleanup is handled
// by PeerConns.
func (rf *RecordForwarder) Close() error {
	rf.mu.Lock()
	rf.closed = true
	for _, nf := range rf.nodes {
		close(nf.ch)
	}
	rf.mu.Unlock()

	rf.wg.Wait()
	return nil
}
