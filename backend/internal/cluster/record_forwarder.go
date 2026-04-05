package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// forwardChanCap is the per-node channel capacity for ingestion forwarding.
	// When full, new records are dropped (best-effort delivery).
	forwardChanCap = 16384

	// streamBurstSize is the max records drained per burst on the stream.
	// After one blocking read, up to streamBurstSize-1 more are drained
	// non-blocking. gRPC's HTTP/2 transport coalesces the SendMsg calls.
	streamBurstSize = 100
)

// forwardEntry is a single record queued for forwarding.
type forwardEntry struct {
	vaultID uuid.UUID
	tierID  uuid.UUID      // non-zero = tier-targeted (secondary append)
	chunkID chunk.ChunkID  // primary's active chunk ID for secondary ID sync
	record  chunk.Record
}

// nodeForwarder manages a per-node channel and stream goroutine.
type nodeForwarder struct {
	ch   chan forwardEntry // ingestion forwarding (best-effort, drops on full)
	done chan struct{}

	// Backoff state — only accessed from the streamLoop goroutine.
	failures int
	backoff  time.Duration
}

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second
)

// streamForwardRecordsDesc is the gRPC stream descriptor for the
// client-streaming StreamForwardRecords RPC.
var streamForwardRecordsDesc = &grpc.StreamDesc{
	StreamName:    "StreamForwardRecords",
	ClientStreams: true,
}

// RecordForwarder implements orchestrator.RecordForwarder with per-node
// buffered channels and client-streaming RPCs.
type RecordForwarder struct {
	peers   *PeerConns
	logger  *slog.Logger
	cw      *chanwatch.Watcher
	alerts  *alert.Collector // optional alert collector
	chanCap int              // per-node channel capacity; 0 = default (16384)

	sent atomic.Int64 // records successfully sent

	mu     sync.Mutex
	nodes  map[string]*nodeForwarder // keyed by node ID
	wg     sync.WaitGroup
	closed bool
	stop   chan struct{} // closed on Close() to signal goroutines to drain and exit

	stopCtx    context.Context    // scoped to forwarder lifetime; used for gRPC streams
	stopCancel context.CancelFunc // cancels stopCtx; called after goroutines exit
	cwCancel   context.CancelFunc // cancels the chanwatch goroutine
}

// NewRecordForwarder creates a RecordForwarder using the shared PeerConns pool.
// chanCap sets the per-node channel capacity; 0 uses the default (16384).
func NewRecordForwarder(peers *PeerConns, logger *slog.Logger, alerts *alert.Collector, chanCap int) *RecordForwarder {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	cwCtx, cwCancel := context.WithCancel(context.Background())
	if chanCap <= 0 {
		chanCap = forwardChanCap
	}
	rf := &RecordForwarder{
		peers:      peers,
		logger:     logger,
		alerts:     alerts,
		chanCap:    chanCap,
		cw:         chanwatch.New(logger, 1*time.Second),
		nodes:      make(map[string]*nodeForwarder),
		stop:       make(chan struct{}),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		cwCancel:   cwCancel,
	}
	rf.cw.SetAlerts(alerts)
	// Run the channel pressure watcher until Close().
	rf.wg.Go(func() {
		rf.cw.Run(cwCtx)
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
				if rf.alerts != nil {
					rf.alerts.Set(
						"forwarder-overflow:"+nodeID,
						alert.Warning, "forwarder",
						fmt.Sprintf("Forward buffer full for node %s, dropping records", nodeID[:8]),
					)
				}
			}
		}
	}
	return nil
}

// ForwardToTier enqueues records for a specific tier on a secondary.
// The tier_id + chunk_id distinguish these from regular vault appends.
// Fire-and-forget — drops silently on full buffer.
func (rf *RecordForwarder) ForwardToTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, records []chunk.Record) error {
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
		case nf.ch <- forwardEntry{vaultID: vaultID, tierID: tierID, chunkID: chunkID, record: rec}:
		default:
			// Drop silently — the sealed-chunk replication will deliver
			// the canonical version. Buffer drops don't lose data.
		}
	}
	return nil
}

// startNode creates and starts a per-node forwarder goroutine.
// Must be called with rf.mu held.
func (rf *RecordForwarder) startNode(nodeID string) *nodeForwarder {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, rf.chanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf
	rf.cw.Watch("forward:"+nodeID, func() (int, int) {
		return len(nf.ch), cap(nf.ch)
	}, 0.9)
	rf.wg.Add(1)
	go rf.streamLoop(nodeID, nf)
	return nf
}

// streamLoop maintains a persistent client stream to a remote node.
// On stream error, it reconnects with exponential backoff.
func (rf *RecordForwarder) streamLoop(nodeID string, nf *nodeForwarder) {
	defer rf.wg.Done()
	defer close(nf.done)

	for {
		if nf.backoff > 0 {
			// Wait for backoff or shutdown.
			select {
			case <-time.After(nf.backoff):
			case <-rf.stop:
				return
			}
		}

		stream, err := rf.openStream(nodeID)
		if err != nil {
			if rf.stopping() {
				return
			}
			rf.bumpBackoff(nodeID, nf, err)
			continue
		}

		// Drain channel onto stream until error or shutdown.
		closed := rf.drainToStream(nodeID, nf, stream)
		if closed {
			// Channel closed — shutdown. Try to close the stream gracefully.
			_ = stream.CloseSend()
			return
		}
		// Stream error — reconnect.
	}
}

// openStream creates a new client-streaming RPC to the remote node.
func (rf *RecordForwarder) openStream(nodeID string) (grpc.ClientStream, error) {
	conn, err := rf.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	stream, err := conn.NewStream(
		rf.stopCtx,
		streamForwardRecordsDesc,
		"/gastrolog.v1.ClusterService/StreamForwardRecords",
	)
	if err != nil {
		rf.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("open stream: %w", err)
	}
	return stream, nil
}

// drainToStream reads from the channel and sends records on the stream in bursts.
// Returns true if shutdown was requested, false on stream error.
func (rf *RecordForwarder) drainToStream(nodeID string, nf *nodeForwarder, stream grpc.ClientStream) bool {
	for {
		var first forwardEntry
		select {
		case first = <-nf.ch:
		case <-rf.stop:
			// Best-effort flush of remaining buffered entries before exiting.
			rf.flushRemaining(nodeID, nf, stream)
			return true
		}

		// Non-blocking drain for up to streamBurstSize-1 more.
		burst := make([]forwardEntry, 1, streamBurstSize)
		burst[0] = first
		for len(burst) < streamBurstSize {
			select {
			case entry := <-nf.ch:
				burst = append(burst, entry)
			default:
				goto send
			}
		}

	send:
		if err := rf.sendBurst(nodeID, nf, stream, burst); err != nil {
			return false
		}
	}
}

// flushRemaining does a best-effort non-blocking drain of any records
// remaining in the channel and sends them on the stream. Errors are
// ignored — we're shutting down and forwarding is best-effort.
func (rf *RecordForwarder) flushRemaining(nodeID string, nf *nodeForwarder, stream grpc.ClientStream) {
	var remaining []forwardEntry
	for {
		select {
		case entry := <-nf.ch:
			remaining = append(remaining, entry)
		default:
			if len(remaining) > 0 {
				_ = rf.sendBurst(nodeID, nf, stream, remaining)
			}
			return
		}
	}
}

// forwardGroupKey groups entries by vault for batching.
type forwardGroupKey struct {
	vaultID uuid.UUID
	tierID  uuid.UUID
	chunkID chunk.ChunkID
}

// sendBurst groups entries by vault and sends one ForwardRecordsRequest per
// vault on the stream. Returns nil on success.
func (rf *RecordForwarder) sendBurst(nodeID string, nf *nodeForwarder, stream grpc.ClientStream, entries []forwardEntry) error {
	// Group by vault, preserving FIFO order within each group.
	type groupData struct {
		key     forwardGroupKey
		records []*gastrologv1.ExportRecord
	}
	var groups []groupData
	groupIdx := make(map[forwardGroupKey]int, 2)
	for _, e := range entries {
		k := forwardGroupKey{vaultID: e.vaultID, tierID: e.tierID, chunkID: e.chunkID}
		idx, ok := groupIdx[k]
		if !ok {
			idx = len(groups)
			groupIdx[k] = idx
			groups = append(groups, groupData{key: k})
		}
		groups[idx].records = append(groups[idx].records, forwardEntryToProto(e))
	}

	for _, g := range groups {
		msg := &gastrologv1.ForwardRecordsRequest{
			VaultId: g.key.vaultID.String(),
			Records: g.records,
		}
		if g.key.tierID != (uuid.UUID{}) {
			msg.TierId = g.key.tierID.String()
		}
		if g.key.chunkID != (chunk.ChunkID{}) {
			msg.ChunkId = g.key.chunkID.String()
		}
		if err := stream.SendMsg(msg); err != nil {
			if !rf.stopping() {
				rf.bumpBackoff(nodeID, nf, err)
				rf.peers.Invalidate(nodeID)
			}
			return err
		}
	}

	rf.sent.Add(int64(len(entries)))

	// Reset backoff on success.
	if nf.failures > 0 {
		rf.logger.Info("forward: stream restored",
			"node", nodeID, "after_failures", nf.failures)
		if rf.alerts != nil {
			rf.alerts.Clear("forwarder-backoff:" + nodeID)
		}
		nf.failures = 0
		nf.backoff = 0
	}
	if rf.alerts != nil {
		rf.alerts.Clear("forwarder-overflow:" + nodeID)
	}
	return nil
}

// forwardEntryToProto converts a forwardEntry to a proto ExportRecord
// with vault_id and EventID fields populated.
func forwardEntryToProto(e forwardEntry) *gastrologv1.ExportRecord {
	rec := &gastrologv1.ExportRecord{
		VaultId:    e.vaultID.String(),
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
	return rec
}

// bumpBackoff increases the backoff after a failure.
func (rf *RecordForwarder) bumpBackoff(nodeID string, nf *nodeForwarder, err error) {
	nf.failures++
	if nf.failures == 1 {
		nf.backoff = backoffMin
	} else {
		nf.backoff = min(nf.backoff*2, backoffMax)
	}
	if !rf.stopping() {
		rf.logger.Info("forward: stream error",
			"node", nodeID, "error", err,
			"consecutive_failures", nf.failures,
			"backoff", nf.backoff)
		if rf.alerts != nil {
			rf.alerts.Set(
				"forwarder-backoff:"+nodeID,
				alert.Warning, "forwarder",
				fmt.Sprintf("Forward to node %s failing (%d consecutive), backing off %s", nodeID[:8], nf.failures, nf.backoff),
			)
		}
	}
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
//
// Shutdown order:
//  1. Reject new Forward calls (closed = true)
//  2. Signal goroutines to drain remaining records and exit (close stop)
//  3. Stop the chanwatch goroutine (cwCancel)
//  4. Wait for all goroutines to finish — streams stay alive for flush
//  5. Cancel stream context (stopCancel) — no goroutines using it anymore
func (rf *RecordForwarder) Close() error {
	rf.mu.Lock()
	rf.closed = true
	close(rf.stop)
	rf.cwCancel()
	rf.mu.Unlock()

	rf.wg.Wait()
	rf.stopCancel()
	return nil
}
