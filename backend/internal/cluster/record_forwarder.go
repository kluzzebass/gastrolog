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
	// forwardChanCap is the per-node channel capacity. When full, new
	// records are dropped to bound memory and prevent backpressure.
	forwardChanCap = 1000

	// streamBurstSize is the max records drained per burst on the stream.
	// After one blocking read, up to streamBurstSize-1 more are drained
	// non-blocking. gRPC's HTTP/2 transport coalesces the SendMsg calls.
	streamBurstSize = 100
)

// forwardEntry is a single record queued for forwarding.
type forwardEntry struct {
	vaultID uuid.UUID
	record  chunk.Record
}

// nodeForwarder manages a per-node channel and stream goroutine.
type nodeForwarder struct {
	ch   chan forwardEntry
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
	peers  *PeerConns
	logger *slog.Logger
	cw     *chanwatch.Watcher
	alerts *alert.Collector // optional alert collector

	sent atomic.Int64 // records successfully sent

	mu       sync.Mutex
	nodes    map[string]*nodeForwarder // keyed by node ID
	wg       sync.WaitGroup
	closed   bool
	stop     chan struct{}       // closed on Close() to unblock backoff sleeps
	cwCancel context.CancelFunc // cancels the chanwatch goroutine
}

// NewRecordForwarder creates a RecordForwarder using the shared PeerConns pool.
func NewRecordForwarder(peers *PeerConns, logger *slog.Logger, alerts *alert.Collector) *RecordForwarder {
	rf := &RecordForwarder{
		peers:  peers,
		logger: logger,
		alerts: alerts,
		cw:     chanwatch.New(logger, 1*time.Second),
		nodes:  make(map[string]*nodeForwarder),
		stop:   make(chan struct{}),
	}
	rf.cw.SetAlerts(alerts)
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
		context.Background(),
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
// One blocking read, then up to streamBurstSize-1 non-blocking reads, then send all.
// Returns true if the channel was closed (shutdown), false on stream error.
func (rf *RecordForwarder) drainToStream(nodeID string, nf *nodeForwarder, stream grpc.ClientStream) bool {
	for {
		// Blocking read for the first entry.
		var first forwardEntry
		var ok bool
		select {
		case first, ok = <-nf.ch:
			if !ok {
				return true // channel closed — shutdown
			}
		case <-rf.stop:
			return true
		}

		// Non-blocking drain for up to streamBurstSize-1 more.
		burst := make([]forwardEntry, 1, streamBurstSize)
		burst[0] = first
		for len(burst) < streamBurstSize {
			select {
			case entry, chOk := <-nf.ch:
				if !chOk {
					// Channel closed mid-burst — send what we have, then exit.
					if err := rf.sendBurst(nodeID, nf, stream, burst); err != nil {
						return true
					}
					return true
				}
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

// sendBurst groups entries by vault and sends one ForwardRecordsRequest per
// vault on the stream. Returns nil on success.
func (rf *RecordForwarder) sendBurst(nodeID string, nf *nodeForwarder, stream grpc.ClientStream, entries []forwardEntry) error {
	// Group by vault.
	byVault := make(map[uuid.UUID][]*gastrologv1.ExportRecord, 2)
	for _, e := range entries {
		byVault[e.vaultID] = append(byVault[e.vaultID], forwardEntryToProto(e))
	}

	for vaultID, records := range byVault {
		msg := &gastrologv1.ForwardRecordsRequest{
			VaultId: vaultID.String(),
			Records: records,
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
