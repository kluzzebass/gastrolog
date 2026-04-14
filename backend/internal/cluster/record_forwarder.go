package cluster

import (
	"gastrolog/internal/glid"
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
	"gastrolog/internal/convert"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
)

const (
	// forwardChanCap is the per-node channel capacity for ingestion forwarding.
	// When full, new records are dropped (best-effort delivery) via Forward,
	// or ForwardSync blocks until space is available.
	forwardChanCap = 16384

	// streamBurstSize is the max records drained per burst on the stream.
	// After one blocking read, up to streamBurstSize-1 more are drained
	// non-blocking. gRPC's HTTP/2 transport coalesces the SendMsg calls.
	streamBurstSize = 100

	// probeNamePrefix is the chanwatch/pressure-gate probe name prefix
	// for per-node forward channels — full probe IDs look like
	// "forward:<nodeID>".
	probeNamePrefix = "forward:"
)

// forwardEntry is a single record queued for forwarding to a remote node's
// vault. Used exclusively for cross-node vault routing — tier-targeted
// follower replication now goes through TierReplicator (see gastrolog-5c6fp).
type forwardEntry struct {
	vaultID glid.GLID
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
	peers  *PeerConns
	logger *slog.Logger
	cw     *chanwatch.Watcher
	alerts *alert.Collector // optional alert collector

	sent atomic.Int64 // records successfully sent

	mu     sync.Mutex
	nodes  map[string]*nodeForwarder // keyed by node ID
	wg     sync.WaitGroup
	closed bool
	stop   chan struct{} // closed on Close() to signal goroutines to drain and exit

	stopCtx    context.Context    // scoped to forwarder lifetime; used for gRPC streams
	stopCancel context.CancelFunc // cancels stopCtx; called after goroutines exit
	cwCancel   context.CancelFunc // cancels the chanwatch goroutine

	// pressureGate (optional) is informed of every per-node forward
	// channel so pipeline-wide pressure classification reflects the
	// cross-node forward path, not just local ingest/digest buffers.
	// Registered via RegisterPressureGate before or after nodes exist;
	// existing nodes have their probes added immediately, and future
	// startNode calls register theirs as they are created. See
	// gastrolog-27zvt.
	pressureGate *chanwatch.PressureGate

	// onNodeUnreachable is called on the first consecutive failure for a
	// node. The orchestrator uses this to trigger immediate placement
	// reconciliation so the dead node's tiers are reassigned without
	// waiting for the 15-second placement interval.
	onNodeUnreachable func(nodeID string)
}

// NewRecordForwarder creates a RecordForwarder using the shared PeerConns pool.
func NewRecordForwarder(peers *PeerConns, logger *slog.Logger, alerts *alert.Collector) *RecordForwarder {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	cwCtx, cwCancel := context.WithCancel(context.Background())
	rf := &RecordForwarder{
		peers:      peers,
		logger:     logger,
		alerts:     alerts,
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
// This is the fire-and-forget path — callers that require durable
// delivery must use ForwardSync.
func (rf *RecordForwarder) Forward(_ context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
	nf, err := rf.nodeFor(nodeID)
	if err != nil {
		return err
	}

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

// ForwardSync enqueues records for delivery to the given node, blocking
// until each record is accepted by the per-node channel or ctx expires.
// Unlike Forward, this path does NOT drop on a full buffer — the caller
// gets an error via ctx.Err() and can propagate it to the ack channel
// for ack-gated ingesters. See gastrolog-27zvt.
//
// Note that "accepted by the channel" is the durability boundary this
// method guarantees; end-to-end remote ack (that the record actually
// landed in the remote node's chunk store) is out of scope and would
// require a bidirectional stream protocol.
func (rf *RecordForwarder) ForwardSync(ctx context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
	nf, err := rf.nodeFor(nodeID)
	if err != nil {
		return err
	}

	for _, rec := range records {
		select {
		case nf.ch <- forwardEntry{vaultID: vaultID, record: rec}:
		case <-ctx.Done():
			return fmt.Errorf("forward to %s: %w", nodeID, ctx.Err())
		case <-rf.stop:
			return errors.New("forwarder closed")
		}
	}
	return nil
}

// nodeFor returns (or lazily starts) the per-node forwarder for nodeID.
// Extracted from Forward/ForwardSync so both entry points share the same
// "look up, lazy-start, reject if closed" logic.
func (rf *RecordForwarder) nodeFor(nodeID string) (*nodeForwarder, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.closed {
		return nil, errors.New("forwarder closed")
	}
	nf, ok := rf.nodes[nodeID]
	if !ok {
		nf = rf.startNode(nodeID)
	}
	return nf, nil
}

// startNode creates and starts a per-node forwarder goroutine.
// Must be called with rf.mu held.
func (rf *RecordForwarder) startNode(nodeID string) *nodeForwarder {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf
	probe := func() (int, int) {
		return len(nf.ch), cap(nf.ch)
	}
	rf.cw.Watch(probeNamePrefix+nodeID, probe, 0.9)
	if rf.pressureGate != nil {
		rf.pressureGate.AddProbe(probeNamePrefix+nodeID, probe)
	}
	rf.wg.Add(1)
	go rf.streamLoop(nodeID, nf)
	return nf
}

// RegisterPressureGate registers the per-node forward channels as probes
// on the given pressure gate so pipeline-wide backpressure classification
// includes cross-node forwarding. Forward channels added later (as new
// peers are discovered) are registered automatically via startNode. Safe
// to call at most once; subsequent calls are ignored.
func (rf *RecordForwarder) RegisterPressureGate(gate *chanwatch.PressureGate) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.pressureGate != nil {
		return
	}
	rf.pressureGate = gate
	// Also register any nodes that already exist — common if the
	// forwarder was used for fire-and-forget traffic before the gate
	// was wired up at orchestrator start.
	for nodeID, nf := range rf.nodes {
		nfCopy := nf
		gate.AddProbe(probeNamePrefix+nodeID, func() (int, int) {
			return len(nfCopy.ch), cap(nfCopy.ch)
		})
	}
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

// sendBurst groups entries by vault and sends one ForwardRecordsRequest per
// vault on the stream. Returns nil on success.
func (rf *RecordForwarder) sendBurst(nodeID string, nf *nodeForwarder, stream grpc.ClientStream, entries []forwardEntry) error {
	// Group by vault, preserving FIFO order within each group.
	type groupData struct {
		vaultID glid.GLID
		records []*gastrologv1.ExportRecord
	}
	var groups []groupData
	groupIdx := make(map[glid.GLID]int, 2)
	for _, e := range entries {
		idx, ok := groupIdx[e.vaultID]
		if !ok {
			idx = len(groups)
			groupIdx[e.vaultID] = idx
			groups = append(groups, groupData{vaultID: e.vaultID})
		}
		groups[idx].records = append(groups[idx].records, forwardEntryToProto(e))
	}

	for _, g := range groups {
		msg := &gastrologv1.ForwardRecordsRequest{
			VaultId: g.vaultID.String(),
			Records: g.records,
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

// forwardEntryToProto converts a forwardEntry to a proto ExportRecord.
// Delegates to the canonical converter after setting VaultID on the record.
func forwardEntryToProto(e forwardEntry) *gastrologv1.ExportRecord {
	rec := e.record
	rec.VaultID = e.vaultID
	return convert.RecordToExport(rec)
}

// bumpBackoff increases the backoff after a failure.
// SetOnNodeUnreachable registers a callback fired on the first consecutive
// stream failure for a node. Safe to call before or after forwarding starts.
func (rf *RecordForwarder) SetOnNodeUnreachable(fn func(nodeID string)) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.onNodeUnreachable = fn
}

func (rf *RecordForwarder) bumpBackoff(nodeID string, nf *nodeForwarder, err error) {
	nf.failures++
	if nf.failures == 1 {
		nf.backoff = backoffMin
		if rf.onNodeUnreachable != nil {
			go rf.onNodeUnreachable(nodeID)
		}
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

// RedirectNode drains all queued records from fromNodeID's channel and
// re-enqueues them to toNodeID. Called when placement reassigns a tier's
// leader to a different node — records buffered for the dead node get
// sent to the new leader instead of being stuck until the channel fills.
// If toNodeID is empty, records are returned for local processing.
func (rf *RecordForwarder) RedirectNode(fromNodeID, toNodeID string) {
	rf.mu.Lock()
	nf, ok := rf.nodes[fromNodeID]
	rf.mu.Unlock()
	if !ok || nf == nil {
		return
	}

	// Drain non-blocking — take whatever is in the channel right now.
	var drained []forwardEntry
	for {
		select {
		case e := <-nf.ch:
			drained = append(drained, e)
		default:
			goto done
		}
	}
done:
	if len(drained) == 0 {
		return
	}

	// Re-enqueue to the new target node (or drop if toNodeID is empty,
	// meaning the vault moved to this node — local append already happened
	// via the new filter set).
	if toNodeID != "" {
		for _, e := range drained {
			_ = rf.Forward(context.Background(), toNodeID, e.vaultID, []chunk.Record{e.record})
		}
	}

	rf.logger.Info("redirected queued records",
		"from", fromNodeID, "to", toNodeID, "count", len(drained))
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
