package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/google/uuid"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
}

// RecordForwarder implements orchestrator.RecordForwarder with per-node
// buffered channels and batched unary RPCs. It follows the Broadcaster
// pattern for connection management.
type RecordForwarder struct {
	raft       *hraft.Raft
	clusterTLS *ClusterTLS
	nodeID     string
	logger     *slog.Logger

	mu    sync.Mutex
	nodes map[string]*nodeForwarder // keyed by node ID
	conns map[string]*grpc.ClientConn
	wg    sync.WaitGroup
	closed bool
}

// NewRecordForwarder creates a RecordForwarder that resolves peer addresses
// from the Raft configuration.
func NewRecordForwarder(r *hraft.Raft, clusterTLS *ClusterTLS, nodeID string, logger *slog.Logger) *RecordForwarder {
	return &RecordForwarder{
		raft:       r,
		clusterTLS: clusterTLS,
		nodeID:     nodeID,
		logger:     logger,
		nodes:      make(map[string]*nodeForwarder),
		conns:      make(map[string]*grpc.ClientConn),
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
			rf.logger.Warn("forward buffer full, dropping record",
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
		// Wait for first entry or channel close.
		select {
		case entry, ok := <-nf.ch:
			if !ok {
				// Channel closed — flush remaining and exit.
				if len(batch) > 0 {
					rf.sendBatch(nodeID, batch)
				}
				return
			}
			batch = append(batch, entry)
		case <-timer.C:
			if len(batch) > 0 {
				rf.sendBatch(nodeID, batch)
				batch = batch[:0]
			}
			timer.Reset(forwardFlushInterval)
			continue
		}

		// Drain up to batch size.
	drain:
		for len(batch) < forwardBatchSize {
			select {
			case entry, ok := <-nf.ch:
				if !ok {
					rf.sendBatch(nodeID, batch)
					return
				}
				batch = append(batch, entry)
			default:
				break drain
			}
		}

		if len(batch) >= forwardBatchSize {
			rf.sendBatch(nodeID, batch)
			batch = batch[:0]
			timer.Reset(forwardFlushInterval)
		}
	}
}

// sendBatch groups entries by vault and sends one ForwardRecords RPC per vault.
func (rf *RecordForwarder) sendBatch(nodeID string, entries []forwardEntry) {
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

	conn, err := rf.nodeConn(nodeID)
	if err != nil {
		rf.logger.Warn("forward: dial failed", "node", nodeID, "error", err)
		return
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
			rf.logger.Warn("forward: RPC failed",
				"node", nodeID, "vault", vaultID,
				"records", len(records), "error", err)
			// Invalidate connection on error so next batch redials.
			rf.invalidateConn(nodeID)
			return
		}
		rf.logger.Debug("forwarded records",
			"node", nodeID, "vault", vaultID,
			"sent", len(records), "written", resp.GetRecordsWritten())
	}
}

// nodeConn returns a cached or newly dialed gRPC connection for the given node.
func (rf *RecordForwarder) nodeConn(nodeID string) (*grpc.ClientConn, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if conn, ok := rf.conns[nodeID]; ok {
		return conn, nil
	}

	addr, err := rf.resolveAddr(nodeID)
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if rf.clusterTLS != nil && rf.clusterTLS.State() != nil {
		creds = rf.clusterTLS.TransportCredentials()
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("dial node %s at %s: %w", nodeID, addr, err)
	}
	rf.conns[nodeID] = conn
	return conn, nil
}

// resolveAddr looks up the node's address from the Raft configuration.
func (rf *RecordForwarder) resolveAddr(nodeID string) (string, error) {
	future := rf.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return "", fmt.Errorf("get raft config: %w", err)
	}
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) == nodeID {
			return string(srv.Address), nil
		}
	}
	return "", fmt.Errorf("node %s not found in raft config", nodeID)
}

// invalidateConn closes and removes the cached connection for a node.
func (rf *RecordForwarder) invalidateConn(nodeID string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if conn, ok := rf.conns[nodeID]; ok {
		_ = conn.Close()
		delete(rf.conns, nodeID)
	}
}

// Close shuts down all per-node forwarders and connections.
func (rf *RecordForwarder) Close() error {
	rf.mu.Lock()
	rf.closed = true
	for _, nf := range rf.nodes {
		close(nf.ch)
	}
	rf.mu.Unlock()

	rf.wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for id, conn := range rf.conns {
		_ = conn.Close()
		delete(rf.conns, id)
	}
	return nil
}
