package cluster

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
)

// Per-frame caps for streaming sealed-chunk imports. Both bound the
// gRPC message size of an ImportRecords frame so a single send never
// approaches the receive cap, regardless of chunk size. The byte budget
// dominates for realistic record sizes; the record-count cap covers the
// pathological case of many tiny records inflating per-record proto
// framing overhead. See gastrolog-4yvhh.
const (
	importRecordsMaxBytes   = 8 * 1024 * 1024
	importRecordsMaxRecords = 4096
)

// streamKey identifies a replication stream to a specific follower for a
// specific vault. One stream per key.
type streamKey struct {
	vaultID glid.GLID
	nodeID string
}

// vaultStream wraps a bidirectional gRPC stream (per vault, per follower)
// with a mutex to serialize sends. The mutex is the ordering guarantee —
// only one command at a time on the wire, and the sender waits for the ack
// before releasing.
type vaultStream struct {
	mu     sync.Mutex
	stream grpc.ClientStream
	cancel context.CancelFunc
	closed bool
}

// ChunkReplicator manages ordered replication streams from a vault leader
// to its followers. All operations for a given (vaultID, followerNodeID)
// are serialized on a single bidirectional gRPC stream.
type ChunkReplicator struct {
	peers  *PeerConns
	logger *slog.Logger

	mu      sync.Mutex
	streams map[streamKey]*vaultStream
}

var chunkReplicationStreamDesc = &grpc.StreamDesc{
	StreamName:    "ChunkReplication",
	ClientStreams: true,
	ServerStreams: true,
}

// NewChunkReplicator creates a replicator using the given peer connections.
func NewChunkReplicator(peers *PeerConns, logger *slog.Logger) *ChunkReplicator {
	return &ChunkReplicator{
		peers:   peers,
		logger:  logger,
		streams: make(map[streamKey]*vaultStream),
	}
}

// getOrOpen returns the stream for the given vault+node, opening a new one
// if needed. The caller must NOT hold tr.mu.
func (tr *ChunkReplicator) getOrOpen(vaultID glid.GLID, nodeID string) (*vaultStream, error) {
	key := streamKey{vaultID: vaultID, nodeID: nodeID}

	tr.mu.Lock()
	ts := tr.streams[key]
	if ts != nil && !ts.closed {
		tr.mu.Unlock()
		return ts, nil
	}
	tr.mu.Unlock()

	// Open a new stream.
	conn, err := tr.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := conn.NewStream(ctx, chunkReplicationStreamDesc,
		"/gastrolog.v1.ClusterService/ChunkReplication")
	if err != nil {
		cancel()
		tr.peers.Invalidate(nodeID, err)
		return nil, fmt.Errorf("open vault replication stream to %s: %w", nodeID, err)
	}

	ts = &vaultStream{stream: stream, cancel: cancel}

	tr.mu.Lock()
	// Another goroutine may have opened one while we were dialing.
	if existing := tr.streams[key]; existing != nil && !existing.closed {
		tr.mu.Unlock()
		cancel()
		return existing, nil
	}
	tr.streams[key] = ts
	tr.mu.Unlock()
	return ts, nil
}

// send sends a command on the stream and waits for the ack. Respects the
// caller's context: if ctx is cancelled while waiting for the ack, send
// closes the stream (so the next call opens a fresh one) and returns the
// ctx error.
//
// The grpc.ClientStream blocking methods (SendMsg, RecvMsg) do NOT
// natively honor a context different from the one used at stream
// creation. We enforce the caller deadline by running the blocking calls
// in a helper goroutine and racing them against ctx.Done().
//
// See gastrolog-5oofa: without this, RecvMsg on a paused peer blocks
// forever, holding ts.mu and cascading into ingest-path stalls.
func (tr *ChunkReplicator) send(ctx context.Context, vaultID glid.GLID, nodeID string, cmd *gastrologv1.ChunkReplicationCommand) error {
	ts, err := tr.getOrOpen(vaultID, nodeID)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return fmt.Errorf("stream to %s for vault %s is closed", nodeID, vaultID)
	}

	sendErr := tr.runWithCtx(ctx, func() error { return ts.stream.SendMsg(cmd) })
	if sendErr != nil {
		tr.closeStream(vaultID, nodeID)
		return fmt.Errorf("send: %w", sendErr)
	}

	ack := &gastrologv1.ChunkReplicationAck{}
	recvErr := tr.runWithCtx(ctx, func() error { return ts.stream.RecvMsg(ack) })
	if recvErr != nil {
		tr.closeStream(vaultID, nodeID)
		return fmt.Errorf("recv ack: %w", recvErr)
	}

	if !ack.Ok {
		return fmt.Errorf("follower rejected command: %s", ack.Error)
	}
	return nil
}

// runWithCtx runs fn in a helper goroutine and returns the first of:
// (a) fn's result, or (b) ctx's error. If ctx fires first, fn continues
// running in the background and its result is discarded — the caller has
// already closed the stream so the stuck fn will eventually error out
// when the stream is cancelled. That cost is bounded; the alternative
// (block forever on a paused peer) is not.
func (tr *ChunkReplicator) runWithCtx(ctx context.Context, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeStream marks a stream as closed and cancels its context.
func (tr *ChunkReplicator) closeStream(vaultID glid.GLID, nodeID string) {
	key := streamKey{vaultID: vaultID, nodeID: nodeID}
	tr.mu.Lock()
	ts := tr.streams[key]
	if ts != nil {
		ts.closed = true
		ts.cancel()
		delete(tr.streams, key)
	}
	tr.mu.Unlock()
}

// AppendRecords forwards records to a follower's active chunk.
func (tr *ChunkReplicator) AppendRecords(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	exports := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exports[i] = convert.RecordToExport(rec)
	}
	return tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_Append{
			Append: &gastrologv1.ChunkReplicationAppend{
				ChunkId: glid.GLID(chunkID).ToProto(),
				Records: exports,
			},
		},
	})
}

// SealVault tells a follower to seal its active chunk for the vault.
func (tr *ChunkReplicator) SealVault(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_Seal{
			Seal: &gastrologv1.ChunkReplicationSeal{
				ChunkId: glid.GLID(chunkID).ToProto(),
			},
		},
	})
}

// ImportSealedChunk streams a canonical sealed chunk to a follower as a
// bounded sequence of frames: Begin → 1..N Records → Commit. The caller
// passes a RecordIterator so the chunk is consumed lazily; nothing
// proportional to chunk size is materialized on the leader's heap.
//
// Per-frame size is capped by importRecordsMaxBytes / importRecordsMaxRecords
// so individual gRPC messages always fit under the receive cap regardless
// of how large the chunk grows. See gastrolog-4yvhh.
func (tr *ChunkReplicator) ImportSealedChunk(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	chunkIDProto := glid.GLID(chunkID).ToProto()

	if err := tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
			ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: chunkIDProto},
		},
	}); err != nil {
		return fmt.Errorf("import begin: %w", err)
	}

	batch := make([]*gastrologv1.ExportRecord, 0, 64)
	batchBytes := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportRecords{
				ImportRecords: &gastrologv1.ChunkReplicationImportRecords{Records: batch},
			},
		})
		// Drop the slice contents before the next batch so per-record
		// allocations don't accumulate across frames; the underlying
		// proto messages are no longer needed once the send returns.
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
		batchBytes = 0
		return err
	}

	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}
		ex := convert.RecordToExport(rec)
		exSize := proto.Size(ex)
		// Flush BEFORE appending if this record would push us past the
		// byte budget, otherwise we'd accumulate one over-budget frame.
		// An empty batch is always allowed to take one oversized record
		// so a single huge record can still make progress.
		if len(batch) > 0 && batchBytes+exSize > importRecordsMaxBytes {
			if err := flush(); err != nil {
				return fmt.Errorf("import records: %w", err)
			}
		}
		batch = append(batch, ex)
		batchBytes += exSize
		if len(batch) >= importRecordsMaxRecords {
			if err := flush(); err != nil {
				return fmt.Errorf("import records: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf("import records (final): %w", err)
	}

	if err := tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_ImportCommit{
			ImportCommit: &gastrologv1.ChunkReplicationImportCommit{ChunkId: chunkIDProto},
		},
	}); err != nil {
		return fmt.Errorf("import commit: %w", err)
	}
	return nil
}

// DeleteChunk tells a follower to delete a sealed chunk.
func (tr *ChunkReplicator) DeleteChunk(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(ctx, vaultID, nodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_DeleteChunk{
			DeleteChunk: &gastrologv1.ChunkReplicationDelete{
				ChunkId: glid.GLID(chunkID).ToProto(),
			},
		},
	})
}

// RequestReplicaCatchup is the follower→leader catchup request. Sent
// by a follower whose lifecycle reconciler has detected sealed chunks
// in the FSM that are missing on its local disk (e.g. after a pause/
// resume window where the leader's seal-time push failed). The
// placement leader's handler fans the actual pushes out asynchronously
// via the existing replicateToFollower machinery, so success here
// means "request accepted, pushes scheduled" — not "delivered". The
// follower will re-request anything still missing on the next sweep
// tick. See gastrolog-2dgvj.
//
// Unary RPC (not on the existing ChunkReplication bidirectional stream
// which is exclusively leader→follower commands): the request is
// follower→leader and small, so a one-shot Invoke is the cleaner
// match.
func (tr *ChunkReplicator) RequestReplicaCatchup(ctx context.Context, leaderNodeID string, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
	conn, err := tr.peers.Conn(leaderNodeID)
	if err != nil {
		return 0, fmt.Errorf("dial leader %s: %w", leaderNodeID, err)
	}
	rawIDs := make([][]byte, len(chunkIDs))
	for i := range chunkIDs {
		rawIDs[i] = chunkIDs[i][:]
	}
	req := &gastrologv1.RequestReplicaCatchupRequest{
		VaultId:         vaultID.ToProto(),
		ChunkIds:        rawIDs,
		RequesterNodeId: []byte(requesterNodeID),
	}
	resp := &gastrologv1.RequestReplicaCatchupResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/RequestReplicaCatchup", req, resp); err != nil {
		return 0, err
	}
	return resp.GetScheduled(), nil
}

// PullRecords is the puller→source pull-by-EventID request. Sent by the
// requesting node to a peer that may hold records the requester needs (for
// reconcile slow path, drain Holding drain, node-return catchup, etc.).
// The source's handler filters its local chunk by the EventID set and
// asynchronously pushes matching records back to the requester via Fill
// frames on the existing per-vault ChunkReplication stream. Returns
// (scheduled, missing) so the requester knows what to expect and which
// EventIDs to seek elsewhere. See gastrolog-4t3y4 and
// docs/pull-records-design.md.
//
// Unary RPC (not on the bidirectional stream) for the same reason as
// RequestReplicaCatchup — request is small, the actual data transfer is
// the asynchronous Fill push.
func (tr *ChunkReplicator) PullRecords(ctx context.Context, sourceNodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, eventIDs []chunk.EventID, requesterNodeID string) (scheduled, missing uint32, err error) {
	conn, err := tr.peers.Conn(sourceNodeID)
	if err != nil {
		return 0, 0, fmt.Errorf("dial source %s: %w", sourceNodeID, err)
	}
	rawIDs := make([][]byte, len(eventIDs))
	for i := range eventIDs {
		buf := eventIDs[i].Bytes()
		rawIDs[i] = buf[:]
	}
	req := &gastrologv1.PullRecordsRequest{
		VaultId:         vaultID.ToProto(),
		ChunkId:         chunkID[:],
		EventIds:        rawIDs,
		RequesterNodeId: []byte(requesterNodeID),
	}
	resp := &gastrologv1.PullRecordsResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/PullRecords", req, resp); err != nil {
		return 0, 0, err
	}
	return resp.GetScheduled(), resp.GetMissing(), nil
}

// SendFillRecords pushes a batch of records to the puller as part of a
// PullRecords-initiated fill sequence. Sent source → puller over the
// existing per-vault chunk-replication stream. lastBatch=true on the
// final frame triggers the puller's CmdAckPull dispatch (wiring lands
// in gastrolog-37k2b-e). See gastrolog-4t3y4.
func (tr *ChunkReplicator) SendFillRecords(ctx context.Context, pullerNodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record, lastBatch bool) error {
	exports := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exports[i] = convert.RecordToExport(rec)
	}
	return tr.send(ctx, vaultID, pullerNodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_FillRecords{
			FillRecords: &gastrologv1.ChunkReplicationFillRecords{
				ChunkId:   glid.GLID(chunkID).ToProto(),
				Records:   exports,
				LastBatch: lastBatch,
			},
		},
	})
}

// SendFillComplete signals end-of-stream for a fill sequence to the puller.
// Used when (a) the source had no matching records to send (records_sent=0,
// no FillRecords frames sent), or (b) the source aborted mid-stream and
// wants to surface the error to the puller's reconcile loop. errMsg should
// be empty on the clean-completion path. See gastrolog-4t3y4.
func (tr *ChunkReplicator) SendFillComplete(ctx context.Context, pullerNodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, recordsSent uint32, errMsg string) error {
	return tr.send(ctx, vaultID, pullerNodeID, &gastrologv1.ChunkReplicationCommand{
		VaultId: vaultID.ToProto(),
		Command: &gastrologv1.ChunkReplicationCommand_FillComplete{
			FillComplete: &gastrologv1.ChunkReplicationFillComplete{
				ChunkId:     glid.GLID(chunkID).ToProto(),
				RecordsSent: recordsSent,
				Error:       errMsg,
			},
		},
	})
}

// CloseStream closes the stream for a specific vault+follower.
func (tr *ChunkReplicator) CloseStream(vaultID glid.GLID, nodeID string) {
	tr.closeStream(vaultID, nodeID)
}

// Close closes all open streams.
func (tr *ChunkReplicator) Close() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, ts := range tr.streams {
		ts.closed = true
		ts.cancel()
	}
	tr.streams = make(map[streamKey]*vaultStream)
}
