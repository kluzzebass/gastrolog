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
	handle PeerConnHandle
	cancel context.CancelFunc
	closed bool
}

// ChunkReplicator manages ordered replication streams from a vault leader
// to its followers. All operations for a given (vaultID, followerNodeID)
// are serialized on a single bidirectional gRPC stream.
type ChunkReplicator struct {
	peers  *PeerConnManager
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
func NewChunkReplicator(peers *PeerConnManager, logger *slog.Logger) *ChunkReplicator {
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
	ctx, cancel := context.WithCancel(context.Background())
	h, stream, err := tr.peers.OpenServiceStream(ctx, nodeID, PurposeReplicate,
		chunkReplicationStreamDesc,
		"/gastrolog.v1.ClusterService/ChunkReplication",
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open vault replication stream to %s: %w", nodeID, err)
	}

	ts = &vaultStream{stream: stream, handle: h, cancel: cancel}

	tr.mu.Lock()
	// Another goroutine may have opened one while we were dialing.
	if existing := tr.streams[key]; existing != nil && !existing.closed {
		tr.mu.Unlock()
		cancel()
		h.Release()
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
		// Tear down the stream so the next ImportSealedChunk opens a fresh
		// handler on the follower. Leaving the stream up after a rejected
		// ack left pending import state on the receiver while the sender
		// moved on to the next chunk on the same stream — ImportBegin
		// preempted the wedged import and spammed WARN on every catchup
		// frame. See gastrolog-2o9e9.
		tr.closeStream(vaultID, nodeID)
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
		if ts.handle != nil {
			ts.handle.Release()
			ts.handle = nil
		}
		delete(tr.streams, key)
	}
	tr.mu.Unlock()
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
	if err := tr.peers.InvokeService(ctx, leaderNodeID, PurposeReplCatchup,
		"/gastrolog.v1.ClusterService/RequestReplicaCatchup", req, resp); err != nil {
		return 0, err
	}
	return resp.GetScheduled(), nil
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
