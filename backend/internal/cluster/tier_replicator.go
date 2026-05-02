package cluster

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"log/slog"
	"sync"

	"google.golang.org/grpc"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
)

// importBlobChunkSize is how many bytes each ImportBlobBody message
// carries. Matches the GLCB seekable-zstd frame size (256 KiB) so the
// network framing aligns with the on-disk frame boundaries — receivers
// could in principle stream into seekable zstd's writer directly, though
// today they just reassemble bytes. Tunable; not part of the wire spec.
const importBlobChunkSize = 256 << 10

// importBlobStreamDesc describes the client-streaming ImportBlob RPC.
var importBlobStreamDesc = &grpc.StreamDesc{
	StreamName:    "ImportBlob",
	ClientStreams: true,
}

// streamKey identifies a replication stream to a specific follower for a
// specific tier. One stream per key.
type streamKey struct {
	tierID glid.GLID
	nodeID string
}

// tierStream wraps a bidirectional gRPC stream with a mutex to serialize
// sends. The mutex is the ordering guarantee — only one command at a time
// on the wire, and the sender waits for the ack before releasing.
type tierStream struct {
	mu     sync.Mutex
	stream grpc.ClientStream
	cancel context.CancelFunc
	closed bool
}

// TierReplicator manages ordered replication streams from a tier leader to
// its followers. All operations for a given (tierID, followerNodeID) are
// serialized on a single bidirectional gRPC stream.
type TierReplicator struct {
	peers  *PeerConns
	logger *slog.Logger

	mu      sync.Mutex
	streams map[streamKey]*tierStream
}

var tierReplicationStreamDesc = &grpc.StreamDesc{
	StreamName:    "TierReplication",
	ClientStreams: true,
	ServerStreams: true,
}

// NewTierReplicator creates a replicator using the given peer connections.
func NewTierReplicator(peers *PeerConns, logger *slog.Logger) *TierReplicator {
	return &TierReplicator{
		peers:   peers,
		logger:  logger,
		streams: make(map[streamKey]*tierStream),
	}
}

// getOrOpen returns the stream for the given tier+node, opening a new one
// if needed. The caller must NOT hold tr.mu.
func (tr *TierReplicator) getOrOpen(tierID glid.GLID, nodeID string) (*tierStream, error) {
	key := streamKey{tierID: tierID, nodeID: nodeID}

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
	stream, err := conn.NewStream(ctx, tierReplicationStreamDesc,
		"/gastrolog.v1.ClusterService/TierReplication")
	if err != nil {
		cancel()
		tr.peers.Invalidate(nodeID, err)
		return nil, fmt.Errorf("open tier replication stream to %s: %w", nodeID, err)
	}

	ts = &tierStream{stream: stream, cancel: cancel}

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
func (tr *TierReplicator) send(ctx context.Context, tierID glid.GLID, nodeID string, cmd *gastrologv1.TierReplicationCommand) error {
	ts, err := tr.getOrOpen(tierID, nodeID)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return fmt.Errorf("stream to %s for tier %s is closed", nodeID, tierID)
	}

	sendErr := tr.runWithCtx(ctx, func() error { return ts.stream.SendMsg(cmd) })
	if sendErr != nil {
		tr.closeStream(tierID, nodeID)
		return fmt.Errorf("send: %w", sendErr)
	}

	ack := &gastrologv1.TierReplicationAck{}
	recvErr := tr.runWithCtx(ctx, func() error { return ts.stream.RecvMsg(ack) })
	if recvErr != nil {
		tr.closeStream(tierID, nodeID)
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
func (tr *TierReplicator) runWithCtx(ctx context.Context, fn func() error) error {
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
func (tr *TierReplicator) closeStream(tierID glid.GLID, nodeID string) {
	key := streamKey{tierID: tierID, nodeID: nodeID}
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
func (tr *TierReplicator) AppendRecords(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	exports := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exports[i] = convert.RecordToExport(rec)
	}
	return tr.send(ctx, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.ToProto(),
		TierId:  tierID.ToProto(),
		Command: &gastrologv1.TierReplicationCommand_Append{
			Append: &gastrologv1.TierReplicationAppend{
				ChunkId: glid.GLID(chunkID).ToProto(),
				Records: exports,
			},
		},
	})
}

// SealTier tells a follower to seal its active chunk.
func (tr *TierReplicator) SealTier(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(ctx, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.ToProto(),
		TierId:  tierID.ToProto(),
		Command: &gastrologv1.TierReplicationCommand_Seal{
			Seal: &gastrologv1.TierReplicationSeal{
				ChunkId: glid.GLID(chunkID).ToProto(),
			},
		},
	})
}

// ImportBlob streams a sealed `data.glcb` blob to a follower via the
// ImportBlob client-streaming RPC. body provides the bytes; totalSize is
// the expected total in bytes (sent in the header so the follower can
// fail fast on size mismatch). Returns the SHA-256 digest the follower
// computed over the received bytes.
//
// Replaces the per-record ImportSealedChunk path: instead of decoding
// records and re-encoding the GLCB on the follower, the leader's
// already-sealed bytes are copied verbatim. See gastrolog-3o5b4.
func (tr *TierReplicator) ImportBlob(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, totalSize int64, body io.Reader) ([32]byte, error) {
	var zero [32]byte

	conn, err := tr.peers.Conn(nodeID)
	if err != nil {
		return zero, fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := conn.NewStream(streamCtx, importBlobStreamDesc,
		"/gastrolog.v1.ClusterService/ImportBlob")
	if err != nil {
		cancel()
		tr.peers.Invalidate(nodeID, err)
		return zero, fmt.Errorf("open ImportBlob stream to %s: %w", nodeID, err)
	}
	defer cancel()

	// Header first.
	header := &gastrologv1.ImportBlobRequest{
		Message: &gastrologv1.ImportBlobRequest_Header{
			Header: &gastrologv1.ImportBlobHeader{
				VaultId:   vaultID.ToProto(),
				TierId:    tierID.ToProto(),
				ChunkId:   glid.GLID(chunkID).ToProto(),
				TotalSize: totalSize,
			},
		},
	}
	if err := stream.SendMsg(header); err != nil {
		return zero, fmt.Errorf("send header: %w", err)
	}

	// Stream body in fixed-size chunks. Per "memory over throughput", we
	// keep one chunk in memory at a time; the leader's source io.Reader
	// is responsible for its own buffering (typically an *os.File, which
	// reads via the OS page cache).
	buf := make([]byte, importBlobChunkSize)
	for {
		n, readErr := io.ReadFull(body, buf)
		if n > 0 {
			body := &gastrologv1.ImportBlobRequest{
				Message: &gastrologv1.ImportBlobRequest_Body{
					Body: &gastrologv1.ImportBlobBody{Data: buf[:n]},
				},
			}
			if err := stream.SendMsg(body); err != nil {
				return zero, fmt.Errorf("send body: %w", err)
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		return zero, fmt.Errorf("read body: %w", readErr)
	}

	if err := stream.CloseSend(); err != nil {
		return zero, fmt.Errorf("close send: %w", err)
	}

	ack := &gastrologv1.ImportBlobAck{}
	if err := stream.RecvMsg(ack); err != nil {
		return zero, fmt.Errorf("recv ack: %w", err)
	}
	if !ack.Ok {
		return zero, fmt.Errorf("follower rejected blob: %s", ack.Error)
	}
	if len(ack.BlobDigest) != 32 {
		return zero, fmt.Errorf("invalid digest length %d", len(ack.BlobDigest))
	}
	var digest [32]byte
	copy(digest[:], ack.BlobDigest)
	return digest, nil
}

// ImportSealedChunk sends a canonical sealed chunk to a follower.
func (tr *TierReplicator) ImportSealedChunk(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	exports := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exports[i] = convert.RecordToExport(rec)
	}
	return tr.send(ctx, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.ToProto(),
		TierId:  tierID.ToProto(),
		Command: &gastrologv1.TierReplicationCommand_ImportSealed{
			ImportSealed: &gastrologv1.TierReplicationImport{
				ChunkId: glid.GLID(chunkID).ToProto(),
				Records: exports,
			},
		},
	})
}

// DeleteChunk tells a follower to delete a sealed chunk.
func (tr *TierReplicator) DeleteChunk(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(ctx, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.ToProto(),
		TierId:  tierID.ToProto(),
		Command: &gastrologv1.TierReplicationCommand_DeleteChunk{
			DeleteChunk: &gastrologv1.TierReplicationDelete{
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
// Unary RPC (not on the existing TierReplication bidirectional stream
// which is exclusively leader→follower commands): the request is
// follower→leader and small, so a one-shot Invoke is the cleaner
// match.
func (tr *TierReplicator) RequestReplicaCatchup(ctx context.Context, leaderNodeID string, vaultID, tierID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
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
		TierId:          tierID.ToProto(),
		ChunkIds:        rawIDs,
		RequesterNodeId: []byte(requesterNodeID),
	}
	resp := &gastrologv1.RequestReplicaCatchupResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/RequestReplicaCatchup", req, resp); err != nil {
		return 0, err
	}
	return resp.GetScheduled(), nil
}

// CloseStream closes the stream for a specific tier+follower.
func (tr *TierReplicator) CloseStream(tierID glid.GLID, nodeID string) {
	tr.closeStream(tierID, nodeID)
}

// Close closes all open streams.
func (tr *TierReplicator) Close() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, ts := range tr.streams {
		ts.closed = true
		ts.cancel()
	}
	tr.streams = make(map[streamKey]*tierStream)
}
