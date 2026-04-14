package cluster

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/grpc"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
)

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
		tr.peers.Invalidate(nodeID)
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

// send sends a command on the stream and waits for the ack. On stream
// failure, marks the stream as closed so the next call reopens it.
func (tr *TierReplicator) send(_, tierID glid.GLID, nodeID string, cmd *gastrologv1.TierReplicationCommand) error {
	ts, err := tr.getOrOpen(tierID, nodeID)
	if err != nil {
		return err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return fmt.Errorf("stream to %s for tier %s is closed", nodeID, tierID)
	}

	if err := ts.stream.SendMsg(cmd); err != nil {
		tr.closeStream(tierID, nodeID)
		return fmt.Errorf("send: %w", err)
	}

	ack := &gastrologv1.TierReplicationAck{}
	if err := ts.stream.RecvMsg(ack); err != nil {
		tr.closeStream(tierID, nodeID)
		return fmt.Errorf("recv ack: %w", err)
	}

	if !ack.Ok {
		return fmt.Errorf("follower rejected command: %s", ack.Error)
	}
	return nil
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
	return tr.send(vaultID, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		Command: &gastrologv1.TierReplicationCommand_Append{
			Append: &gastrologv1.TierReplicationAppend{
				ChunkId: chunkID.String(),
				Records: exports,
			},
		},
	})
}

// SealTier tells a follower to seal its active chunk.
func (tr *TierReplicator) SealTier(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(vaultID, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		Command: &gastrologv1.TierReplicationCommand_Seal{
			Seal: &gastrologv1.TierReplicationSeal{
				ChunkId: chunkID.String(),
			},
		},
	})
}

// ImportSealedChunk sends a canonical sealed chunk to a follower.
func (tr *TierReplicator) ImportSealedChunk(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	exports := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exports[i] = convert.RecordToExport(rec)
	}
	return tr.send(vaultID, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		Command: &gastrologv1.TierReplicationCommand_ImportSealed{
			ImportSealed: &gastrologv1.TierReplicationImport{
				ChunkId: chunkID.String(),
				Records: exports,
			},
		},
	})
}

// DeleteChunk tells a follower to delete a sealed chunk.
func (tr *TierReplicator) DeleteChunk(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	return tr.send(vaultID, tierID, nodeID, &gastrologv1.TierReplicationCommand{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		Command: &gastrologv1.TierReplicationCommand_DeleteChunk{
			DeleteChunk: &gastrologv1.TierReplicationDelete{
				ChunkId: chunkID.String(),
			},
		},
	})
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
