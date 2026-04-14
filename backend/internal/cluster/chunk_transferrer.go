package cluster

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"

	"google.golang.org/grpc"
)

// Per-call timeouts protect every cluster forwarder against wedges where the
// remote node's user-space process is paused (e.g. SIGSTOP, GC stall, debugger
// attach) but its kernel TCP stack still acks packets. Without these, gRPC's
// stream.RecvMsg blocks forever waiting for an application-level response that
// will never come, holding pipeline channel slots indefinitely and silently
// wedging the entire data plane. With these in place, the wedge is bounded
// and surfaces as a logged error within seconds. See gastrolog-4rp6i.
//
// Tunings:
//   - Unary RPCs (ForwardAppend, …) are small request/response pairs. 5s is
//     generous for round-trip + processing.
//   - Streaming RPCs (TransferRecords, StreamToTier) transfer entire sealed
//     chunks (typically <10MB). 15s allows for slow networks (1MB/s) plus
//     margin without leaving the cluster wedged for too long when a peer
//     becomes unresponsive.
const (
	unaryCallTimeout  = 5 * time.Second
	streamCallTimeout = 15 * time.Second
)

// ErrSourceRead marks errors that originated from reading the source
// chunk's record iterator (cursor.Next) rather than from the network /
// destination peer. Callers that distinguish "transient destination
// failure" from "source chunk corruption" can check `errors.Is(err,
// ErrSourceRead)` to tell them apart — e.g. transition.transitionChunk
// uses this to decide whether to mark the source chunk unreadable.
// See gastrolog-50271.
var ErrSourceRead = errors.New("source chunk read failed")

// ChunkTransferrer sends chunk records to a remote node for cross-node chunk
// migration. Uses client-streaming gRPC so records flow one-at-a-time from
// cursor through the network to disk on the destination — at most one
// ExportRecord + one chunk.Record live in memory at a time.
// Synchronous — the caller blocks until the remote node confirms.
// Follows the SearchForwarder pattern: holds PeerConns, invalidates on error.
type ChunkTransferrer struct {
	peers *PeerConns
}

// NewChunkTransferrer creates a ChunkTransferrer using the shared PeerConns pool.
func NewChunkTransferrer(peers *PeerConns) *ChunkTransferrer {
	return &ChunkTransferrer{peers: peers}
}

// TransferRecords sends records to the given node's vault via a client-streaming
// ForwardImportRecords RPC. Each record is sent as a separate message so the
// entire chunk never materializes in memory.
func (ct *ChunkTransferrer) TransferRecords(ctx context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error {
	ctx, cancel := context.WithTimeout(ctx, streamCallTimeout)
	defer cancel()

	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	streamDesc := &grpc.StreamDesc{
		StreamName:    "ForwardImportRecords",
		ClientStreams: true,
	}
	stream, err := conn.NewStream(ctx, streamDesc, "/gastrolog.v1.ClusterService/ForwardImportRecords")
	if err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("open import stream to %s: %w", nodeID, err)
	}

	vid := vaultID.String()
	for {
		rec, iterErr := next()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			break
		}
		if iterErr != nil {
			return fmt.Errorf("%w: transfer: %w", ErrSourceRead, iterErr)
		}
		msg := &gastrologv1.ImportRecordMessage{
			VaultId: vid,
			Record:  convert.RecordToExport(rec),
		}
		if err := stream.SendMsg(msg); err != nil {
			ct.peers.Invalidate(nodeID)
			return fmt.Errorf("send record to %s: %w", nodeID, err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	resp := &gastrologv1.ForwardRecordsResponse{}
	if err := stream.RecvMsg(resp); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("receive response from %s: %w", nodeID, err)
	}
	return nil
}

// ForwardAppend sends records to a remote node via the unary ForwardRecords
// RPC, which appends them to the destination vault's active chunk (same path
// as live ingestion forwarding). Synchronous — blocks until the remote node
// confirms the append. Used by retention eject for remote delivery.
func (ct *ChunkTransferrer) ForwardAppend(ctx context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
	ctx, cancel := context.WithTimeout(ctx, unaryCallTimeout)
	defer cancel()

	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	exportRecs := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		exportRecs[i] = convert.RecordToExport(rec)
	}

	req := &gastrologv1.ForwardRecordsRequest{
		VaultId: vaultID.String(),
		Records: exportRecs,
	}
	resp := &gastrologv1.ForwardRecordsResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardRecords", req, resp); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("forward append to %s: %w", nodeID, err)
	}
	return nil
}

// StreamToTier opens a single gRPC stream and pipes all records from the
// iterator to a remote tier's active chunk. The stream close is the ack.
// Used for remote tier transitions — destination handles its own chunking.
func (ct *ChunkTransferrer) StreamToTier(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, next chunk.RecordIterator) error {
	ctx, cancel := context.WithTimeout(ctx, streamCallTimeout)
	defer cancel()

	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	streamDesc := &grpc.StreamDesc{
		StreamName:    "ForwardImportRecords",
		ClientStreams: true,
	}
	stream, err := conn.NewStream(ctx, streamDesc, "/gastrolog.v1.ClusterService/ForwardImportRecords")
	if err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("open transition stream to %s: %w", nodeID, err)
	}

	streamClosed := false
	defer func() {
		if !streamClosed {
			_ = stream.CloseSend()
		}
	}()

	vid := vaultID.String()
	tid := tierID.String()
	for {
		rec, iterErr := next()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			break
		}
		if iterErr != nil {
			return fmt.Errorf("%w: transition: %w", ErrSourceRead, iterErr)
		}
		msg := &gastrologv1.ImportRecordMessage{
			VaultId: vid,
			TierId:  tid,
			Record:  convert.RecordToExport(rec),
		}
		if err := stream.SendMsg(msg); err != nil {
			ct.peers.Invalidate(nodeID)
			return fmt.Errorf("send record to %s: %w", nodeID, err)
		}
	}

	streamClosed = true
	if err := stream.CloseSend(); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	resp := &gastrologv1.ForwardRecordsResponse{}
	if err := stream.RecvMsg(resp); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("receive response from %s: %w", nodeID, err)
	}
	return nil
}

// WaitVaultReady polls the target node until the vault is registered and
// accepting records, or ctx expires. Uses ForwardListChunks as a lightweight
// existence probe — it returns an error if the vault doesn't exist.
func (ct *ChunkTransferrer) WaitVaultReady(ctx context.Context, nodeID string, vaultID glid.GLID) error {
	const pollInterval = 100 * time.Millisecond

	vid := vaultID.String()
	for {
		conn, err := ct.peers.Conn(nodeID)
		if err == nil {
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			req := &gastrologv1.ForwardListChunksRequest{VaultId: vid}
			resp := &gastrologv1.ForwardListChunksResponse{}
			err = conn.Invoke(probeCtx, "/gastrolog.v1.ClusterService/ForwardListChunks", req, resp)
			cancel()
			if err == nil {
				return nil // vault exists on target
			}
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return fmt.Errorf("vault %s not ready on node %s: %w", vaultID, nodeID, ctx.Err())
		}
	}
}

