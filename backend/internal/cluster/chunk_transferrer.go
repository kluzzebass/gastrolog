package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"

	"google.golang.org/grpc"
)

// Per-call timeouts protect every cluster forwarder against wedges where the
// remote node's user-space process is paused (e.g. SIGSTOP, GC stall, debugger
// attach) but its kernel TCP stack still acks packets. Without these, gRPC's
// stream.RecvMsg blocks forever waiting for an application-level response that
// will never come, holding pipeline channel slots indefinitely and silently
// wedging the entire data plane. With these in place, the wedge is bounded
// and surfaces as a logged error within seconds.
const (
	unaryCallTimeout  = 5 * time.Second
	streamCallTimeout = 15 * time.Second
)

// ErrSourceRead marks errors that originated from reading the source
// chunk's record iterator (cursor.Next) rather than from the network /
// destination peer.
var ErrSourceRead = errors.New("source chunk read failed")

// ChunkTransferrer sends chunk records to a remote node for cross-node chunk
// migration. Uses client-streaming gRPC so records flow one-at-a-time from
// cursor through the network to disk on the destination — at most one
// ExportRecord + one chunk.Record live in memory at a time.
type ChunkTransferrer struct {
	peers *PeerConnManager
}

// NewChunkTransferrer creates a ChunkTransferrer using the peer connection manager.
func NewChunkTransferrer(peers *PeerConnManager) *ChunkTransferrer {
	return &ChunkTransferrer{peers: peers}
}

// TransferRecords sends records to the given node's vault via a client-streaming
// ForwardImportRecords RPC.
func (ct *ChunkTransferrer) TransferRecords(ctx context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error {
	ctx, cancel := context.WithTimeout(ctx, streamCallTimeout)
	defer cancel()

	h, stream, err := ct.peers.OpenServiceStream(ctx, nodeID, PurposeChunkXfer,
		&grpc.StreamDesc{StreamName: "ForwardImportRecords", ClientStreams: true},
		"/gastrolog.v1.ClusterService/ForwardImportRecords",
	)
	if err != nil {
		return fmt.Errorf("open import stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	vid := vaultID.ToProto()
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
			h.Invalidate(err)
			return fmt.Errorf("send record to %s: %w", nodeID, err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	resp := &gastrologv1.ForwardImportRecordsResponse{}
	if err := stream.RecvMsg(resp); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("receive response from %s: %w", nodeID, err)
	}
	return nil
}

// WaitVaultReady blocks until the target node reports the vault registered
// and accepting records, or ctx expires. The target node holds the request
// open on its vault-ready signal and responds the instant registration
// completes — no client-side polling. ctx (the caller's drain context)
// bounds the wait: on cancellation the RPC is torn down and the target's
// waiter unblocks via the same context. Replaces the former 100ms
// ForwardListChunks poll.
func (ct *ChunkTransferrer) WaitVaultReady(ctx context.Context, nodeID string, vaultID glid.GLID) error {
	req := &gastrologv1.ForwardWaitVaultReadyRequest{VaultId: vaultID.ToProto()}
	resp := &gastrologv1.ForwardWaitVaultReadyResponse{}
	if err := ct.peers.InvokeService(ctx, nodeID, PurposeChunkWait,
		"/gastrolog.v1.ClusterService/ForwardWaitVaultReady", req, resp); err != nil {
		return fmt.Errorf("vault %s not ready on node %s: %w", vaultID, nodeID, err)
	}
	return nil
}
