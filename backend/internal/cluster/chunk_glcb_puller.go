package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	"google.golang.org/grpc"
)

// ChunkGLCBPuller streams a sealed pipeline chunk's GLCB from a peer home
// over the internode ClusterService — the replica catch-up transport for
// homes that missed the build window and whose source segments are already
// released. Same service-lane/mTLS pattern as SegmentPuller.
type ChunkGLCBPuller struct {
	peers *PeerConnManager
}

// NewChunkGLCBPuller creates a ChunkGLCBPuller over the shared PeerConnManager.
func NewChunkGLCBPuller(peers *PeerConnManager) *ChunkGLCBPuller {
	return &ChunkGLCBPuller{peers: peers}
}

// Pull streams the GLCB bytes for (vaultID, chunkID) from nodeID into dest.
// The caller writes to a temp file, verifies the assembled GLCB's seal
// metadata against the vault-ctl manifest entry, and renames into place —
// a mid-stream failure surfaces as an error and leaves nothing promoted.
func (p *ChunkGLCBPuller) Pull(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, dest io.Writer) error {
	h, stream, err := p.peers.OpenServiceStream(ctx, nodeID, PurposeSegmentPull,
		&grpc.StreamDesc{
			StreamName:    "PullChunkGLCB",
			ServerStreams: true,
		},
		"/gastrolog.v1.ClusterService/PullChunkGLCB",
	)
	if err != nil {
		return fmt.Errorf("open GLCB pull stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	req := &gastrologv1.PullChunkGLCBRequest{
		VaultId: vaultID[:],
		ChunkId: chunkID[:],
	}
	if err := stream.SendMsg(req); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("send GLCB pull request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	frame := &gastrologv1.PullChunkGLCBChunk{}
	for {
		frame.Reset()
		if err := stream.RecvMsg(frame); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			h.Invalidate(err)
			return fmt.Errorf("receive GLCB bytes from %s: %w", nodeID, err)
		}
		if len(frame.GetData()) > 0 {
			if _, err := dest.Write(frame.GetData()); err != nil {
				return fmt.Errorf("write GLCB bytes: %w", err)
			}
		}
	}
	return nil
}
