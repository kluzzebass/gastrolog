package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"

	"google.golang.org/grpc"
)

const segmentPullerPurpose = "segment-puller"

// SegmentPuller streams completed segments from a peer node (the segment
// origin or another holder) over the internode ClusterService, using the shared
// PeerConnManager service lane and cluster mTLS — the same pattern as
// ManagedFileTransferrer.
// It is the production transport behind collection.PullClient (Rubicon C).
type SegmentPuller struct {
	peers *PeerConnManager
}

// NewSegmentPuller creates a SegmentPuller over the shared PeerConnManager.
func NewSegmentPuller(peers *PeerConnManager) *SegmentPuller {
	return &SegmentPuller{peers: peers}
}

// Pull streams the bytes of a completed segment from nodeID into dest. The
// caller (Collection) writes to a pre-head temp file and verifies the checksum
// against the vault-ctl registry before promoting to head/, so Pull itself does
// no integrity check — a mid-stream failure surfaces as an error and leaves no
// promoted segment.
func (sp *SegmentPuller) Pull(ctx context.Context, nodeID string, vaultID, segmentID glid.GLID, dest io.Writer) error {
	h, stream, err := sp.peers.OpenServiceStream(ctx, nodeID, segmentPullerPurpose,
		&grpc.StreamDesc{
			StreamName:    "PullSegment",
			ServerStreams: true,
		},
		"/gastrolog.v1.ClusterService/PullSegment",
	)
	if err != nil {
		return fmt.Errorf("open pull stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	req := &gastrologv1.PullSegmentRequest{
		VaultId:   vaultID[:],
		SegmentId: segmentID[:],
	}
	if err := stream.SendMsg(req); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("send pull request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	for {
		chunk := &gastrologv1.PullSegmentChunk{}
		if err := stream.RecvMsg(chunk); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			h.Invalidate(err)
			return fmt.Errorf("receive segment chunk from %s: %w", nodeID, err)
		}
		if len(chunk.GetData()) > 0 {
			if _, err := dest.Write(chunk.GetData()); err != nil {
				return fmt.Errorf("write segment chunk: %w", err)
			}
		}
	}
	return nil
}
