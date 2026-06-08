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

// SegmentPuller streams completed segments from a peer node (the segment
// origin or another holder) over the internode ClusterService, using the shared
// PeerConns pool and cluster mTLS — the same pattern as ManagedFileTransferrer.
// It is the production transport behind collection.PullClient (Rubicon C).
type SegmentPuller struct {
	peers *PeerConns
}

// NewSegmentPuller creates a SegmentPuller over the shared PeerConns pool.
func NewSegmentPuller(peers *PeerConns) *SegmentPuller {
	return &SegmentPuller{peers: peers}
}

// Pull streams the bytes of a completed segment from nodeID into dest. The
// caller (Collection) writes to a pre-head temp file and verifies the checksum
// against the vault-ctl registry before promoting to head/, so Pull itself does
// no integrity check — a mid-stream failure surfaces as an error and leaves no
// promoted segment.
func (sp *SegmentPuller) Pull(ctx context.Context, nodeID string, vaultID, segmentID glid.GLID, dest io.Writer) error {
	conn, err := sp.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{
			StreamName:    "PullSegment",
			ServerStreams: true,
		},
		"/gastrolog.v1.ClusterService/PullSegment",
	)
	if err != nil {
		sp.peers.Invalidate(nodeID, err)
		return fmt.Errorf("open pull stream to %s: %w", nodeID, err)
	}

	req := &gastrologv1.PullSegmentRequest{
		VaultId:   vaultID[:],
		SegmentId: segmentID[:],
	}
	if err := stream.SendMsg(req); err != nil {
		sp.peers.Invalidate(nodeID, err)
		return fmt.Errorf("send pull request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		sp.peers.Invalidate(nodeID, err)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	for {
		chunk := &gastrologv1.PullSegmentChunk{}
		if err := stream.RecvMsg(chunk); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			sp.peers.Invalidate(nodeID, err)
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
