package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
)

const managedFileChunkSize = 64 * 1024 // 64 KB per streamed chunk

// ManagedFileTransferrer handles cross-node distribution of managed files.
// Uses the shared PeerConns pool, following the same pattern as
// ChunkTransferrer and SearchForwarder.
type ManagedFileTransferrer struct {
	peers *PeerConns
}

// NewManagedFileTransferrer creates a ManagedFileTransferrer using the shared PeerConns pool.
func NewManagedFileTransferrer(peers *PeerConns) *ManagedFileTransferrer {
	return &ManagedFileTransferrer{peers: peers}
}

// PullFile downloads a managed file from a peer node and writes it to destDir.
// The file is streamed chunk-by-chunk (never fully buffered) and verified
// against the expected SHA256 hash before the temp file is renamed to its
// final location.
func (lt *ManagedFileTransferrer) PullFile(ctx context.Context, nodeID, fileID, destDir string) error {
	conn, err := lt.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{
			StreamName:    "PullManagedFile",
			ServerStreams: true,
		},
		"/gastrolog.v1.ClusterService/PullManagedFile",
	)
	if err != nil {
		lt.peers.Invalidate(nodeID)
		return fmt.Errorf("open pull stream to %s: %w", nodeID, err)
	}

	// Send the request.
	if err := stream.SendMsg(&gastrologv1.PullManagedFileRequest{FileId: []byte(fileID)}); err != nil {
		lt.peers.Invalidate(nodeID)
		return fmt.Errorf("send pull request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		lt.peers.Invalidate(nodeID)
		return fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	// Receive chunks, writing to a temp file.
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	tmp, err := os.CreateTemp(destDir, ".pull-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) // no-op after successful rename
	}()

	h := sha256.New()
	var filename, expectedHash string

	for {
		chunk := &gastrologv1.PullManagedFileChunk{}
		if err := stream.RecvMsg(chunk); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			lt.peers.Invalidate(nodeID)
			return fmt.Errorf("receive chunk from %s: %w", nodeID, err)
		}
		// First chunk carries metadata.
		if chunk.GetName() != "" {
			filename = chunk.GetName()
		}
		if chunk.GetSha256() != "" {
			expectedHash = chunk.GetSha256()
		}
		if len(chunk.GetData()) > 0 {
			if _, err := tmp.Write(chunk.GetData()); err != nil {
				return fmt.Errorf("write chunk: %w", err)
			}
			_, _ = h.Write(chunk.GetData())
		}
	}
	_ = tmp.Close()

	if filename == "" {
		return fmt.Errorf("peer %s sent no filename for file %s", nodeID, fileID)
	}

	// Verify hash.
	actualHash := hex.EncodeToString(h.Sum(nil))
	if expectedHash != "" && actualHash != expectedHash {
		return fmt.Errorf("hash mismatch for %s: expected %s, got %s", fileID, expectedHash, actualHash)
	}

	finalPath := filepath.Join(destDir, filename)
	if err := os.Rename(tmpPath, finalPath); err != nil { //nolint:gosec // G703: paths from trusted peer + filename
		return fmt.Errorf("rename to final path: %w", err)
	}

	return nil
}

// ListPeerFiles asks a peer which managed files it has on disk.
func (lt *ManagedFileTransferrer) ListPeerFiles(ctx context.Context, nodeID string) ([]string, error) {
	conn, err := lt.peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	resp := &gastrologv1.ListPeerManagedFilesResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ListPeerManagedFiles", &gastrologv1.ListPeerManagedFilesRequest{}, resp); err != nil {
		lt.peers.Invalidate(nodeID)
		return nil, fmt.Errorf("list peer files on %s: %w", nodeID, err)
	}

	bids := resp.GetFileIds()
	ids := make([]string, len(bids))
	for i, b := range bids {
		ids[i] = string(b)
	}
	return ids, nil
}
