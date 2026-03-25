package cluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
func (ct *ChunkTransferrer) TransferRecords(ctx context.Context, nodeID string, vaultID uuid.UUID, next chunk.RecordIterator) error {
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
			return fmt.Errorf("read records for transfer: %w", iterErr)
		}
		msg := &gastrologv1.ImportRecordMessage{
			VaultId: vid,
			Record:  chunkRecordToExport(rec),
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
func (ct *ChunkTransferrer) ForwardAppend(ctx context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	exportRecs := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		er := chunkRecordToExport(rec)
		er.IngestSeq = rec.EventID.IngestSeq
		if rec.EventID.IngesterID != ([16]byte{}) {
			er.IngesterId = rec.EventID.IngesterID[:]
		}
		exportRecs[i] = er
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

// ForwardTierAppend sends records to a specific tier on a remote node.
// Same as ForwardAppend but sets TierId so the receiver targets that tier
// instead of the vault's active tier. Used by inter-tier transition.
func (ct *ChunkTransferrer) ForwardTierAppend(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, records []chunk.Record) error {
	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	exportRecs := make([]*gastrologv1.ExportRecord, len(records))
	for i, rec := range records {
		er := chunkRecordToExport(rec)
		er.IngestSeq = rec.EventID.IngestSeq
		if rec.EventID.IngesterID != ([16]byte{}) {
			er.IngesterId = rec.EventID.IngesterID[:]
		}
		exportRecs[i] = er
	}

	req := &gastrologv1.ForwardRecordsRequest{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		Records: exportRecs,
	}
	resp := &gastrologv1.ForwardRecordsResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardRecords", req, resp); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("forward tier append to %s: %w", nodeID, err)
	}
	return nil
}

// ForwardSealTier commands a secondary to seal its active chunk at the same
// boundary as the primary. Used for seal synchronization during replication.
func (ct *ChunkTransferrer) ForwardSealTier(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
	conn, err := ct.peers.Conn(nodeID)
	if err != nil {
		return fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	req := &gastrologv1.ForwardSealTierRequest{
		VaultId: vaultID.String(),
		TierId:  tierID.String(),
		ChunkId: chunkID.String(),
	}
	resp := &gastrologv1.ForwardSealTierResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardSealTier", req, resp); err != nil {
		ct.peers.Invalidate(nodeID)
		return fmt.Errorf("forward seal tier to %s: %w", nodeID, err)
	}
	return nil
}

// WaitVaultReady polls the target node until the vault is registered and
// accepting records, or ctx expires. Uses ForwardListChunks as a lightweight
// existence probe — it returns an error if the vault doesn't exist.
func (ct *ChunkTransferrer) WaitVaultReady(ctx context.Context, nodeID string, vaultID uuid.UUID) error {
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

// chunkRecordToExport converts a chunk.Record to a proto ExportRecord.
func chunkRecordToExport(rec chunk.Record) *gastrologv1.ExportRecord {
	er := &gastrologv1.ExportRecord{Raw: rec.Raw}
	if !rec.SourceTS.IsZero() {
		er.SourceTs = timestamppb.New(rec.SourceTS)
	}
	if !rec.IngestTS.IsZero() {
		er.IngestTs = timestamppb.New(rec.IngestTS)
	}
	// WriteTS is not sent — the destination re-stamps at import time.
	if len(rec.Attrs) > 0 {
		er.Attrs = make(map[string]string, len(rec.Attrs))
		maps.Copy(er.Attrs, rec.Attrs)
	}
	return er
}
