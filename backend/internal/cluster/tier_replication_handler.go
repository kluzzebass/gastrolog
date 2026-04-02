package cluster

import (
	"context"
	"errors"
	"io"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// tierReplicationStreamHandler processes a bidirectional TierReplication
// stream. The leader sends TierReplicationCommand messages; this handler
// processes them sequentially and replies with TierReplicationAck.
//
// Sequential processing on a single stream is the ordering guarantee —
// a seal command is fully processed before the subsequent sealed chunk
// import arrives. This eliminates the race between record forwarding and
// sealed chunk replacement.
func tierReplicationStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)

	for {
		msg := &gastrologv1.TierReplicationCommand{}
		if err := stream.RecvMsg(msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		ack := s.handleReplicationCommand(stream.Context(), msg)
		if err := stream.SendMsg(ack); err != nil {
			return err
		}
	}
}

func (s *Server) handleReplicationCommand(ctx context.Context, msg *gastrologv1.TierReplicationCommand) *gastrologv1.TierReplicationAck {
	vaultID, err := uuid.Parse(msg.GetVaultId())
	if err != nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "invalid vault_id: " + err.Error()}
	}
	tierID, err := uuid.Parse(msg.GetTierId())
	if err != nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "invalid tier_id: " + err.Error()}
	}

	switch cmd := msg.Command.(type) {
	case *gastrologv1.TierReplicationCommand_Append:
		return s.handleReplicationAppend(ctx, vaultID, tierID, cmd.Append)
	case *gastrologv1.TierReplicationCommand_Seal:
		return s.handleReplicationSeal(ctx, vaultID, tierID, cmd.Seal)
	case *gastrologv1.TierReplicationCommand_ImportSealed:
		return s.handleReplicationImport(ctx, vaultID, tierID, cmd.ImportSealed)
	case *gastrologv1.TierReplicationCommand_DeleteChunk:
		return s.handleReplicationDelete(ctx, vaultID, tierID, cmd.DeleteChunk)
	default:
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "unknown command type"}
	}
}

func (s *Server) handleReplicationAppend(ctx context.Context, vaultID, tierID uuid.UUID, cmd *gastrologv1.TierReplicationAppend) *gastrologv1.TierReplicationAck {
	if s.recordTierAppender == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "tier appender not configured"}
	}

	chunkID := chunk.ChunkID{}
	if cmd.GetChunkId() != "" {
		if parsed, err := chunk.ParseChunkID(cmd.GetChunkId()); err == nil {
			chunkID = parsed
		}
	}

	for _, er := range cmd.GetRecords() {
		rec := exportRecordToChunk(er)
		if err := s.recordTierAppender(ctx, vaultID, tierID, chunkID, rec); err != nil {
			return &gastrologv1.TierReplicationAck{
				Ok:      false,
				Error:   "append failed: " + err.Error(),
				ChunkId: cmd.GetChunkId(),
			}
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

func (s *Server) handleReplicationSeal(ctx context.Context, vaultID, tierID uuid.UUID, cmd *gastrologv1.TierReplicationSeal) *gastrologv1.TierReplicationAck {
	if s.sealTierExecutor == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "seal executor not configured"}
	}

	chunkID := chunk.ChunkID{}
	if cmd.GetChunkId() != "" {
		if parsed, err := chunk.ParseChunkID(cmd.GetChunkId()); err == nil {
			chunkID = parsed
		}
	}

	if err := s.sealTierExecutor(ctx, vaultID, tierID, chunkID); err != nil {
		return &gastrologv1.TierReplicationAck{
			Ok:      false,
			Error:   "seal failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

func (s *Server) handleReplicationImport(ctx context.Context, vaultID, tierID uuid.UUID, cmd *gastrologv1.TierReplicationImport) *gastrologv1.TierReplicationAck {
	if s.tierRecordImporter == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "tier importer not configured"}
	}

	chunkID, err := chunk.ParseChunkID(cmd.GetChunkId())
	if err != nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "invalid chunk_id: " + err.Error()}
	}

	records := make([]chunk.Record, 0, len(cmd.GetRecords()))
	for _, er := range cmd.GetRecords() {
		records = append(records, exportRecordToChunk(er))
	}

	idx := 0
	iter := func() (chunk.Record, error) {
		if idx >= len(records) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		rec := records[idx]
		idx++
		return rec, nil
	}

	if err := s.tierRecordImporter(ctx, vaultID, tierID, chunkID, iter); err != nil {
		return &gastrologv1.TierReplicationAck{
			Ok:      false,
			Error:   "import failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

func (s *Server) handleReplicationDelete(ctx context.Context, vaultID, tierID uuid.UUID, cmd *gastrologv1.TierReplicationDelete) *gastrologv1.TierReplicationAck {
	if s.deleteChunkExecutor == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "delete executor not configured"}
	}

	chunkID, err := chunk.ParseChunkID(cmd.GetChunkId())
	if err != nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "invalid chunk_id: " + err.Error()}
	}

	if err := s.deleteChunkExecutor(ctx, vaultID, tierID, chunkID); err != nil {
		return &gastrologv1.TierReplicationAck{
			Ok:      false,
			Error:   "delete failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}
