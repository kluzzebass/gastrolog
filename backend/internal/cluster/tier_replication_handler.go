package cluster

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"io"

	"google.golang.org/grpc"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
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
		if err := s.recvOrShutdown(stream, msg); err != nil {
			// EOF = peer closed the stream normally; errShuttingDown = we
			// are tearing down the cluster server. Both are clean exits.
			if errors.Is(err, io.EOF) || errors.Is(err, errShuttingDown) {
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
	vaultID := glid.FromBytes(msg.GetVaultId())
	tierID := glid.FromBytes(msg.GetTierId())

	switch cmd := msg.Command.(type) {
	case *gastrologv1.TierReplicationCommand_Append:
		return s.handleReplicationAppend(ctx, vaultID, tierID, cmd.Append)
	case *gastrologv1.TierReplicationCommand_Seal:
		return s.handleReplicationSeal(ctx, vaultID, tierID, cmd.Seal)
	case *gastrologv1.TierReplicationCommand_DeleteChunk:
		return s.handleReplicationDelete(ctx, vaultID, tierID, cmd.DeleteChunk)
	default:
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "unknown command type"}
	}
}

func (s *Server) handleReplicationAppend(ctx context.Context, vaultID, tierID glid.GLID, cmd *gastrologv1.TierReplicationAppend) *gastrologv1.TierReplicationAck {
	if s.recordTierAppender == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "tier appender not configured"}
	}

	chunkID := chunk.ChunkID{}
	if len(cmd.GetChunkId()) >= glid.Size {
		chunkID = chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
	}

	for _, er := range cmd.GetRecords() {
		rec := convert.ExportToRecord(er)
		if err := s.recordTierAppender(ctx, vaultID, tierID, chunkID, rec); err != nil {
			if isTombstonedErr(err) {
				// Chunk was deleted between the leader scheduling this
				// append and its arrival here. Ack as success — goal
				// (chunk absent on this node) is already achieved.
				return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
			}
			return &gastrologv1.TierReplicationAck{
				Ok:      false,
				Error:   "append failed: " + err.Error(),
				ChunkId: cmd.GetChunkId(),
			}
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

func (s *Server) handleReplicationSeal(ctx context.Context, vaultID, tierID glid.GLID, cmd *gastrologv1.TierReplicationSeal) *gastrologv1.TierReplicationAck {
	if s.sealTierExecutor == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "seal executor not configured"}
	}

	chunkID := chunk.ChunkID{}
	if len(cmd.GetChunkId()) >= glid.Size {
		chunkID = chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
	}

	if err := s.sealTierExecutor(ctx, vaultID, tierID, chunkID); err != nil {
		if isTombstonedErr(err) {
			return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
		}
		return &gastrologv1.TierReplicationAck{
			Ok:      false,
			Error:   "seal failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

// isTombstonedErr reports whether err indicates the target chunk has been
// tombstoned (deleted and within the retention window). Such errors are
// translated into successful acks on the replication receive path — the
// goal (chunk absent on this node) is already achieved.
func isTombstonedErr(err error) bool {
	return errors.Is(err, chunk.ErrChunkTombstoned)
}

func (s *Server) handleReplicationDelete(ctx context.Context, vaultID, tierID glid.GLID, cmd *gastrologv1.TierReplicationDelete) *gastrologv1.TierReplicationAck {
	if s.deleteChunkExecutor == nil {
		return &gastrologv1.TierReplicationAck{Ok: false, Error: "delete executor not configured"}
	}

	chunkID := chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))

	if err := s.deleteChunkExecutor(ctx, vaultID, tierID, chunkID); err != nil {
		return &gastrologv1.TierReplicationAck{
			Ok:      false,
			Error:   "delete failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.TierReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}
