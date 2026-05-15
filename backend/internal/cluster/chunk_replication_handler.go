package cluster

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"

	"google.golang.org/grpc"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
)

// pendingImport tracks an in-flight streaming ImportSealed on the follower
// side. The handler pushes records into recordCh as ImportRecords frames
// arrive; the importer goroutine pulls them via the RecordIterator handed
// to vaultRecordImporter. On ImportCommit the handler closes recordCh and
// waits for resultCh, which carries the importer's final error.
//
// Backpressure flows naturally: recordCh is unbuffered, so the leader's
// per-frame ack only fires after every record in the frame has been
// pulled by the importer.
type pendingImport struct {
	chunkID  chunk.ChunkID
	recordCh chan chunk.Record
	resultCh chan error
	cancel   context.CancelFunc
}

// chunkReplicationStreamHandler processes a bidirectional VaultChunkReplication
// stream. The leader sends ChunkReplicationCommand messages; this handler
// processes them sequentially and replies with ChunkReplicationAck.
//
// Sequential processing on a single stream is the ordering guarantee —
// a seal command is fully processed before the subsequent sealed chunk
// import arrives. This eliminates the race between record forwarding and
// sealed chunk replacement.
//
// Streaming ImportSealed (gastrolog-4yvhh) maintains per-stream state in
// `pending`: ImportBegin opens it, ImportRecords frames push records into
// the channel, ImportCommit closes the channel and collects the importer
// result. At most one import is in flight per stream because the leader's
// send/ack mutex serializes commands on the wire — a stale `pending` here
// would already be a protocol violation.
func chunkReplicationStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	var pending *pendingImport
	defer func() {
		if pending != nil {
			pending.cancel()
			// Importer may still be blocked in iter waiting for input;
			// closing the record channel after cancel ensures iter
			// observes the cancellation, returns an error, and the
			// importer exits.
			close(pending.recordCh)
			<-pending.resultCh
		}
	}()

	for {
		msg := &gastrologv1.ChunkReplicationCommand{}
		if err := s.recvOrShutdown(stream, msg); err != nil {
			// EOF = peer closed the stream normally; errShuttingDown = we
			// are tearing down the cluster server. Both are clean exits.
			if errors.Is(err, io.EOF) || errors.Is(err, errShuttingDown) {
				return nil
			}
			return err
		}

		ack := s.handleReplicationCommand(stream.Context(), msg, &pending)
		if err := stream.SendMsg(ack); err != nil {
			return err
		}
	}
}

func (s *Server) handleReplicationCommand(ctx context.Context, msg *gastrologv1.ChunkReplicationCommand, pending **pendingImport) *gastrologv1.ChunkReplicationAck {
	vaultID := glid.FromBytes(msg.GetVaultId())

	switch cmd := msg.Command.(type) {
	case *gastrologv1.ChunkReplicationCommand_Append:
		return s.handleReplicationAppend(ctx, vaultID, cmd.Append)
	case *gastrologv1.ChunkReplicationCommand_Seal:
		return s.handleReplicationSeal(ctx, vaultID, cmd.Seal)
	case *gastrologv1.ChunkReplicationCommand_ImportBegin:
		return s.handleReplicationImportBegin(ctx, vaultID, cmd.ImportBegin, pending)
	case *gastrologv1.ChunkReplicationCommand_ImportRecords:
		return s.handleReplicationImportRecords(cmd.ImportRecords, *pending)
	case *gastrologv1.ChunkReplicationCommand_ImportCommit:
		return s.handleReplicationImportCommit(cmd.ImportCommit, pending)
	case *gastrologv1.ChunkReplicationCommand_DeleteChunk:
		return s.handleReplicationDelete(ctx, vaultID, cmd.DeleteChunk)
	default:
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "unknown command type"}
	}
}

func (s *Server) handleReplicationAppend(ctx context.Context, vaultID glid.GLID, cmd *gastrologv1.ChunkReplicationAppend) *gastrologv1.ChunkReplicationAck {
	if s.recordAppenderForVault == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "vault appender not configured"}
	}

	chunkID := chunk.ChunkID{}
	if len(cmd.GetChunkId()) >= glid.Size {
		chunkID = chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
	}

	for _, er := range cmd.GetRecords() {
		rec := convert.ExportToRecord(er)
		if err := s.recordAppenderForVault(ctx, vaultID, chunkID, rec); err != nil {
			if isTombstonedErr(err) {
				// Chunk was deleted between the leader scheduling this
				// append and its arrival here. Ack as success — goal
				// (chunk absent on this node) is already achieved.
				return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
			}
			return &gastrologv1.ChunkReplicationAck{
				Ok:      false,
				Error:   "append failed: " + err.Error(),
				ChunkId: cmd.GetChunkId(),
			}
		}
	}

	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

func (s *Server) handleReplicationSeal(ctx context.Context, vaultID glid.GLID, cmd *gastrologv1.ChunkReplicationSeal) *gastrologv1.ChunkReplicationAck {
	if s.chunkSealExecutor == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "seal executor not configured"}
	}

	chunkID := chunk.ChunkID{}
	if len(cmd.GetChunkId()) >= glid.Size {
		chunkID = chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
	}

	if err := s.chunkSealExecutor(ctx, vaultID, chunkID); err != nil {
		if isTombstonedErr(err) {
			return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
		}
		return &gastrologv1.ChunkReplicationAck{
			Ok:      false,
			Error:   "seal failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

// isTombstonedErr reports whether err indicates the target chunk has been
// tombstoned (deleted and within the retention window). Such errors are
// translated into successful acks on the replication receive path — the
// goal (chunk absent on this node) is already achieved.
func isTombstonedErr(err error) bool {
	return errors.Is(err, chunk.ErrChunkTombstoned)
}

// handleReplicationImportBegin opens a streaming import: spawn an
// importer goroutine that consumes from an unbuffered record channel,
// stash the channel + result on `pending`. The importer call (which
// drives storage writes, indexing, etc.) runs concurrently with the
// gRPC handler so the stream can keep receiving ImportRecords frames
// without first materializing the whole chunk.
func (s *Server) handleReplicationImportBegin(ctx context.Context, vaultID glid.GLID, cmd *gastrologv1.ChunkReplicationImportBegin, pending **pendingImport) *gastrologv1.ChunkReplicationAck {
	if s.vaultRecordImporter == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "vault importer not configured"}
	}
	// If a previous import is stuck pending (leader gave up between Begin
	// and Commit — orchestrator restart, cursor read error, ctx cancel),
	// the per-stream state would wedge every subsequent ImportSealed for
	// this (vault, follower) pair until the stream itself tore down. Treat
	// a fresh ImportBegin as the leader's signal that it has moved on:
	// cancel the wedged import, drain its result, and accept the new one.
	// See gastrolog-5z7l8 — without this, partial imports leave sealed
	// chunks permanently under-replicated, with the leader logging
	// "ImportBegin while import for chunk X already in flight" forever.
	if p := *pending; p != nil {
		s.logger.Warn("ImportBegin while previous import in flight — preempting",
			"vault", vaultID, "prev_chunk", p.chunkID, "new_chunk",
			chunk.ChunkID(glid.FromBytes(cmd.GetChunkId())))
		p.cancel()
		close(p.recordCh)
		<-p.resultCh
		*pending = nil
	}

	chunkID := chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
	importCtx, cancel := context.WithCancel(ctx)
	recordCh := make(chan chunk.Record)
	resultCh := make(chan error, 1)

	iter := func() (chunk.Record, error) {
		select {
		case rec, ok := <-recordCh:
			if !ok {
				return chunk.Record{}, chunk.ErrNoMoreRecords
			}
			return rec, nil
		case <-importCtx.Done():
			return chunk.Record{}, importCtx.Err()
		}
	}

	go func() {
		resultCh <- s.vaultRecordImporter(importCtx, vaultID, chunkID, iter)
	}()

	*pending = &pendingImport{
		chunkID:  chunkID,
		recordCh: recordCh,
		resultCh: resultCh,
		cancel:   cancel,
	}
	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}

// handleReplicationImportRecords feeds one batch of records into the
// in-flight importer's input channel. The ack is delayed until every
// record in the batch has been pulled by the importer — that's the
// backpressure mechanism that ties wire-frame cadence to actual storage
// progress.
//
// If the importer goroutine has already exited (e.g. an early storage
// error), the channel sends will block forever; the resultCh select
// detects that case and surfaces the importer's error to the leader so
// it can stop pumping records into a dead import.
func (s *Server) handleReplicationImportRecords(cmd *gastrologv1.ChunkReplicationImportRecords, pending *pendingImport) *gastrologv1.ChunkReplicationAck {
	if pending == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "ImportRecords without ImportBegin"}
	}
	chunkIDBytes := glid.GLID(pending.chunkID).ToProto()
	for _, er := range cmd.GetRecords() {
		rec := convert.ExportToRecord(er)
		select {
		case pending.recordCh <- rec:
		case err := <-pending.resultCh:
			// Importer exited before we finished pushing this batch.
			// Put the error back so ImportCommit also sees it, then
			// fail this frame.
			pending.resultCh <- err
			if err != nil && isTombstonedErr(err) {
				return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: chunkIDBytes}
			}
			msg := "import aborted before all records consumed"
			if err != nil {
				msg = "import failed mid-stream: " + err.Error()
			}
			return &gastrologv1.ChunkReplicationAck{
				Ok:      false,
				Error:   msg,
				ChunkId: chunkIDBytes,
			}
		}
	}
	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: chunkIDBytes}
}

// handleReplicationImportCommit closes the record channel, waits for
// the importer goroutine to finalize, and acks with the result.
func (s *Server) handleReplicationImportCommit(cmd *gastrologv1.ChunkReplicationImportCommit, pending **pendingImport) *gastrologv1.ChunkReplicationAck {
	p := *pending
	if p == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "ImportCommit without ImportBegin"}
	}
	*pending = nil
	chunkIDBytes := glid.GLID(p.chunkID).ToProto()

	if len(cmd.GetChunkId()) >= glid.Size {
		got := chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))
		if got != p.chunkID {
			p.cancel()
			close(p.recordCh)
			<-p.resultCh
			return &gastrologv1.ChunkReplicationAck{
				Ok:      false,
				Error:   fmt.Sprintf("ImportCommit chunk_id mismatch: got %s, want %s", got, p.chunkID),
				ChunkId: chunkIDBytes,
			}
		}
	}

	close(p.recordCh)
	err := <-p.resultCh
	p.cancel()

	if err != nil {
		if isTombstonedErr(err) {
			return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: chunkIDBytes}
		}
		return &gastrologv1.ChunkReplicationAck{
			Ok:      false,
			Error:   "import failed: " + err.Error(),
			ChunkId: chunkIDBytes,
		}
	}
	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: chunkIDBytes}
}

func (s *Server) handleReplicationDelete(ctx context.Context, vaultID glid.GLID, cmd *gastrologv1.ChunkReplicationDelete) *gastrologv1.ChunkReplicationAck {
	if s.deleteChunkExecutor == nil {
		return &gastrologv1.ChunkReplicationAck{Ok: false, Error: "delete executor not configured"}
	}

	chunkID := chunk.ChunkID(glid.FromBytes(cmd.GetChunkId()))

	if err := s.deleteChunkExecutor(ctx, vaultID, chunkID); err != nil {
		return &gastrologv1.ChunkReplicationAck{
			Ok:      false,
			Error:   "delete failed: " + err.Error(),
			ChunkId: cmd.GetChunkId(),
		}
	}

	return &gastrologv1.ChunkReplicationAck{Ok: true, ChunkId: cmd.GetChunkId()}
}
