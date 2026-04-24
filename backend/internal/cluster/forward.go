package cluster

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"io"
	"iter"
	"os"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecordAppender appends a single record to a local vault.
// Used by the ForwardRecords handler to write received records.
type RecordAppender func(ctx context.Context, vaultID glid.GLID, rec chunk.Record) error

// RecordTierAppender appends a single record to a specific tier in a local vault.
// Used by the ForwardRecords handler when tier_id is set (inter-tier transition).
type RecordTierAppender func(ctx context.Context, vaultID, tierID glid.GLID, leaderChunkID chunk.ChunkID, rec chunk.Record) error

// SearchExecutor runs a search on a local vault and returns results.
// For regular searches, it returns an iterator over records (the caller
// streams them as they arrive). For pipeline queries (stats, timechart),
// it returns a TableResult with a nil iterator. The histogram slice (if
// non-nil) provides an approximate volume histogram for the searched vault.
// Used by the ForwardSearch handler to serve remote search requests.
// The resumeToken parameter allows resuming a paginated search. The returned
// getToken function returns a resume token for the next page (nil if exhausted).
type SearchExecutor func(ctx context.Context, vaultID glid.GLID, queryExpr string, resumeToken []byte) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error)

// ContextExecutor fetches records surrounding a specific position in a local vault.
// Used by the ForwardGetContext handler to serve remote context requests.
type ContextExecutor func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, pos uint64, before, after int) ([]chunk.Record, chunk.Record, []chunk.Record, error)

// ListChunksExecutor lists chunks in a local vault for remote requests.
type ListChunksExecutor func(ctx context.Context, vaultID glid.GLID) ([]*gastrologv1.ChunkMeta, error)

// GetIndexesExecutor returns index status for a chunk in a local vault.
type GetIndexesExecutor func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) (*gastrologv1.GetIndexesResponse, error)

// ValidateVaultExecutor validates a local vault and returns the result.
type ValidateVaultExecutor func(ctx context.Context, vaultID glid.GLID) (*gastrologv1.ValidateVaultResponse, error)

// ExplainExecutor returns the explain plan for local vaults matching the query.
// Used by the ForwardExplain handler to serve remote explain requests.
type ExplainExecutor func(ctx context.Context, vaultIDs []glid.GLID, queryExpr string) ([]*gastrologv1.ChunkPlan, int32, error)

// FollowExecutor runs a follow (tail -f) on local vaults for a remote request.
// Returns an iterator that yields new records as they arrive. The caller is
// responsible for cancelling the context to stop the follow.
type FollowExecutor func(ctx context.Context, vaultIDs []glid.GLID, queryExpr string) (iter.Seq2[chunk.Record, error], error)

// GetChunkExecutor returns details for a specific chunk in a local vault.
type GetChunkExecutor func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) (*gastrologv1.ChunkMeta, error)

// AnalyzeChunkExecutor runs index analysis on a local vault (or specific chunk).
type AnalyzeChunkExecutor func(ctx context.Context, vaultID glid.GLID, chunkID string) ([]*gastrologv1.ChunkAnalysis, error)

// SealVaultExecutor seals the active chunk of a local vault.
type SealVaultExecutor func(ctx context.Context, vaultID glid.GLID) error

// ReindexVaultExecutor rebuilds all indexes for a local vault.
type ReindexVaultExecutor func(ctx context.Context, vaultID glid.GLID) (string, error)

// ExportToVaultExecutor runs an export-to-vault job on a local vault.
// Returns the job ID.
type ExportToVaultExecutor func(ctx context.Context, expression string, targetVaultID glid.GLID) (string, error)

// RecordImporter imports records as a new sealed chunk in a vault.
// Used by the ForwardImportRecords handler for cross-node chunk migration.
type RecordImporter func(ctx context.Context, vaultID glid.GLID, next chunk.RecordIterator) error

// TierRecordImporter imports records as a sealed chunk in a specific tier,
// preserving the original chunk ID. Used for sealed-chunk replication.
type TierRecordImporter func(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error

// TierStreamAppender appends streamed records to a tier's active chunk.
// Used for tier transitions — records flow into the destination tier like
// normal ingestion. The tier's rotation policy handles sealing.
type TierStreamAppender func(ctx context.Context, vaultID, tierID glid.GLID, next chunk.RecordIterator) error

// ManagedFileReader opens a managed file for streaming to a peer.
// Returns the original filename, a ReadCloser for the content, and the SHA256 hex hash.
type ManagedFileReader func(fileID string) (name string, rc io.ReadCloser, sha256hex string, err error)

// ManagedFileIDsLister returns the IDs of managed files present on this node's disk.
type ManagedFileIDsLister func() []string

// ── ID parse helpers ────────────────────────────────────────────────

func parseVaultID(raw []byte) (glid.GLID, error) {
	if len(raw) < glid.Size {
		return glid.Nil, status.Error(codes.InvalidArgument, "invalid vault_id: too short")
	}
	return glid.FromBytes(raw), nil
}

func parseTierID(raw []byte) (glid.GLID, error) {
	if len(raw) < glid.Size {
		return glid.Nil, status.Error(codes.InvalidArgument, "invalid tier_id: too short")
	}
	return glid.FromBytes(raw), nil
}

func parseChunkID(raw []byte) (chunk.ChunkID, error) {
	if len(raw) < glid.Size {
		return chunk.ChunkID{}, status.Error(codes.InvalidArgument, "invalid chunk_id: too short")
	}
	return chunk.ChunkID(glid.FromBytes(raw)), nil
}

// SetRecordAppender injects the callback for writing forwarded records.
// Must be called before the cluster server receives ForwardRecords RPCs.
func (s *Server) SetRecordAppender(fn RecordAppender) {
	s.recordAppender = fn
}

// SetRecordTierAppender injects the callback for tier-targeted forwarding.
func (s *Server) SetRecordTierAppender(fn RecordTierAppender) {
	s.recordTierAppender = fn
}

// SetRecordImporter injects the callback for importing transferred records.
// Must be called before ForwardImportRecords RPCs.
func (s *Server) SetRecordImporter(fn RecordImporter) {
	s.recordImporter = fn
}

// SetTierRecordImporter injects the callback for tier-targeted sealed-chunk imports.
func (s *Server) SetTierRecordImporter(fn TierRecordImporter) {
	s.tierRecordImporter = fn
}

// SetTierStreamAppender injects the callback for streaming records to a tier's active chunk.
func (s *Server) SetTierStreamAppender(fn TierStreamAppender) {
	s.tierStreamAppender = fn
}

// SetSearchExecutor injects the callback for handling remote search requests.
func (s *Server) SetSearchExecutor(fn SearchExecutor) {
	s.searchExecutor = fn
}

// SetContextExecutor injects the callback for handling remote GetContext requests.
func (s *Server) SetContextExecutor(fn ContextExecutor) {
	s.contextExecutor = fn
}

// SetListChunksExecutor injects the callback for handling remote ListChunks requests.
func (s *Server) SetListChunksExecutor(fn ListChunksExecutor) {
	s.listChunksExecutor = fn
}

// SetGetIndexesExecutor injects the callback for handling remote GetIndexes requests.
func (s *Server) SetGetIndexesExecutor(fn GetIndexesExecutor) {
	s.getIndexesExecutor = fn
}

// SetValidateVaultExecutor injects the callback for handling remote ValidateVault requests.
func (s *Server) SetValidateVaultExecutor(fn ValidateVaultExecutor) {
	s.validateVaultExecutor = fn
}

// SetExplainExecutor injects the callback for handling remote Explain requests.
func (s *Server) SetExplainExecutor(fn ExplainExecutor) {
	s.explainExecutor = fn
}

// SetFollowExecutor injects the callback for handling remote follow requests.
func (s *Server) SetFollowExecutor(fn FollowExecutor) {
	s.followExecutor = fn
}

// SetGetChunkExecutor injects the callback for handling remote GetChunk requests.
func (s *Server) SetGetChunkExecutor(fn GetChunkExecutor) {
	s.getChunkExecutor = fn
}

// SetAnalyzeChunkExecutor injects the callback for handling remote AnalyzeChunk requests.
func (s *Server) SetAnalyzeChunkExecutor(fn AnalyzeChunkExecutor) {
	s.analyzeChunkExecutor = fn
}

// SetSealVaultExecutor injects the callback for handling remote SealVault requests.
func (s *Server) SetSealVaultExecutor(fn SealVaultExecutor) {
	s.sealVaultExecutor = fn
}

// SetReindexVaultExecutor injects the callback for handling remote ReindexVault requests.
func (s *Server) SetReindexVaultExecutor(fn ReindexVaultExecutor) {
	s.reindexVaultExecutor = fn
}

// SetExportToVaultExecutor injects the callback for handling remote ExportToVault requests.
func (s *Server) SetExportToVaultExecutor(fn ExportToVaultExecutor) {
	s.exportToVaultExecutor = fn
}

// SetManagedFileReader injects the callback for streaming managed files to peers.
func (s *Server) SetManagedFileReader(fn ManagedFileReader) {
	s.managedFileReader = fn
}

// SetManagedFileIDs injects the callback for listing local managed file IDs.
func (s *Server) SetManagedFileIDs(fn ManagedFileIDsLister) {
	s.managedFileIDs = fn
}

// forwardRecords handles the unary ForwardRecords RPC. Used by retention
// eject to send records to the node owning the destination vault. Records
// are appended to the vault's active chunk (no tier targeting).
func (s *Server) forwardRecords(ctx context.Context, req *gastrologv1.ForwardRecordsRequest) (*gastrologv1.ForwardRecordsResponse, error) {
	if s.recordAppender == nil {
		return nil, status.Error(codes.Unavailable, "record appender not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}

	var written int64
	for _, exportRec := range req.GetRecords() {
		rec := convert.ExportToRecord(exportRec)
		if appendErr := s.recordAppender(ctx, vaultID, rec); appendErr != nil {
			if errors.Is(appendErr, ErrForwardTargetNotReady) {
				s.cfg.Logger.Debug("forward: append failed (target not ready)",
					"vault", vaultID, "error", appendErr)
			} else {
				s.cfg.Logger.Warn("forward: append failed",
					"vault", vaultID, "error", appendErr)
			}
			return nil, status.Errorf(codes.Internal, "append record: %v", appendErr)
		}
		written++
	}
	s.forwardedReceived.Add(written)

	return &gastrologv1.ForwardRecordsResponse{RecordsWritten: written}, nil
}

// streamForwardRecordsHandler handles the client-streaming StreamForwardRecords
// RPC. Each message is a ForwardRecordsRequest (vault_id + batch of records).
// This is the same payload as the unary ForwardRecords RPC, but on a persistent
// stream — eliminating per-RPC connection overhead.
func streamForwardRecordsHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	if s.recordAppender == nil {
		return status.Error(codes.Unavailable, "record appender not configured")
	}

	var written int64
	for {
		var msg gastrologv1.ForwardRecordsRequest
		err := s.recvOrShutdown(stream, &msg)
		if errors.Is(err, io.EOF) {
			return stream.SendMsg(&gastrologv1.ForwardRecordsResponse{
				RecordsWritten: written,
			})
		}
		// Cluster server is shutting down — return cleanly so GracefulStop
		// can unblock. See gastrolog-1e5ke.
		if errors.Is(err, errShuttingDown) {
			return nil
		}
		if err != nil {
			return err
		}

		vaultID, err := parseVaultID(msg.GetVaultId())
		if err != nil {
			continue
		}

		// StreamForwardRecords is exclusively the cross-node vault routing
		// path since gastrolog-5c6fp — tier-targeted replication goes through
		// TierReplication instead. Always append as a regular vault record.
		for _, exportRec := range msg.GetRecords() {
			rec := convert.ExportToRecord(exportRec)
			if appendErr := s.recordAppender(stream.Context(), vaultID, rec); appendErr != nil {
				if errors.Is(appendErr, ErrForwardTargetNotReady) {
					s.logger.Debug("stream forward: append failed (target not ready)",
						"vault", vaultID, "error", appendErr)
				} else {
					s.logger.Warn("stream forward: append failed",
						"vault", vaultID, "error", appendErr)
				}
				continue
			}
			written++
		}
		s.forwardedReceived.Add(int64(len(msg.GetRecords())))
	}
}

// forwardImportRecordsStreamHandler handles the client-streaming
// ForwardImportRecords RPC. Each message carries a single record; the server
// wraps the stream as a RecordIterator and feeds it into ImportRecords so at
// most one ExportRecord lives in memory at a time.
func forwardImportRecordsStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	if s.recordImporter == nil {
		return status.Error(codes.Unavailable, "record importer not configured")
	}

	// Read first message to get vault_id and optional tier_id.
	first := &gastrologv1.ImportRecordMessage{}
	err := s.recvOrShutdown(stream, first)
	if errors.Is(err, io.EOF) {
		// Empty stream — send zero-record response.
		return stream.SendMsg(&gastrologv1.ForwardRecordsResponse{})
	}
	// Cluster server is shutting down — return cleanly. The iterator
	// was never entered so no partial chunk state to worry about.
	if errors.Is(err, errShuttingDown) {
		return nil
	}
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "receive first message: %v", err)
	}
	vaultID, err := parseVaultID(first.GetVaultId())
	if err != nil {
		return err
	}

	// tier_id routes the stream to a specific tier's active chunk (used by
	// StreamToTier for tier transitions). Empty tier_id means the import
	// creates a new sealed chunk in the vault (used by TransferRecords for
	// cross-node chunk migration).
	var tierID glid.GLID
	if len(first.GetTierId()) >= glid.Size {
		tierID, err = parseTierID(first.GetTierId())
		if err != nil {
			return err
		}
	}

	// Build iterator: yields first record, then reads from stream.
	var count int64
	firstConsumed := false
	next := chunk.RecordIterator(func() (chunk.Record, error) {
		if !firstConsumed {
			firstConsumed = true
			count++
			return convert.ExportToRecord(first.GetRecord()), nil
		}
		msg := &gastrologv1.ImportRecordMessage{}
		recvErr := s.recvOrShutdown(stream, msg)
		if errors.Is(recvErr, io.EOF) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		// Cluster server is shutting down — treat as end of iteration so
		// the importer finalizes with the records received so far. The
		// partial chunk is safe because ImportRecords has atomic-seal
		// semantics. See gastrolog-1e5ke.
		if errors.Is(recvErr, errShuttingDown) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		if recvErr != nil {
			return chunk.Record{}, recvErr
		}
		count++
		return convert.ExportToRecord(msg.GetRecord()), nil
	})

	if tierID != glid.Nil && s.tierStreamAppender != nil {
		if err := s.tierStreamAppender(stream.Context(), vaultID, tierID, next); err != nil {
			return status.Errorf(codes.Internal, "tier stream append: %v", err)
		}
	} else {
		if err := s.recordImporter(stream.Context(), vaultID, next); err != nil {
			return status.Errorf(codes.Internal, "import records: %v", err)
		}
	}

	return stream.SendMsg(&gastrologv1.ForwardRecordsResponse{RecordsWritten: count})
}

// forwardFollowStreamHandler handles the server-streaming ForwardFollow RPC.
// Runs eng.Follow() on the specified local vaults and streams new records back
// to the coordinating node as they arrive.
func forwardFollowStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	if s.followExecutor == nil {
		return status.Error(codes.Unavailable, "follow executor not configured")
	}

	req := &gastrologv1.ForwardFollowRequest{}
	if err := stream.RecvMsg(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "receive request: %v", err)
	}

	vaultIDs := glid.SliceFromProto(req.GetVaultIds())

	records, err := s.followExecutor(stream.Context(), vaultIDs, req.GetQuery())
	if err != nil {
		return status.Errorf(codes.Internal, "follow: %v", err)
	}

	for rec, err := range records {
		if err != nil {
			return status.Errorf(codes.Internal, "follow record: %v", err)
		}
		resp := &gastrologv1.ForwardFollowResponse{
			Records: []*gastrologv1.ExportRecord{convert.RecordToExport(rec)},
		}
		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
	return nil
}

// forwardSearchStreamHandler handles the server-streaming ForwardSearch RPC.
// Executes a search on a local vault and streams matching records back to the
// requesting node in batches of 200. For pipeline queries, sends a single
// message with the TableResult.
func forwardSearchStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	if s.searchExecutor == nil {
		return status.Error(codes.Unavailable, "search executor not configured")
	}

	req := &gastrologv1.ForwardSearchRequest{}
	if err := stream.RecvMsg(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "receive request: %v", err)
	}

	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return err
	}

	searchIter, getToken, tableResult, histogram, err := s.searchExecutor(stream.Context(), vaultID, req.GetQuery(), req.GetResumeToken())
	if err != nil {
		return status.Errorf(codes.Internal, "search: %v", err)
	}

	// Pipeline path: send single message with TableResult + Histogram.
	if tableResult != nil {
		return stream.SendMsg(&gastrologv1.ForwardSearchResponse{
			TableResult: tableResult,
			Histogram:   histogram,
		})
	}

	// No results (vault has no leader tiers on this node).
	if searchIter == nil {
		return stream.SendMsg(&gastrologv1.ForwardSearchResponse{Histogram: histogram})
	}

	// Record path: iterate and batch 200 records per message.
	const batchSize = 200
	batch := make([]*gastrologv1.ExportRecord, 0, batchSize)
	first := true
	for rec, iterErr := range searchIter {
		if iterErr != nil {
			// EOF can occur when a chunk is deleted mid-read (e.g., ImportToTier
			// replacing a forwarded-record chunk on a follower). Treat as
			// end-of-results — the data is still available via retry.
			if errors.Is(iterErr, io.EOF) || isMissingLocalChunkFileError(iterErr) {
				break
			}
			return status.Errorf(codes.Internal, "search record: %v", iterErr)
		}
		batch = append(batch, convert.RecordToExport(rec))
		if len(batch) >= batchSize {
			resp := &gastrologv1.ForwardSearchResponse{Records: batch}
			if first {
				resp.Histogram = histogram
				first = false
			}
			if err := stream.SendMsg(resp); err != nil {
				return err
			}
			batch = make([]*gastrologv1.ExportRecord, 0, batchSize)
		}
	}
	// Send remaining records + resume token in the final message.
	resp := &gastrologv1.ForwardSearchResponse{Records: batch}
	if first {
		resp.Histogram = histogram
	}
	if getToken != nil {
		resp.ResumeToken = getToken()
		resp.HasMore = len(resp.ResumeToken) > 0
	}
	return stream.SendMsg(resp)
}

func isMissingLocalChunkFileError(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := err.Error()
	if !strings.Contains(msg, "no such file or directory") {
		return false
	}
	return strings.Contains(msg, "open raw.log") ||
		strings.Contains(msg, "open idx.log") ||
		strings.Contains(msg, "open attr.log") ||
		strings.Contains(msg, "open attr_dict")
}

// forwardGetContext handles the ForwardGetContext RPC. Runs GetContext on a
// local vault and returns the anchor + surrounding records to the requesting node.
func (s *Server) forwardGetContext(ctx context.Context, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	if s.contextExecutor == nil {
		return nil, status.Error(codes.Unavailable, "context executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunkID, err := parseChunkID(req.GetChunkId())
	if err != nil {
		return nil, err
	}

	before, anchor, after, err := s.contextExecutor(ctx, vaultID, chunkID, req.GetPos(), int(req.GetBefore()), int(req.GetAfter()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get context: %v", err)
	}

	resp := &gastrologv1.ForwardGetContextResponse{
		Anchor: convert.RecordToExport(anchor),
		Before: make([]*gastrologv1.ExportRecord, len(before)),
		After:  make([]*gastrologv1.ExportRecord, len(after)),
	}
	for i, rec := range before {
		resp.Before[i] = convert.RecordToExport(rec)
	}
	for i, rec := range after {
		resp.After[i] = convert.RecordToExport(rec)
	}
	return resp, nil
}

// forwardListChunks handles the ForwardListChunks RPC. Lists chunks in a
// local vault and returns them to the requesting node.
func (s *Server) forwardListChunks(ctx context.Context, req *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error) {
	if s.listChunksExecutor == nil {
		return nil, status.Error(codes.Unavailable, "list chunks executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunks, err := s.listChunksExecutor(ctx, vaultID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list chunks: %v", err)
	}
	return &gastrologv1.ForwardListChunksResponse{Chunks: chunks}, nil
}

// forwardGetIndexes handles the ForwardGetIndexes RPC. Returns index status
// for a chunk in a local vault.
func (s *Server) forwardGetIndexes(ctx context.Context, req *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error) {
	if s.getIndexesExecutor == nil {
		return nil, status.Error(codes.Unavailable, "get indexes executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunkID, err := parseChunkID(req.GetChunkId())
	if err != nil {
		return nil, err
	}
	resp, err := s.getIndexesExecutor(ctx, vaultID, chunkID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get indexes: %v", err)
	}
	return &gastrologv1.ForwardGetIndexesResponse{
		Sealed:  resp.GetSealed(),
		Indexes: resp.GetIndexes(),
	}, nil
}

// forwardValidateVault handles the ForwardValidateVault RPC. Validates a
// local vault's chunk and index integrity.
func (s *Server) forwardValidateVault(ctx context.Context, req *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error) {
	if s.validateVaultExecutor == nil {
		return nil, status.Error(codes.Unavailable, "validate vault executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	resp, err := s.validateVaultExecutor(ctx, vaultID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "validate vault: %v", err)
	}
	return &gastrologv1.ForwardValidateVaultResponse{
		Valid:  resp.GetValid(),
		Chunks: resp.GetChunks(),
	}, nil
}

// forwardGetChunk handles the ForwardGetChunk RPC. Returns details for a
// specific chunk in a local vault.
func (s *Server) forwardGetChunk(ctx context.Context, req *gastrologv1.ForwardGetChunkRequest) (*gastrologv1.ForwardGetChunkResponse, error) {
	if s.getChunkExecutor == nil {
		return nil, status.Error(codes.Unavailable, "get chunk executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	chunkID, err := parseChunkID(req.GetChunkId())
	if err != nil {
		return nil, err
	}
	meta, err := s.getChunkExecutor(ctx, vaultID, chunkID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get chunk: %v", err)
	}
	return &gastrologv1.ForwardGetChunkResponse{Chunk: meta}, nil
}

// forwardAnalyzeChunk handles the ForwardAnalyzeChunk RPC. Runs index analysis
// on a local vault (or specific chunk).
func (s *Server) forwardAnalyzeChunk(ctx context.Context, req *gastrologv1.ForwardAnalyzeChunkRequest) (*gastrologv1.ForwardAnalyzeChunkResponse, error) {
	if s.analyzeChunkExecutor == nil {
		return nil, status.Error(codes.Unavailable, "analyze chunk executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	analyses, err := s.analyzeChunkExecutor(ctx, vaultID, string(req.GetChunkId()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "analyze chunk: %v", err)
	}
	return &gastrologv1.ForwardAnalyzeChunkResponse{Analyses: analyses}, nil
}

// forwardSealVault handles the ForwardSealVault RPC. Seals the active chunk
// of a local vault.
func (s *Server) forwardSealVault(ctx context.Context, req *gastrologv1.ForwardSealVaultRequest) (*gastrologv1.ForwardSealVaultResponse, error) {
	if s.sealVaultExecutor == nil {
		return nil, status.Error(codes.Unavailable, "seal vault executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	if err := s.sealVaultExecutor(ctx, vaultID); err != nil {
		return nil, status.Errorf(codes.Internal, "seal vault: %v", err)
	}
	return &gastrologv1.ForwardSealVaultResponse{}, nil
}

// SealTierExecutor seals a specific tier's active chunk on this node.
// Invoked by the TierReplication stream handler.
type SealTierExecutor func(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error

// SetSealTierExecutor injects the callback for handling TierReplicationSeal commands.
func (s *Server) SetSealTierExecutor(fn SealTierExecutor) {
	s.sealTierExecutor = fn
}

// DeleteChunkExecutor deletes a specific sealed chunk from a tier on this node.
// Invoked by the TierReplication stream handler.
type DeleteChunkExecutor func(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error

// SetDeleteChunkExecutor injects the callback for handling TierReplicationDelete commands.
func (s *Server) SetDeleteChunkExecutor(fn DeleteChunkExecutor) {
	s.deleteChunkExecutor = fn
}

// forwardReindexVault handles the ForwardReindexVault RPC. Rebuilds all indexes
// for a local vault.
func (s *Server) forwardReindexVault(ctx context.Context, req *gastrologv1.ForwardReindexVaultRequest) (*gastrologv1.ForwardReindexVaultResponse, error) {
	if s.reindexVaultExecutor == nil {
		return nil, status.Error(codes.Unavailable, "reindex vault executor not configured")
	}
	vaultID, err := parseVaultID(req.GetVaultId())
	if err != nil {
		return nil, err
	}
	jobID, err := s.reindexVaultExecutor(ctx, vaultID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reindex vault: %v", err)
	}
	return &gastrologv1.ForwardReindexVaultResponse{JobId: []byte(jobID)}, nil
}

// forwardExportToVault handles the ForwardExportToVault RPC. Runs an
// export-to-vault job on a local vault.
func (s *Server) forwardExportToVault(ctx context.Context, req *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error) {
	if s.exportToVaultExecutor == nil {
		return nil, status.Error(codes.Unavailable, "export to vault executor not configured")
	}
	vaultID := glid.FromBytes(req.GetTargetVaultId())
	jobID, err := s.exportToVaultExecutor(ctx, req.GetExpression(), vaultID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "export to vault: %v", err)
	}
	return &gastrologv1.ForwardExportToVaultResponse{JobId: []byte(jobID)}, nil
}

// forwardExplain handles the ForwardExplain RPC. Returns the explain plan
// for the requested local vaults.
func (s *Server) forwardExplain(ctx context.Context, req *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	if s.explainExecutor == nil {
		return nil, status.Error(codes.Unavailable, "explain executor not configured")
	}
	vaultIDs := make([]glid.GLID, 0, len(req.GetVaultIds()))
	for _, vs := range req.GetVaultIds() {
		vid := glid.FromBytes(vs)
		vaultIDs = append(vaultIDs, vid)
	}
	chunks, totalChunks, err := s.explainExecutor(ctx, vaultIDs, req.GetQuery())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "explain: %v", err)
	}
	return &gastrologv1.ForwardExplainResponse{
		Chunks:      chunks,
		TotalChunks: totalChunks,
	}, nil
}

// forwardApply handles the ForwardApply RPC on the leader.
// Followers call this to proxy config writes through the leader's raft.Apply().
func (s *Server) forwardApply(ctx context.Context, req *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error) {
	if s.applyFn == nil {
		return nil, status.Error(codes.Unavailable, "apply function not configured")
	}
	if err := s.applyFn(ctx, req.GetCommand()); err != nil {
		return nil, status.Errorf(codes.Internal, "apply: %v", err)
	}
	return &gastrologv1.ForwardApplyResponse{}, nil
}

// forwardTierApply handles the ForwardTierApply RPC. The payload is a
// tierfsm command wrapped with OpTierFSM + tier ID (see
// NewVaultCtlTierApplyForwarder), and the group ID is the vault-ctl
// Raft group ID. Dispatches to the shared groupApplyFn.
func (s *Server) forwardTierApply(ctx context.Context, req *gastrologv1.ForwardTierApplyRequest) (*gastrologv1.ForwardTierApplyResponse, error) {
	if s.groupApplyFn == nil {
		return nil, status.Error(codes.Unavailable, "group apply function not configured")
	}
	if err := s.groupApplyFn(ctx, string(req.GetGroupId()), req.GetCommand()); err != nil {
		return nil, status.Errorf(codes.Internal, "tier apply: %v", err)
	}
	return &gastrologv1.ForwardTierApplyResponse{}, nil
}

// forwardVaultApply handles the ForwardVaultApply RPC on the vault
// control-plane Raft leader. The payload is a native vault-ctl FSM
// command (no OpTierFSM wrapping); the group ID identifies the vault's
// control-plane Raft group. Dispatches to the shared groupApplyFn.
func (s *Server) forwardVaultApply(ctx context.Context, req *gastrologv1.ForwardVaultApplyRequest) (*gastrologv1.ForwardVaultApplyResponse, error) {
	if s.groupApplyFn == nil {
		return nil, status.Error(codes.Unavailable, "group apply function not configured")
	}
	if err := s.groupApplyFn(ctx, string(req.GetGroupId()), req.GetCommand()); err != nil {
		return nil, status.Errorf(codes.Internal, "vault apply: %v", err)
	}
	return &gastrologv1.ForwardVaultApplyResponse{}, nil
}

// forwardRemoveNode handles the ForwardRemoveNode RPC on the leader.
// Followers call this to proxy node removal through the leader.
func (s *Server) forwardRemoveNode(ctx context.Context, req *gastrologv1.ForwardRemoveNodeRequest) (*gastrologv1.ForwardRemoveNodeResponse, error) {
	if s.removeNodeFn == nil {
		return nil, status.Error(codes.Unavailable, "remove node not configured")
	}
	if err := s.removeNodeFn(ctx, string(req.GetNodeId())); err != nil {
		return nil, status.Errorf(codes.Internal, "remove node: %v", err)
	}
	return &gastrologv1.ForwardRemoveNodeResponse{}, nil
}

// forwardSetNodeSuffrage handles the ForwardSetNodeSuffrage RPC on the leader.
// Followers call this to proxy suffrage changes through the leader.
func (s *Server) forwardSetNodeSuffrage(ctx context.Context, req *gastrologv1.ForwardSetNodeSuffrageRequest) (*gastrologv1.ForwardSetNodeSuffrageResponse, error) {
	if s.setNodeSuffrageFn == nil {
		return nil, status.Error(codes.Unavailable, "set node suffrage not configured")
	}
	if err := s.setNodeSuffrageFn(ctx, string(req.GetNodeId()), req.GetNodeAddr(), req.GetVoter()); err != nil {
		return nil, status.Errorf(codes.Internal, "set node suffrage: %v", err)
	}
	return &gastrologv1.ForwardSetNodeSuffrageResponse{}, nil
}

// notifyEviction handles the NotifyEviction RPC — tells this node it has been
// removed from the cluster. The eviction handler (if registered) is called
// asynchronously so the RPC can return before shutdown begins.
func (s *Server) notifyEviction(_ context.Context, req *gastrologv1.NotifyEvictionRequest) (*gastrologv1.NotifyEvictionResponse, error) {
	s.logger.Warn("received eviction notification", "reason", req.GetReason())
	if s.evictionHandler != nil {
		go s.evictionHandler()
	}
	return &gastrologv1.NotifyEvictionResponse{}, nil
}

// listPeerManagedFiles handles the unary ListPeerManagedFiles RPC.
// Returns the file IDs of managed files present on this node's disk.
func (s *Server) listPeerManagedFiles(_ context.Context, _ *gastrologv1.ListPeerManagedFilesRequest) (*gastrologv1.ListPeerManagedFilesResponse, error) {
	if s.managedFileIDs == nil {
		return &gastrologv1.ListPeerManagedFilesResponse{}, nil
	}
	ids := s.managedFileIDs()
	bids := make([][]byte, len(ids))
	for i, id := range ids {
		bids[i] = []byte(id)
	}
	return &gastrologv1.ListPeerManagedFilesResponse{FileIds: bids}, nil
}

// pullManagedFileStreamHandler handles the server-streaming PullManagedFile RPC.
// Reads the requested managed file from disk and streams it back in 64KB chunks.
func pullManagedFileStreamHandler(srv any, stream grpc.ServerStream) error {
	s := srv.(*Server)
	if s.managedFileReader == nil {
		return status.Error(codes.Unavailable, "managed file reader not configured")
	}

	req := &gastrologv1.PullManagedFileRequest{}
	if err := stream.RecvMsg(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "receive request: %v", err)
	}

	name, rc, sha256hex, err := s.managedFileReader(string(req.GetFileId()))
	if err != nil {
		return status.Errorf(codes.NotFound, "open managed file %s: %v", req.GetFileId(), err)
	}
	defer rc.Close() //nolint:errcheck // best-effort close

	// Send first chunk with metadata.
	buf := make([]byte, managedFileChunkSize)
	first := true
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			msg := &gastrologv1.PullManagedFileChunk{
				Data: buf[:n],
			}
			if first {
				msg.Name = name
				msg.Sha256 = sha256hex
				first = false
			}
			if err := stream.SendMsg(msg); err != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read file: %v", readErr)
		}
	}

	return nil
}

// clusterServiceDesc is a manually-defined gRPC ServiceDesc for
// gastrolog.v1.ClusterService. We register this manually rather than using
// protoc-gen-go-grpc to avoid generating unused gRPC stubs for all services
// in the proto package.
var clusterServiceDesc = grpc.ServiceDesc{
	ServiceName: "gastrolog.v1.ClusterService",
	HandlerType: (*clusterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ForwardApply",
			Handler:    forwardApplyHandler,
		},
		{
			MethodName: "Enroll",
			Handler:    enrollRPCHandler,
		},
		{
			MethodName: "Broadcast",
			Handler:    broadcastHandler,
		},
		{
			MethodName: "ForwardRecords",
			Handler:    forwardRecordsHandler,
		},
		{
			MethodName: "ForwardGetContext",
			Handler:    forwardGetContextHandler,
		},
		{
			MethodName: "ForwardListChunks",
			Handler:    forwardListChunksHandler,
		},
		{
			MethodName: "ForwardGetIndexes",
			Handler:    forwardGetIndexesHandler,
		},
		{
			MethodName: "ForwardValidateVault",
			Handler:    forwardValidateVaultHandler,
		},
		{
			MethodName: "NotifyEviction",
			Handler:    notifyEvictionHandler,
		},
		{
			MethodName: "ForwardRemoveNode",
			Handler:    forwardRemoveNodeHandler,
		},
		{
			MethodName: "ForwardSetNodeSuffrage",
			Handler:    forwardSetNodeSuffrageHandler,
		},
		{
			MethodName: "ForwardExplain",
			Handler:    forwardExplainHandler,
		},
		{
			MethodName: "ForwardGetChunk",
			Handler:    forwardGetChunkHandler,
		},
		{
			MethodName: "ForwardAnalyzeChunk",
			Handler:    forwardAnalyzeChunkHandler,
		},
		{
			MethodName: "ForwardSealVault",
			Handler:    forwardSealVaultHandler,
		},
		{
			MethodName: "ForwardReindexVault",
			Handler:    forwardReindexVaultHandler,
		},
		{
			MethodName: "ForwardExportToVault",
			Handler:    forwardExportToVaultHandler,
		},
		{
			MethodName: "ListPeerManagedFiles",
			Handler:    listPeerManagedFilesHandler,
		},
		{
			MethodName: "ForwardTierApply",
			Handler:    forwardTierApplyHandler,
		},
		{
			MethodName: "ForwardVaultApply",
			Handler:    forwardVaultApplyHandler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "TierReplication",
			Handler:       tierReplicationStreamHandler,
			ClientStreams: true,
			ServerStreams: true,
		},
		{
			StreamName:    "ForwardImportRecords",
			Handler:       forwardImportRecordsStreamHandler,
			ClientStreams: true,
		},
		{
			StreamName:    "ForwardSearch",
			Handler:       forwardSearchStreamHandler,
			ServerStreams: true,
		},
		{
			StreamName:    "ForwardFollow",
			Handler:       forwardFollowStreamHandler,
			ServerStreams: true,
		},
		{
			StreamName:    "PullManagedFile",
			Handler:       pullManagedFileStreamHandler,
			ServerStreams: true,
		},
		{
			StreamName:    "StreamForwardRecords",
			Handler:       streamForwardRecordsHandler,
			ClientStreams: true,
		},
		{
			StreamName:    "ForwardRPC",
			Handler:       forwardRPCStreamHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}

// clusterServiceServer is the interface the gRPC runtime uses for type-checking.
type clusterServiceServer interface {
	forwardApply(context.Context, *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error)
	enroll(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error)
	broadcast(context.Context, *gastrologv1.BroadcastRequest) (*gastrologv1.BroadcastResponse, error)
	forwardRecords(context.Context, *gastrologv1.ForwardRecordsRequest) (*gastrologv1.ForwardRecordsResponse, error)
	forwardGetContext(context.Context, *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error)
	forwardListChunks(context.Context, *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error)
	forwardGetIndexes(context.Context, *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error)
	forwardValidateVault(context.Context, *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error)
	notifyEviction(context.Context, *gastrologv1.NotifyEvictionRequest) (*gastrologv1.NotifyEvictionResponse, error)
	forwardRemoveNode(context.Context, *gastrologv1.ForwardRemoveNodeRequest) (*gastrologv1.ForwardRemoveNodeResponse, error)
	forwardSetNodeSuffrage(context.Context, *gastrologv1.ForwardSetNodeSuffrageRequest) (*gastrologv1.ForwardSetNodeSuffrageResponse, error)
	forwardExplain(context.Context, *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error)
	forwardGetChunk(context.Context, *gastrologv1.ForwardGetChunkRequest) (*gastrologv1.ForwardGetChunkResponse, error)
	forwardAnalyzeChunk(context.Context, *gastrologv1.ForwardAnalyzeChunkRequest) (*gastrologv1.ForwardAnalyzeChunkResponse, error)
	forwardSealVault(context.Context, *gastrologv1.ForwardSealVaultRequest) (*gastrologv1.ForwardSealVaultResponse, error)
	forwardReindexVault(context.Context, *gastrologv1.ForwardReindexVaultRequest) (*gastrologv1.ForwardReindexVaultResponse, error)
	forwardExportToVault(context.Context, *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error)
	listPeerManagedFiles(context.Context, *gastrologv1.ListPeerManagedFilesRequest) (*gastrologv1.ListPeerManagedFilesResponse, error)
}

func forwardApplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardApplyRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardApply(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardApply",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardApply(ctx, req.(*gastrologv1.ForwardApplyRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardTierApplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardTierApplyRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardTierApply(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardTierApply",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardTierApply(ctx, req.(*gastrologv1.ForwardTierApplyRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardVaultApplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardVaultApplyRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardVaultApply(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardVaultApply",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardVaultApply(ctx, req.(*gastrologv1.ForwardVaultApplyRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardRecordsHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardRecordsRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardRecords(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardRecords",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardRecords(ctx, req.(*gastrologv1.ForwardRecordsRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardGetContextHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardGetContextRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardGetContext(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardGetContext",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardGetContext(ctx, req.(*gastrologv1.ForwardGetContextRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardListChunksHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardListChunksRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardListChunks(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardListChunks",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardListChunks(ctx, req.(*gastrologv1.ForwardListChunksRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardGetIndexesHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardGetIndexesRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardGetIndexes(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardGetIndexes",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardGetIndexes(ctx, req.(*gastrologv1.ForwardGetIndexesRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardValidateVaultHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardValidateVaultRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardValidateVault(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardValidateVault",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardValidateVault(ctx, req.(*gastrologv1.ForwardValidateVaultRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardRemoveNodeHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardRemoveNodeRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardRemoveNode(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardRemoveNode",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardRemoveNode(ctx, req.(*gastrologv1.ForwardRemoveNodeRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardSetNodeSuffrageHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardSetNodeSuffrageRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardSetNodeSuffrage(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardSetNodeSuffrage",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardSetNodeSuffrage(ctx, req.(*gastrologv1.ForwardSetNodeSuffrageRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardExplainHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardExplainRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardExplain(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardExplain",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardExplain(ctx, req.(*gastrologv1.ForwardExplainRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardGetChunkHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardGetChunkRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardGetChunk(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardGetChunk",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardGetChunk(ctx, req.(*gastrologv1.ForwardGetChunkRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardAnalyzeChunkHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardAnalyzeChunkRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardAnalyzeChunk(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardAnalyzeChunk",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardAnalyzeChunk(ctx, req.(*gastrologv1.ForwardAnalyzeChunkRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardSealVaultHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardSealVaultRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardSealVault(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardSealVault",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardSealVault(ctx, req.(*gastrologv1.ForwardSealVaultRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardReindexVaultHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardReindexVaultRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardReindexVault(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardReindexVault",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardReindexVault(ctx, req.(*gastrologv1.ForwardReindexVaultRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func forwardExportToVaultHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ForwardExportToVaultRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.forwardExportToVault(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ForwardExportToVault",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.forwardExportToVault(ctx, req.(*gastrologv1.ForwardExportToVaultRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func notifyEvictionHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.NotifyEvictionRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.notifyEviction(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/NotifyEviction",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.notifyEviction(ctx, req.(*gastrologv1.NotifyEvictionRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func listPeerManagedFilesHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &gastrologv1.ListPeerManagedFilesRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	s := srv.(*Server)
	if interceptor == nil {
		return s.listPeerManagedFiles(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gastrolog.v1.ClusterService/ListPeerManagedFiles",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return s.listPeerManagedFiles(ctx, req.(*gastrologv1.ListPeerManagedFilesRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func registerClusterService(s *grpc.Server, srv *Server) {
	s.RegisterService(&clusterServiceDesc, srv)
}

// ForwardApplyClient is a client for the ForwardApply RPC.
type ForwardApplyClient struct {
	cc grpc.ClientConnInterface
}

// NewForwardApplyClient creates a client bound to a connection.
func NewForwardApplyClient(cc grpc.ClientConnInterface) *ForwardApplyClient {
	return &ForwardApplyClient{cc: cc}
}

// ForwardApply sends a config command to the leader.
func (c *ForwardApplyClient) ForwardApply(ctx context.Context, req *gastrologv1.ForwardApplyRequest) (*gastrologv1.ForwardApplyResponse, error) {
	out := &gastrologv1.ForwardApplyResponse{}
	if err := c.cc.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardApply", req, out); err != nil {
		return nil, err
	}
	return out, nil
}

// NotifyEvictionClient sends eviction notifications to a peer node.
type NotifyEvictionClient struct {
	cc grpc.ClientConnInterface
}

// NewNotifyEvictionClient creates a client bound to a connection.
func NewNotifyEvictionClient(cc grpc.ClientConnInterface) *NotifyEvictionClient {
	return &NotifyEvictionClient{cc: cc}
}

// NotifyEviction tells a peer node it has been evicted from the cluster.
func (c *NotifyEvictionClient) NotifyEviction(ctx context.Context, reason string) error {
	req := &gastrologv1.NotifyEvictionRequest{Reason: reason}
	out := &gastrologv1.NotifyEvictionResponse{}
	return c.cc.Invoke(ctx, "/gastrolog.v1.ClusterService/NotifyEviction", req, out)
}

// ForwardRemoveNodeClient forwards node removal to the leader via cluster gRPC.
type ForwardRemoveNodeClient struct {
	cc grpc.ClientConnInterface
}

// NewForwardRemoveNodeClient creates a client bound to a connection.
func NewForwardRemoveNodeClient(cc grpc.ClientConnInterface) *ForwardRemoveNodeClient {
	return &ForwardRemoveNodeClient{cc: cc}
}

// ForwardRemoveNode asks the leader to remove a node from the cluster.
func (c *ForwardRemoveNodeClient) ForwardRemoveNode(ctx context.Context, nodeID string) error {
	req := &gastrologv1.ForwardRemoveNodeRequest{NodeId: []byte(nodeID)}
	out := &gastrologv1.ForwardRemoveNodeResponse{}
	return c.cc.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardRemoveNode", req, out)
}

// ForwardSetNodeSuffrageClient forwards suffrage changes to the leader via cluster gRPC.
type ForwardSetNodeSuffrageClient struct {
	cc grpc.ClientConnInterface
}

// NewForwardSetNodeSuffrageClient creates a client bound to a connection.
func NewForwardSetNodeSuffrageClient(cc grpc.ClientConnInterface) *ForwardSetNodeSuffrageClient {
	return &ForwardSetNodeSuffrageClient{cc: cc}
}

// ForwardSetNodeSuffrage asks the leader to change a node's suffrage.
func (c *ForwardSetNodeSuffrageClient) ForwardSetNodeSuffrage(ctx context.Context, nodeID, nodeAddr string, voter bool) error {
	req := &gastrologv1.ForwardSetNodeSuffrageRequest{NodeId: []byte(nodeID), NodeAddr: nodeAddr, Voter: voter}
	out := &gastrologv1.ForwardSetNodeSuffrageResponse{}
	return c.cc.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardSetNodeSuffrage", req, out)
}
