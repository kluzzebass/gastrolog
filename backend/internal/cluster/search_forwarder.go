package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
)

const searchForwarderPurpose = PurposeSearch

// SearchForwarder sends search requests to remote cluster nodes.
type SearchForwarder struct {
	peers *PeerConnManager
}

// NewSearchForwarder creates a SearchForwarder using the peer connection manager.
func NewSearchForwarder(peers *PeerConnManager) *SearchForwarder {
	return &SearchForwarder{peers: peers}
}

func (sf *SearchForwarder) invoke(ctx context.Context, nodeID, method string, req, resp any, action string) error {
	if err := sf.peers.InvokeService(ctx, nodeID, searchForwarderPurpose, method, req, resp); err != nil {
		return fmt.Errorf("%s to %s: %w", action, nodeID, err)
	}
	return nil
}

// Search sends a ForwardSearch RPC to the given node and collects the full
// streamed response into a single ForwardSearchResponse.
func (sf *SearchForwarder) Search(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	h, stream, err := sf.peers.OpenServiceStream(ctx, nodeID, searchForwarderPurpose,
		&grpc.StreamDesc{StreamName: "ForwardSearch", ServerStreams: true},
		"/gastrolog.v1.ClusterService/ForwardSearch",
	)
	if err != nil {
		return nil, fmt.Errorf("open search stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	if err := stream.SendMsg(req); err != nil {
		h.Invalidate(err)
		return nil, fmt.Errorf("send search request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	merged := &gastrologv1.ForwardSearchResponse{}
	for {
		msg := &gastrologv1.ForwardSearchResponse{}
		if err := stream.RecvMsg(msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			h.Invalidate(err)
			return nil, fmt.Errorf("search stream from %s: %w", nodeID, err)
		}
		merged.Records = append(merged.Records, msg.GetRecords()...)
		if msg.GetTableResult() != nil {
			merged.TableResult = msg.GetTableResult()
		}
		if msg.GetHistogram() != nil {
			merged.Histogram = msg.GetHistogram()
		}
	}
	return merged, nil
}

// SearchStream opens a server-streaming ForwardSearch RPC and returns the
// results via channels.
func (sf *SearchForwarder) SearchStream(ctx context.Context, nodeID string, req *gastrologv1.ForwardSearchRequest) (
	records <-chan []*gastrologv1.ExportRecord,
	histogram []*gastrologv1.HistogramBucket,
	tableResult *gastrologv1.TableResult,
	errCh <-chan error,
	getResumeToken func() []byte,
) {
	var resumeToken []byte
	getResumeToken = func() []byte { return resumeToken }
	recCh := make(chan []*gastrologv1.ExportRecord, 16)
	eCh := make(chan error, 1)

	h, stream, err := sf.peers.OpenServiceStream(ctx, nodeID, searchForwarderPurpose,
		&grpc.StreamDesc{StreamName: "ForwardSearch", ServerStreams: true},
		"/gastrolog.v1.ClusterService/ForwardSearch",
	)
	if err != nil {
		eCh <- fmt.Errorf("open search stream to %s: %w", nodeID, err)
		close(recCh)
		close(eCh)
		return recCh, nil, nil, eCh, getResumeToken
	}

	if err := stream.SendMsg(req); err != nil {
		h.Invalidate(err)
		h.Release()
		eCh <- fmt.Errorf("send search request to %s: %w", nodeID, err)
		close(recCh)
		close(eCh)
		return recCh, nil, nil, eCh, getResumeToken
	}
	if err := stream.CloseSend(); err != nil {
		h.Release()
		eCh <- fmt.Errorf("close send to %s: %w", nodeID, err)
		close(recCh)
		close(eCh)
		return recCh, nil, nil, eCh, getResumeToken
	}

	first := &gastrologv1.ForwardSearchResponse{}
	if err := stream.RecvMsg(first); err != nil {
		if !errors.Is(err, io.EOF) {
			h.Invalidate(err)
			eCh <- fmt.Errorf("search stream from %s: %w", nodeID, err)
		}
		h.Release()
		close(recCh)
		close(eCh)
		return recCh, nil, nil, eCh, getResumeToken
	}
	histogram = first.GetHistogram()
	tableResult = first.GetTableResult()

	if tableResult != nil {
		h.Release()
		close(recCh)
		close(eCh)
		return recCh, histogram, tableResult, eCh, getResumeToken
	}

	if len(first.GetRecords()) > 0 {
		recCh <- first.GetRecords()
	}
	if len(first.GetResumeToken()) > 0 {
		resumeToken = first.GetResumeToken()
	}

	go func() {
		defer h.Release()
		defer close(recCh)
		defer close(eCh)
		for {
			msg := &gastrologv1.ForwardSearchResponse{}
			if err := stream.RecvMsg(msg); err != nil {
				if !errors.Is(err, io.EOF) {
					h.Invalidate(err)
					eCh <- fmt.Errorf("search stream from %s: %w", nodeID, err)
				}
				return
			}
			if len(msg.GetRecords()) > 0 {
				select {
				case recCh <- msg.GetRecords():
				case <-ctx.Done():
					return
				}
			}
			if len(msg.GetResumeToken()) > 0 {
				resumeToken = msg.GetResumeToken()
			}
		}
	}()

	return recCh, histogram, tableResult, eCh, getResumeToken
}

func (sf *SearchForwarder) GetContext(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	resp := &gastrologv1.ForwardGetContextResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardGetContext", req, resp, "forward get context"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) ListChunks(ctx context.Context, nodeID string, req *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error) {
	resp := &gastrologv1.ForwardListChunksResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardListChunks", req, resp, "forward list chunks"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) GetPipelineBacklogDisk(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetPipelineBacklogRequest) (*gastrologv1.ForwardGetPipelineBacklogResponse, error) {
	resp := &gastrologv1.ForwardGetPipelineBacklogResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardGetPipelineBacklog", req, resp, "forward pipeline backlog"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) GetChunk(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetChunkRequest) (*gastrologv1.ForwardGetChunkResponse, error) {
	resp := &gastrologv1.ForwardGetChunkResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardGetChunk", req, resp, "forward get chunk"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) GetIndexes(ctx context.Context, nodeID string, req *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error) {
	resp := &gastrologv1.ForwardGetIndexesResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardGetIndexes", req, resp, "forward get indexes"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) AnalyzeChunk(ctx context.Context, nodeID string, req *gastrologv1.ForwardAnalyzeChunkRequest) (*gastrologv1.ForwardAnalyzeChunkResponse, error) {
	resp := &gastrologv1.ForwardAnalyzeChunkResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardAnalyzeChunk", req, resp, "forward analyze chunk"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) ValidateVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error) {
	resp := &gastrologv1.ForwardValidateVaultResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardValidateVault", req, resp, "forward validate vault"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) SealVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardSealVaultRequest) (*gastrologv1.ForwardSealVaultResponse, error) {
	resp := &gastrologv1.ForwardSealVaultResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardSealVault", req, resp, "forward seal vault"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) ReindexVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardReindexVaultRequest) (*gastrologv1.ForwardReindexVaultResponse, error) {
	resp := &gastrologv1.ForwardReindexVaultResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardReindexVault", req, resp, "forward reindex vault"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) Explain(ctx context.Context, nodeID string, req *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	resp := &gastrologv1.ForwardExplainResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardExplain", req, resp, "forward explain"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) Follow(ctx context.Context, nodeID string, req *gastrologv1.ForwardFollowRequest) (<-chan *gastrologv1.ExportRecord, <-chan error) {
	recCh := make(chan *gastrologv1.ExportRecord, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(recCh)
		defer close(errCh)

		h, stream, err := sf.peers.OpenServiceStream(ctx, nodeID, searchForwarderPurpose,
			&grpc.StreamDesc{StreamName: "ForwardFollow", ServerStreams: true},
			"/gastrolog.v1.ClusterService/ForwardFollow",
		)
		if err != nil {
			errCh <- fmt.Errorf("open follow stream to %s: %w", nodeID, err)
			return
		}
		defer h.Release()

		if err := stream.SendMsg(req); err != nil {
			h.Invalidate(err)
			errCh <- fmt.Errorf("send follow request to %s: %w", nodeID, err)
			return
		}
		if err := stream.CloseSend(); err != nil {
			errCh <- fmt.Errorf("close send to %s: %w", nodeID, err)
			return
		}

		for {
			resp := &gastrologv1.ForwardFollowResponse{}
			if err := stream.RecvMsg(resp); err != nil {
				if !errors.Is(err, io.EOF) {
					h.Invalidate(err)
					errCh <- fmt.Errorf("follow stream from %s: %w", nodeID, err)
				}
				return
			}
			for _, rec := range resp.GetRecords() {
				select {
				case recCh <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return recCh, errCh
}

func (sf *SearchForwarder) ExportToVault(ctx context.Context, nodeID string, req *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error) {
	resp := &gastrologv1.ForwardExportToVaultResponse{}
	if err := sf.invoke(ctx, nodeID, "/gastrolog.v1.ClusterService/ForwardExportToVault", req, resp, "forward export to vault"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (sf *SearchForwarder) WatchChunks(ctx context.Context, nodeID string, onEvent func(*gastrologv1.ForwardWatchChunksResponse) error) error {
	h, stream, err := sf.peers.OpenServiceStream(ctx, nodeID, searchForwarderPurpose,
		&grpc.StreamDesc{StreamName: "ForwardWatchChunks", ServerStreams: true},
		"/gastrolog.v1.ClusterService/ForwardWatchChunks",
	)
	if err != nil {
		return fmt.Errorf("open watchchunks stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	if err := stream.SendMsg(&gastrologv1.ForwardWatchChunksRequest{}); err != nil {
		h.Invalidate(err)
		return fmt.Errorf("send watchchunks request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close watchchunks send to %s: %w", nodeID, err)
	}
	for {
		msg := &gastrologv1.ForwardWatchChunksResponse{}
		if err := stream.RecvMsg(msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.Invalidate(err)
			return fmt.Errorf("watchchunks stream from %s: %w", nodeID, err)
		}
		if err := onEvent(msg); err != nil {
			return err
		}
	}
}

func (sf *SearchForwarder) Close() error { return nil }
