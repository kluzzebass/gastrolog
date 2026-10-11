package cluster

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func searchExecutorFailing(err error, inIterator bool) SearchExecutor {
	return func(context.Context, *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
		if !inIterator {
			return nil, nil, nil, nil, err
		}
		return func(yield func(chunk.Record, error) bool) { yield(chunk.Record{}, err) }, nil, nil, nil, nil
	}
}

// A peer whose share of a query overflows its memory budget tells the
// coordinator so: the error leaves the peer as ResourceExhausted, not as
// Internal with the type flattened into text. Any other failure stays
// Internal.
func TestForwardSearchReportsMemoryOverflowAsResourceExhausted(t *testing.T) {
	memErr := fmt.Errorf("vault v chunk c: %w", &query.MemoryLimitError{Consumer: "sort buffer", Limit: query.MaxQueryMemoryBytes})
	otherErr := errors.New("disk on fire")
	cases := []struct {
		name string
		exec SearchExecutor
		want codes.Code
	}{
		{"executor memory overflow", searchExecutorFailing(memErr, false), codes.ResourceExhausted},
		{"iterator memory overflow", searchExecutorFailing(memErr, true), codes.ResourceExhausted},
		{"executor other failure", searchExecutorFailing(otherErr, false), codes.Internal},
		{"iterator other failure", searchExecutorFailing(otherErr, true), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{}
			srv.SetSearchExecutor(tc.exec)
			stream := &recordingServerStream{
				ctx: context.Background(),
				req: &gastrologv1.ForwardSearchRequest{VaultId: glid.New().ToProto()},
			}
			err := forwardSearchStreamHandler(srv, stream)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}

// GetContext runs a query on the peer too, under the same mapping.
func TestForwardGetContextReportsMemoryOverflowAsResourceExhausted(t *testing.T) {
	memErr := &query.MemoryLimitError{Consumer: "record buffer", Limit: query.MaxQueryMemoryBytes}
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{memErr, codes.ResourceExhausted},
		{errors.New("disk on fire"), codes.Internal},
	} {
		srv := &Server{}
		srv.SetContextExecutor(func(context.Context, glid.GLID, chunk.ChunkID, uint64, int, int) ([]chunk.Record, chunk.Record, []chunk.Record, error) {
			return nil, chunk.Record{}, nil, tc.err
		})
		_, err := srv.forwardGetContext(context.Background(), &gastrologv1.ForwardGetContextRequest{
			VaultId: glid.New().ToProto(),
			ChunkId: glid.GLID(chunk.NewChunkID()).ToProto(),
		})
		if got := status.Code(err); got != tc.want {
			t.Errorf("%v: code = %v, want %v (err: %v)", tc.err, got, tc.want, err)
		}
	}
}
