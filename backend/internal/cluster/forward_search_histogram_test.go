package cluster

import (
	"context"
	"fmt"
	"iter"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// recordingServerStream delivers one request and captures every response the
// handler sends, in order.
type recordingServerStream struct {
	ctx  context.Context
	req  *gastrologv1.ForwardSearchRequest
	recv bool
	sent []*gastrologv1.ForwardSearchResponse
}

func (s *recordingServerStream) RecvMsg(m any) error {
	if s.recv {
		return fmt.Errorf("request already delivered")
	}
	dst, ok := m.(*gastrologv1.ForwardSearchRequest)
	if !ok {
		return fmt.Errorf("unexpected request type %T", m)
	}
	proto.Reset(dst)
	proto.Merge(dst, s.req)
	s.recv = true
	return nil
}

func (s *recordingServerStream) SendMsg(m any) error {
	msg, ok := m.(*gastrologv1.ForwardSearchResponse)
	if !ok {
		return fmt.Errorf("unexpected response type %T", m)
	}
	s.sent = append(s.sent, proto.Clone(msg).(*gastrologv1.ForwardSearchResponse))
	return nil
}

func (s *recordingServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *recordingServerStream) SendHeader(metadata.MD) error { return nil }
func (s *recordingServerStream) SetTrailer(metadata.MD)       {}
func (s *recordingServerStream) Context() context.Context     { return s.ctx }

func recordSeqOfN(n int) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for i := range n {
			if !yield(chunk.Record{Raw: fmt.Appendf(nil, "rec-%d", i)}, nil) {
				return
			}
		}
	}
}

// The whole fan-out rests on a remote search's histogram arriving complete in
// the stream's FIRST response and in no other: the coordinator publishes that
// message's histogram and never looks again, so a handler that moved the
// buckets to a later message — or split them across batches — would truncate
// every remote histogram in the cluster while every other test stayed green.
func TestForwardSearchSendsHistogramOnlyInTheFirstResponse(t *testing.T) {
	buckets := []*gastrologv1.HistogramBucket{
		{TimestampMs: 1000, Count: 5},
		{TimestampMs: 2000, Count: 9},
	}

	tests := []struct {
		name      string
		records   int
		table     *gastrologv1.TableResult
		wantSends int
	}{
		// Several batches: the handler must not re-attach the histogram to any
		// of the later ones.
		{name: "many record batches", records: 450, wantSends: 3},
		{name: "one partial batch", records: 5, wantSends: 1},
		{name: "empty result", records: 0, wantSends: 1},
		{name: "pipeline table", table: &gastrologv1.TableResult{Columns: []string{"count"}}, wantSends: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{}
			srv.SetSearchExecutor(func(context.Context, *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
				if tc.table != nil {
					return nil, nil, tc.table, buckets, nil
				}
				return recordSeqOfN(tc.records), nil, nil, buckets, nil
			})

			stream := &recordingServerStream{
				ctx: context.Background(),
				req: &gastrologv1.ForwardSearchRequest{VaultId: glid.New().ToProto()},
			}
			if err := forwardSearchStreamHandler(srv, stream); err != nil {
				t.Fatalf("handler: %v", err)
			}

			if len(stream.sent) != tc.wantSends {
				t.Fatalf("sent %d responses, want %d", len(stream.sent), tc.wantSends)
			}

			// Complete in the first response.
			first := stream.sent[0].GetHistogram()
			if len(first) != len(buckets) {
				t.Fatalf("first response carries %d buckets, want all %d", len(first), len(buckets))
			}
			for i := range buckets {
				if first[i].TimestampMs != buckets[i].TimestampMs || first[i].Count != buckets[i].Count {
					t.Errorf("bucket %d = (%d, %d), want (%d, %d)", i,
						first[i].TimestampMs, first[i].Count, buckets[i].TimestampMs, buckets[i].Count)
				}
			}

			// Absent from every later response.
			for i, resp := range stream.sent[1:] {
				if h := resp.GetHistogram(); len(h) != 0 {
					t.Errorf("response %d also carries %d histogram buckets; the coordinator reads only the first, so a split histogram is silently truncated", i+1, len(h))
				}
			}

			// Premise for the multi-batch case: there really are later
			// responses for the histogram to have leaked into.
			if tc.records > 200 && len(stream.sent) < 2 {
				t.Fatalf("expected more than one response for %d records", tc.records)
			}
		})
	}
}
