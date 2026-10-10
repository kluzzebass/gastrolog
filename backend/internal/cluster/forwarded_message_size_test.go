package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// grpcDefaultRecvBytes is gRPC's default client receive cap — the size a
// forwarded response has to exceed to prove it does not depend on it.
const grpcDefaultRecvBytes = 4 << 20

// forwardingPeer starts a real cluster gRPC server for node "owner",
// configured by setup, and returns a SearchForwarder and PeerConnManager on
// node "caller" that reach it over loopback.
func forwardingPeer(t *testing.T, setup func(*Server)) (*SearchForwarder, *PeerConnManager) {
	t.Helper()
	peer, err := New(Config{ClusterAddr: "127.0.0.1:0", NodeID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	peer.Transport()
	setup(peer)
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Stop)
	peers := NewStaticPeerConns("caller", func(id string) (string, bool) {
		return peer.Addr(), id == "owner"
	})
	t.Cleanup(func() { _ = peers.Close() })
	return NewSearchForwarder(peers), peers
}

// sizedRecords returns n records whose raw bodies are size bytes each, every
// body distinct and naming its position.
func sizedRecords(n, size int) []chunk.Record {
	records := make([]chunk.Record, n)
	for i := range records {
		raw := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		copy(raw, fmt.Sprintf("rec-%06d|", i))
		records[i] = chunk.Record{Raw: raw}
	}
	return records
}

func recordSeq(records []chunk.Record) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for _, rec := range records {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// servingRecords configures the peer to answer every ForwardSearch with
// records.
func servingRecords(records []chunk.Record) func(*Server) {
	return func(s *Server) {
		s.SetSearchExecutor(func(context.Context, *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
			return recordSeq(records), nil, nil, nil, nil
		})
	}
}

func forwardSearchRequest() *gastrologv1.ForwardSearchRequest {
	return &gastrologv1.ForwardSearchRequest{VaultId: glid.New().ToProto()}
}

// requireRecordsInOrder fails unless got carries exactly want's raw bodies,
// each once, in order.
func requireRecordsInOrder(t *testing.T, got []*gastrologv1.ExportRecord, want []chunk.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("received %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].GetRaw(), want[i].Raw) {
			t.Fatalf("record %d: got %d bytes starting %.12q, want %d bytes starting %.12q",
				i, len(got[i].GetRaw()), got[i].GetRaw(), len(want[i].Raw), want[i].Raw)
		}
	}
}

// drainSearchStream collects every batch of a SearchStream and its error.
func drainSearchStream(t *testing.T, sf *SearchForwarder) ([][]*gastrologv1.ExportRecord, error) {
	t.Helper()
	recCh, _, errCh, _, _ := sf.SearchStream(context.Background(), "owner", forwardSearchRequest())
	var batches [][]*gastrologv1.ExportRecord
	for batch := range recCh {
		batches = append(batches, batch)
	}
	return batches, <-errCh
}

func flatten(batches [][]*gastrologv1.ExportRecord) []*gastrologv1.ExportRecord {
	var out []*gastrologv1.ExportRecord
	for _, b := range batches {
		out = append(out, b...)
	}
	return out
}

// requireBatchesWithinBounds fails unless every batch stays within the
// forwarded batch bounds; a lone record may exceed the byte bound.
func requireBatchesWithinBounds(t *testing.T, batches [][]*gastrologv1.ExportRecord) {
	t.Helper()
	for i, b := range batches {
		size := proto.Size(&gastrologv1.ForwardSearchResponse{Records: b})
		if len(b) > forwardBatchMaxRecords || (len(b) > 1 && size > forwardBatchMaxBytes) {
			t.Fatalf("batch %d holds %d records in %d bytes, bounds are %d records and %d bytes",
				i, len(b), size, forwardBatchMaxRecords, forwardBatchMaxBytes)
		}
	}
}

// TestForwardSearchBatchOverDefaultRecvCapArrivesWhole: 200 records of 25 KiB
// make one count-sized batch of over 4 MiB. Every record reaches the
// coordinating node once and in order, over both the streaming and the
// collecting client, in messages within the forwarded batch bounds.
func TestForwardSearchBatchOverDefaultRecvCapArrivesWhole(t *testing.T) {
	records := sizedRecords(200, 25<<10)
	sf, _ := forwardingPeer(t, servingRecords(records))

	t.Run("stream", func(t *testing.T) {
		batches, err := drainSearchStream(t, sf)
		if err != nil {
			t.Fatalf("search stream: %v", err)
		}
		requireRecordsInOrder(t, flatten(batches), records)
		requireBatchesWithinBounds(t, batches)
	})
	t.Run("collect", func(t *testing.T) {
		resp, err := sf.Search(context.Background(), "owner", forwardSearchRequest())
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		requireRecordsInOrder(t, resp.GetRecords(), records)
	})
}

// TestForwardSearchSmallRecordsKeepCountBound: records far below the byte
// bound still travel forwardBatchMaxRecords to a message.
func TestForwardSearchSmallRecordsKeepCountBound(t *testing.T) {
	records := sizedRecords(2*forwardBatchMaxRecords+50, 64)
	sf, _ := forwardingPeer(t, servingRecords(records))

	batches, err := drainSearchStream(t, sf)
	if err != nil {
		t.Fatalf("search stream: %v", err)
	}
	requireRecordsInOrder(t, flatten(batches), records)
	var sizes []int
	for _, b := range batches {
		sizes = append(sizes, len(b))
	}
	if want := []int{forwardBatchMaxRecords, forwardBatchMaxRecords, 50}; !slices.Equal(sizes, want) {
		t.Fatalf("batch sizes %v, want %v", sizes, want)
	}
}

// largeRecordAmongSmall returns small records around one record larger than
// the 10 MiB request body the HTTP and OTLP ingesters accept.
func largeRecordAmongSmall() []chunk.Record {
	records := sizedRecords(7, 1<<10)
	large := sizedRecords(1, 12<<20)[0]
	copy(large.Raw, "rec-large|")
	return slices.Insert(records, 3, large)
}

// TestForwardSearchSingleLargeRecordArrives: a record larger than any batch
// bound and larger than gRPC's default receive cap reaches the coordinating
// node whole, between its neighbours.
func TestForwardSearchSingleLargeRecordArrives(t *testing.T) {
	records := largeRecordAmongSmall()
	sf, _ := forwardingPeer(t, servingRecords(records))

	t.Run("stream", func(t *testing.T) {
		batches, err := drainSearchStream(t, sf)
		if err != nil {
			t.Fatalf("search stream: %v", err)
		}
		requireRecordsInOrder(t, flatten(batches), records)
		requireBatchesWithinBounds(t, batches)
	})
	t.Run("collect", func(t *testing.T) {
		resp, err := sf.Search(context.Background(), "owner", forwardSearchRequest())
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		requireRecordsInOrder(t, resp.GetRecords(), records)
	})
}

// TestForwardFollowSingleLargeRecordArrives: follow streams one record per
// message, so a record over gRPC's default receive cap arrives whole and in
// order among the others.
func TestForwardFollowSingleLargeRecordArrives(t *testing.T) {
	records := largeRecordAmongSmall()
	sf, _ := forwardingPeer(t, func(s *Server) {
		s.SetFollowExecutor(func(context.Context, []glid.GLID, string) (iter.Seq2[chunk.Record, error], error) {
			return recordSeq(records), nil
		})
	})

	recCh, errCh := sf.Follow(context.Background(), "owner", &gastrologv1.ForwardFollowRequest{})
	var got []*gastrologv1.ExportRecord
	for rec := range recCh {
		got = append(got, rec)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("follow: %v", err)
	}
	requireRecordsInOrder(t, got, records)
}

// largeTable returns a table whose rows encode to well over gRPC's default
// receive cap.
func largeTable() *gastrologv1.TableResult {
	table := &gastrologv1.TableResult{
		Columns:    []string{"k", "count"},
		Truncated:  true,
		ResultType: "table",
	}
	for i := range 6000 {
		table.Rows = append(table.Rows, &gastrologv1.TableRow{
			Values: []string{fmt.Sprintf("%05d-%s", i, strings.Repeat("k", 1<<10)), fmt.Sprint(i)},
		})
	}
	return table
}

// TestForwardSearchLargeTableArrivesWhole: a pipeline table over gRPC's
// default receive cap reaches the collecting client with its columns, flags,
// histogram and every row in order.
func TestForwardSearchLargeTableArrivesWhole(t *testing.T) {
	table := largeTable()
	if n := proto.Size(table); n <= grpcDefaultRecvBytes {
		t.Fatalf("table encodes to %d bytes, not over the %d-byte default cap", n, grpcDefaultRecvBytes)
	}
	histogram := []*gastrologv1.HistogramBucket{{TimestampMs: 1000, Count: 6000}}
	sf, _ := forwardingPeer(t, func(s *Server) {
		s.SetSearchExecutor(func(context.Context, *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
			return nil, nil, table, histogram, nil
		})
	})

	resp, err := sf.Search(context.Background(), "owner", forwardSearchRequest())
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got := resp.GetTableResult(); !proto.Equal(got, table) {
		t.Fatalf("table arrived with columns %v, truncated %v, type %q and %d rows; want %v, %v, %q and %d rows",
			got.GetColumns(), got.GetTruncated(), got.GetResultType(), len(got.GetRows()),
			table.GetColumns(), table.GetTruncated(), table.GetResultType(), len(table.GetRows()))
	}
	if len(resp.GetHistogram()) != 1 || resp.GetHistogram()[0].GetCount() != 6000 {
		t.Fatalf("histogram = %v, want the one bucket sent", resp.GetHistogram())
	}
}

// captureSearchStream is a grpc.ServerStream that records every
// ForwardSearchResponse the handler sends.
type captureSearchStream struct {
	sent []*gastrologv1.ForwardSearchResponse
}

func (s *captureSearchStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureSearchStream) SendHeader(metadata.MD) error { return nil }
func (s *captureSearchStream) SetTrailer(metadata.MD)       {}
func (s *captureSearchStream) Context() context.Context     { return context.Background() }
func (s *captureSearchStream) RecvMsg(any) error            { return io.EOF }
func (s *captureSearchStream) SendMsg(m any) error {
	s.sent = append(s.sent, m.(*gastrologv1.ForwardSearchResponse))
	return nil
}

// TestSendForwardedTableSplitsRowsByBytes: the table travels in messages of at
// most forwardBatchMaxBytes, only the first carrying columns, flags and
// histogram, and the rows concatenate back to the original in order.
func TestSendForwardedTableSplitsRowsByBytes(t *testing.T) {
	table := largeTable()
	histogram := []*gastrologv1.HistogramBucket{{TimestampMs: 1000, Count: 1}}
	stream := &captureSearchStream{}
	if err := sendForwardedTable(stream, table, histogram); err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) < 2 {
		t.Fatalf("table sent in %d message(s), want it split", len(stream.sent))
	}
	var rows []*gastrologv1.TableRow
	for i, msg := range stream.sent {
		if n := proto.Size(msg); n > forwardBatchMaxBytes {
			t.Fatalf("message %d is %d bytes, over forwardBatchMaxBytes %d", i, n, forwardBatchMaxBytes)
		}
		first := i == 0
		if first != (len(msg.GetHistogram()) > 0) || first != (len(msg.GetTableResult().GetColumns()) > 0) {
			t.Fatalf("message %d: histogram %v, columns %v; only the first message carries them",
				i, msg.GetHistogram(), msg.GetTableResult().GetColumns())
		}
		rows = append(rows, msg.GetTableResult().GetRows()...)
	}
	if !proto.Equal(&gastrologv1.TableResult{Rows: rows}, &gastrologv1.TableResult{Rows: table.GetRows()}) {
		t.Fatalf("rows reassemble to %d rows that differ from the %d sent", len(rows), len(table.GetRows()))
	}
}

// TestForwardListChunksOverDefaultRecvCapArrives: a unary forwarded list whose
// response exceeds gRPC's default receive cap — a vault with tens of
// thousands of chunks — arrives whole.
func TestForwardListChunksOverDefaultRecvCapArrives(t *testing.T) {
	vaultID := glid.New()
	metas := make([]*gastrologv1.ChunkMeta, 60000)
	for i := range metas {
		metas[i] = &gastrologv1.ChunkMeta{
			Id:             glid.New().ToProto(),
			VaultId:        vaultID.ToProto(),
			RecordCount:    int64(i),
			Bytes:          int64(i) << 10,
			Sealed:         true,
			VaultType:      "file",
			ReplicaNodeIds: []string{"node-one", "node-two", "node-three"},
		}
	}
	want := &gastrologv1.ForwardListChunksResponse{Chunks: metas}
	if n := proto.Size(want); n <= grpcDefaultRecvBytes {
		t.Fatalf("list encodes to %d bytes, not over the %d-byte default cap", n, grpcDefaultRecvBytes)
	}
	sf, _ := forwardingPeer(t, func(s *Server) {
		s.SetListChunksExecutor(func(context.Context, glid.GLID) ([]*gastrologv1.ChunkMeta, error) {
			return metas, nil
		})
	})

	got, err := sf.ListChunks(context.Background(), "owner", &gastrologv1.ForwardListChunksRequest{VaultId: vaultID.ToProto()})
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("received %d chunks that differ from the %d sent", len(got.GetChunks()), len(metas))
	}
}

// serviceConnIDs returns the IDs of the pooled service-lane connections.
func serviceConnIDs(peers *PeerConnManager) []uint64 {
	var ids []uint64
	for _, s := range peers.Snapshot() {
		if s.Lane == LaneService.String() {
			ids = append(ids, s.ConnID)
		}
	}
	return ids
}

// TestForwardSizeErrorKeepsPooledConnection: a message over the receiver's
// cap fails only its own call. The forwarders hand every receive error to
// Invalidate, and a size error must not tear down the healthy pooled
// connection the next forwarded call reuses.
func TestForwardSizeErrorKeepsPooledConnection(t *testing.T) {
	records := sizedRecords(3, 25<<10)
	sf, peers := forwardingPeer(t, servingRecords(records))

	h, stream, err := peers.OpenServiceStream(context.Background(), "owner", searchForwarderPurpose,
		&grpc.StreamDesc{StreamName: "ForwardSearch", ServerStreams: true},
		"/gastrolog.v1.ClusterService/ForwardSearch",
		grpc.MaxCallRecvMsgSize(1<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	before := serviceConnIDs(peers)
	if len(before) != 1 {
		t.Fatalf("service connections before = %v, want one", before)
	}
	if err := stream.SendMsg(forwardSearchRequest()); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	recvErr := stream.RecvMsg(&gastrologv1.ForwardSearchResponse{})
	if status.Code(recvErr) != codes.ResourceExhausted {
		t.Fatalf("recv error = %v, want ResourceExhausted", recvErr)
	}
	h.Invalidate(recvErr)
	h.Release()

	if after := serviceConnIDs(peers); !slices.Equal(after, before) {
		t.Fatalf("service connections after the size error = %v, want %v kept", after, before)
	}
	resp, err := sf.Search(context.Background(), "owner", forwardSearchRequest())
	if err != nil {
		t.Fatalf("search after the size error: %v", err)
	}
	requireRecordsInOrder(t, resp.GetRecords(), records)
	if after := serviceConnIDs(peers); !slices.Equal(after, before) {
		t.Fatalf("service connections after the next search = %v, want %v reused", after, before)
	}
}

// TestForwardSearchRecordOverServiceLaneLimitIsRefusedByName: a record that
// cannot fit in any message the coordinating node receives fails the search
// with an error naming the limit, and leaves the connection serving the next
// search.
func TestForwardSearchRecordOverServiceLaneLimitIsRefusedByName(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a record over the 128 MiB service-lane limit")
	}
	small := sizedRecords(2, 64)
	withHuge := []chunk.Record{small[0], {Raw: make([]byte, maxServiceLaneMsgBytes)}, small[1]}
	sf, peers := forwardingPeer(t, func(s *Server) {
		s.SetSearchExecutor(func(_ context.Context, req *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
			if req.GetQuery() == "huge" {
				return recordSeq(withHuge), nil, nil, nil, nil
			}
			return recordSeq(small), nil, nil, nil, nil
		})
	})
	search := func(query string) (*gastrologv1.ForwardSearchResponse, error) {
		req := forwardSearchRequest()
		req.Query = query
		return sf.Search(context.Background(), "owner", req)
	}
	if _, err := search(""); err != nil {
		t.Fatalf("warm-up search: %v", err)
	}
	before := serviceConnIDs(peers)

	_, err := search("huge")
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "maxServiceLaneMsgBytes") {
		t.Fatalf("search error = %v, want ResourceExhausted naming maxServiceLaneMsgBytes", err)
	}

	resp, err := search("")
	if err != nil {
		t.Fatalf("search after the refusal: %v", err)
	}
	requireRecordsInOrder(t, resp.GetRecords(), small)
	if after := serviceConnIDs(peers); !slices.Equal(after, before) {
		t.Fatalf("service connections = %v after the refusal, want %v kept", after, before)
	}
}
