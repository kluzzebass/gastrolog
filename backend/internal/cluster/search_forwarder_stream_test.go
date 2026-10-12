package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// scriptedStream hands out a fixed sequence of ForwardSearch responses.
type scriptedStream struct {
	ctx  context.Context
	msgs []*gastrologv1.ForwardSearchResponse
	next int
	// failAfterScript, when set, is returned instead of io.EOF once the
	// scripted messages run out.
	failAfterScript error
}

func (s *scriptedStream) RecvMsg(m any) error {
	if s.next >= len(s.msgs) {
		if s.failAfterScript != nil {
			return s.failAfterScript
		}
		return io.EOF
	}
	dst, ok := m.(*gastrologv1.ForwardSearchResponse)
	if !ok {
		return fmt.Errorf("unexpected message type %T", m)
	}
	proto.Reset(dst)
	proto.Merge(dst, s.msgs[s.next])
	s.next++
	return nil
}

func (s *scriptedStream) Context() context.Context     { return s.ctx }
func (s *scriptedStream) Header() (metadata.MD, error) { return nil, nil }
func (s *scriptedStream) Trailer() metadata.MD         { return nil }
func (s *scriptedStream) CloseSend() error             { return nil }
func (s *scriptedStream) SendMsg(any) error            { return nil }

// noopHandle is a PeerConnHandle that owns nothing.
type noopHandle struct{ invalidated atomic.Int64 }

func (h *noopHandle) Spec() ConnSpec         { return ConnSpec{PeerNodeID: "peer"} }
func (h *noopHandle) Purpose() string        { return searchForwarderPurpose }
func (h *noopHandle) GRPC() *grpc.ClientConn { return nil }
func (h *noopHandle) Release()               {}
func (h *noopHandle) Invalidate(error)       { h.invalidated.Add(1) }

func searchResponseWithRecords(n int) *gastrologv1.ForwardSearchResponse {
	records := make([]*gastrologv1.ExportRecord, n)
	for i := range records {
		records[i] = &gastrologv1.ExportRecord{Raw: fmt.Appendf(nil, "rec-%d", i)}
	}
	return &gastrologv1.ForwardSearchResponse{Records: records}
}

// A stream that fails mid-flight must report the failure after delivering what
// it received: the coordinator has to learn the search is incomplete, or a
// partial result passes for a whole one.
func TestSearchStreamDrainFailureReportsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &scriptedStream{
		ctx:  ctx,
		msgs: []*gastrologv1.ForwardSearchResponse{searchResponseWithRecords(3)},
		// A non-EOF failure once the scripted messages run out.
		failAfterScript: fmt.Errorf("peer went away"),
	}
	handle := &noopHandle{}
	sf := &SearchForwarder{}
	recCh := make(chan []*gastrologv1.ExportRecord, searchStreamBatchSlots)
	eCh := make(chan error, 1)
	var resumeToken []byte

	sf.drainSearchStream(ctx, "peer", stream, handle, recCh, eCh, &resumeToken)

	var delivered int
	for batch := range recCh {
		delivered += len(batch)
	}
	if delivered != 3 {
		t.Errorf("delivered %d records before the failure, want 3", delivered)
	}
	err, ok := <-eCh
	if !ok || err == nil {
		t.Fatal("drain failure did not reach errCh: the coordinator would treat a truncated stream as complete")
	}
	if !strings.Contains(err.Error(), "peer went away") {
		t.Errorf("error %q does not carry the stream failure", err)
	}
	if handle.invalidated.Load() != 1 {
		t.Errorf("connection invalidated %d times, want 1", handle.invalidated.Load())
	}
}

// A setup failure never starts the drain goroutine. SearchStream itself must
// close the channels and report the failure, or every such failure hangs the
// coordinator instead of failing it.
func TestSearchStreamSetupFailureReportsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A peer that cannot be resolved fails the very first setup step, before
	// any drain goroutine exists.
	peers := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:        "self",
		StaticResolve: func(string) (string, bool) { return "", false },
	})
	sf := NewSearchForwarder(peers)
	recCh, table, eCh, getResumeToken := sf.SearchStream(ctx, "peer", &gastrologv1.ForwardSearchRequest{})

	if table != nil {
		t.Errorf("tableResult = %v, want nil", table)
	}
	if got := getResumeToken(); got != nil {
		t.Errorf("resume token = %v, want nil", got)
	}
	if _, ok := <-recCh; ok {
		t.Error("record channel yielded a batch from a stream that never opened")
	}
	err, ok := <-eCh
	if !ok || err == nil {
		t.Fatal("setup failure did not reach errCh")
	}
}
