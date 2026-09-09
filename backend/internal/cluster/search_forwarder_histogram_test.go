package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// scriptedStream hands out a fixed sequence of ForwardSearch responses. The
// message at gateIdx blocks until release closes, so a test can hold the
// stream open at a known point and assert what the drain has published by
// then without waiting on the clock.
type scriptedStream struct {
	ctx      context.Context
	msgs     []*gastrologv1.ForwardSearchResponse
	next     int
	holdAt   int
	release  <-chan struct{}
	received atomic.Int64
	// failAfterScript, when set, is returned instead of io.EOF once the
	// scripted messages run out.
	failAfterScript error
}

func (s *scriptedStream) RecvMsg(m any) error {
	if s.next == s.holdAt && s.release != nil {
		select {
		case <-s.release:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
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
	s.received.Add(1)
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

// The coordinator asks a remote vault for its histogram before it reads a
// single record batch, and every record send can park on a full channel. So
// the histogram has to be published from the first message: publishing it at
// end of stream means it is never published for any vault whose match set
// outruns the channel, and the search hangs until the request deadline.
//
// The stream here is held open after its first message, so "the drain has not
// reached the end" is a fact the test controls rather than something it waits
// for — an end-of-stream gate could not possibly have opened at the point the
// histogram is demanded below.
func TestSearchStreamPublishesHistogramFromTheFirstMessage(t *testing.T) {
	streamCtx, stopStream := context.WithCancel(context.Background())
	defer stopStream()

	buckets := []*gastrologv1.HistogramBucket{
		{TimestampMs: 1000, Count: 7},
		{TimestampMs: 2000, Count: 11},
	}
	release := make(chan struct{})
	// Message 0 carries the histogram, then the stream is held. The remaining
	// messages are more record batches than recCh can hold, so the drain would
	// park even once released.
	msgs := []*gastrologv1.ForwardSearchResponse{{Histogram: buckets}}
	for range 40 {
		msgs = append(msgs, searchResponseWithRecords(200))
	}
	stream := &scriptedStream{ctx: streamCtx, msgs: msgs, holdAt: 1, release: release}

	sf := &SearchForwarder{}
	gate := newHistogramGate()
	recCh := make(chan []*gastrologv1.ExportRecord, searchStreamBatchSlots)
	eCh := make(chan error, 1)
	var resumeToken []byte
	go sf.drainSearchStream(streamCtx, "peer", stream, &noopHandle{}, recCh, eCh, gate, &resumeToken)

	// Nobody is reading recCh, exactly as the coordinator is not until it has
	// the histogram. The deadline is only the failure detector.
	got := make(chan []*gastrologv1.HistogramBucket, 1)
	go func() { got <- gate.waitFor(streamCtx) }()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	var published []*gastrologv1.HistogramBucket
	select {
	case published = <-got:
	case <-waitCtx.Done():
		t.Fatal("histogram never published while the stream was held open after its first message: the fan-out cannot start")
	}

	// Premise: the stream is provably still at its second message, so nothing
	// that waited for end of stream could have run.
	if received := stream.received.Load(); received != 1 {
		t.Fatalf("stream delivered %d messages before the histogram was published, want 1", received)
	}

	if len(published) != len(buckets) {
		t.Fatalf("published %d buckets, want %d", len(published), len(buckets))
	}
	for i := range buckets {
		if published[i].TimestampMs != buckets[i].TimestampMs || published[i].Count != buckets[i].Count {
			t.Errorf("bucket %d = (%d, %d), want (%d, %d)", i,
				published[i].TimestampMs, published[i].Count, buckets[i].TimestampMs, buckets[i].Count)
		}
	}

	// Releasing the stream lets records flow to a coordinator that is now
	// draining, which is the whole point of publishing early.
	close(release)
	var delivered int
	for batch := range recCh {
		delivered += len(batch)
	}
	if want := 40 * 200; delivered != want {
		t.Errorf("delivered %d records after the histogram unblocked the coordinator, want %d", delivered, want)
	}
}

// A stream that dies before its first message publishes no histogram, but it
// must still release the coordinator — otherwise a failing peer hangs the
// query instead of failing it.
func TestSearchStreamOpensHistogramGateOnEarlyFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &scriptedStream{ctx: ctx} // no messages: RecvMsg returns EOF
	sf := &SearchForwarder{}
	gate := newHistogramGate()
	recCh := make(chan []*gastrologv1.ExportRecord, searchStreamBatchSlots)
	eCh := make(chan error, 1)
	var resumeToken []byte

	sf.drainSearchStream(ctx, "peer", stream, &noopHandle{}, recCh, eCh, gate, &resumeToken)

	if got := gate.waitFor(ctx); got != nil {
		t.Errorf("histogram = %v, want nil for a stream that produced nothing", got)
	}
}

// A stream that fails mid-flight must open the gate AND report the failure:
// the coordinator has to stop waiting and has to learn the search is
// incomplete, or a partial result passes for a whole one.
func TestSearchStreamDrainFailureOpensGateAndReportsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buckets := []*gastrologv1.HistogramBucket{{TimestampMs: 1000, Count: 3}}
	stream := &scriptedStream{
		ctx:  ctx,
		msgs: []*gastrologv1.ForwardSearchResponse{{Histogram: buckets}},
		// A non-EOF failure once the scripted messages run out.
		failAfterScript: fmt.Errorf("peer went away"),
	}
	handle := &noopHandle{}
	sf := &SearchForwarder{}
	gate := newHistogramGate()
	recCh := make(chan []*gastrologv1.ExportRecord, searchStreamBatchSlots)
	eCh := make(chan error, 1)
	var resumeToken []byte

	sf.drainSearchStream(ctx, "peer", stream, handle, recCh, eCh, gate, &resumeToken)

	if got := gate.waitFor(ctx); len(got) != 1 || got[0].Count != 3 {
		t.Errorf("histogram = %v, want the first message's buckets even though the stream then failed", got)
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

// A setup failure never starts the drain goroutine, so nothing downstream can
// open the gate. SearchStream itself must, or every such failure hangs the
// coordinator instead of failing it.
func TestSearchStreamSetupFailureOpensHistogramGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A peer that cannot be resolved fails the very first setup step, before
	// any drain goroutine exists to open the gate.
	peers := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:        "self",
		StaticResolve: func(string) (string, bool) { return "", false },
	})
	sf := NewSearchForwarder(peers)
	recCh, table, eCh, getResumeToken, getHistogram := sf.SearchStream(ctx, "peer", &gastrologv1.ForwardSearchRequest{})

	// Nothing may block: the caller reads the histogram before the records.
	if got := getHistogram(); got != nil {
		t.Errorf("histogram = %v, want nil for a stream that never opened", got)
	}
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
