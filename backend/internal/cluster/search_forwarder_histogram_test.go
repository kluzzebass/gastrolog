package cluster

import (
	"context"
	"fmt"
	"io"
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
	recCh := make(chan []*gastrologv1.ExportRecord, 16)
	eCh := make(chan error, 1)
	var resumeToken []byte
	go sf.drainSearchStream(streamCtx, "peer", stream, &noopHandle{}, recCh, eCh, gate, &resumeToken)

	// Nobody is reading recCh, exactly as the coordinator is not until it has
	// the histogram. The deadline is only the failure detector.
	got := make(chan []*gastrologv1.HistogramBucket, 1)
	go func() { got <- gate.get() }()

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
	recCh := make(chan []*gastrologv1.ExportRecord, 16)
	eCh := make(chan error, 1)
	var resumeToken []byte

	sf.drainSearchStream(ctx, "peer", stream, &noopHandle{}, recCh, eCh, gate, &resumeToken)

	if got := gate.get(); got != nil {
		t.Errorf("histogram = %v, want nil for a stream that produced nothing", got)
	}
}
