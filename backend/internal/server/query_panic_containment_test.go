package server

import (
	"context"
	"iter"
	"testing"
	"time"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/logging/logtest"
)

func panicTestServer() (*QueryServer, *logtest.Recorder) {
	logger, logs := logtest.New()
	return &QueryServer{logger: logger}, logs
}

// TestGuardedHistogramSurvivesAPanic proves a panic in histogram bucketing
// costs the client its chart, not the process. The caller blocks on exactly
// one value from the histogram channel, so the guard must return a value
// rather than abandon the goroutine.
func TestGuardedHistogramSurvivesAPanic(t *testing.T) {
	t.Parallel()
	s, logs := panicTestServer()

	histCh := make(chan []*apiv1.HistogramBucket, 1)
	go func() {
		histCh <- s.guardedHistogram(func() []*apiv1.HistogramBucket {
			panic("bucket arithmetic")
		})
	}()

	select {
	case buckets := <-histCh:
		if buckets != nil {
			t.Errorf("expected no buckets after a panic, got %d", len(buckets))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the search would hang: no histogram value was ever sent")
	}

	if !logs.Contains("recovered panic", "search histogram", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

func TestGuardedHistogramPassesThroughItsResult(t *testing.T) {
	t.Parallel()
	s, logs := panicTestServer()

	want := []*apiv1.HistogramBucket{{Count: 7}}
	got := s.guardedHistogram(func() []*apiv1.HistogramBucket { return want })
	if len(got) != 1 || got[0].GetCount() != 7 {
		t.Fatalf("guard altered the result: %v", got)
	}
	if logs.Contains("recovered panic") {
		t.Errorf("logged a panic that never happened: %s", logs)
	}
}

// TestPumpLocalFollowSurvivesAPanic proves a panic inside the local search
// iterator ends one client's follow instead of the process, and still closes
// the channel the merge loop is waiting on.
func TestPumpLocalFollowSurvivesAPanic(t *testing.T) {
	t.Parallel()
	s, logs := panicTestServer()

	panicking := func(yield func(chunk.Record, error) bool) {
		panic("iterator arithmetic")
	}

	localCh := make(chan localFollowMsg, 4)
	go s.pumpLocalFollow(context.Background(), iter.Seq2[chunk.Record, error](panicking), localCh)

	select {
	case _, ok := <-localCh:
		if ok {
			t.Fatal("expected no records from a panicking iterator")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follow would hang: the local channel was never closed")
	}

	if !logs.Contains("recovered panic", "follow local iterator", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

// panickingSearcher panics on Follow, standing in for any bug on the
// cross-node follow path.
type panickingSearcher struct {
	RemoteSearcher
}

func (p *panickingSearcher) Follow(_ context.Context, _ string, _ *apiv1.ForwardFollowRequest) (<-chan *apiv1.ExportRecord, <-chan error) {
	panic("remote follow decode")
}

// TestPumpRemoteFollowSurvivesAPanic proves a panic while draining one
// node's follow stream costs the follow that node's records, leaving the
// process — and the other nodes' streams — running.
func TestPumpRemoteFollowSurvivesAPanic(t *testing.T) {
	t.Parallel()
	s, logs := panicTestServer()
	s.remoteSearcher = &panickingSearcher{}

	merged := make(chan *apiv1.Record, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pumpRemoteFollow(context.Background(), "node-2", nil, "*", merged)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the remote follow goroutine never returned")
	}

	if !logs.Contains("recovered panic", "follow remote stream", "node-2", "stack") {
		t.Errorf("panic was contained but not reported with the node and a stack: %s", logs)
	}
}
