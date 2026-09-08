package docker

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

// panickingBody stands in for a log stream whose framing raises a panic —
// the class of bug a malformed or hostile frame would trigger inside
// readMultiplexed.
type panickingBody struct{}

func (panickingBody) Read([]byte) (int, error) { panic("frame decode") }
func (panickingBody) Close() error             { return nil }

// panickingLogClient serves exactly one container whose log body panics.
type panickingLogClient struct{}

func (panickingLogClient) ContainerList(context.Context) ([]containerInfo, error) {
	return nil, errors.New("not used")
}

func (panickingLogClient) ContainerLogs(context.Context, string, time.Time, bool, bool, bool) (io.ReadCloser, bool, error) {
	return panickingBody{}, false, nil
}

func (panickingLogClient) Events(context.Context) (<-chan containerEvent, <-chan error) {
	return nil, nil
}

func (panickingLogClient) ContainerInspect(context.Context, string) (containerInfo, error) {
	return containerInfo{}, errors.New("not used")
}

func (panickingLogClient) Ping(context.Context) (string, error) { return "", nil }

// panickingEventClient serves an empty container list and panics when the
// event stream is opened.
type panickingEventClient struct{ panickingLogClient }

func (panickingEventClient) ContainerList(context.Context) ([]containerInfo, error) {
	return nil, nil
}

func (panickingEventClient) Events(context.Context) (<-chan containerEvent, <-chan error) {
	panic("event decode")
}

// TestDockerEventLoopPanicFailsTheRun proves a panic in container discovery
// ends the run rather than leaving the ingester alive but blind to new
// containers. The ingester manager retries a failed run; a swallowed panic
// would leave a process that looks healthy and ingests nothing new.
func TestDockerEventLoopPanicFailsTheRun(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.New()
	ing := newIngesterWithClient(ingesterConfig{
		ID:        "test-docker",
		StateFile: filepath.Join(t.TempDir(), "state.json"),
		Logger:    logger,
	}, panickingEventClient{})

	runErr := make(chan error, 1)
	go func() { runErr <- ing.Run(t.Context(), make(chan ingestion.IngesterMessage, 1)) }()

	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("panic in the event loop was swallowed; the run reported success")
		}
		if !strings.Contains(err.Error(), "panicked") {
			t.Errorf("run error does not identify the panic: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run never returned after the event loop panicked")
	}

	if !logs.Wait(5*time.Second, "recovered panic", "docker event loop", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

// TestDockerFrameReaderPanicFailsTheStream proves a panic while decoding
// Docker log frames is reported as a stream error, so the container's stream
// reconnects. Left unguarded it would end the process, taking every vault
// and Raft group on the node down with one container's log driver.
func TestDockerFrameReaderPanicFailsTheStream(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.New()
	out := make(chan ingestion.IngesterMessage, 1)
	info := containerInfo{ID: "0123456789abcdef", Name: "victim", Image: "test"}
	since := time.Now()

	err := streamOnce(t.Context(), panickingLogClient{}, info, since, true, true, "test-docker", logger, out, nil, &since, nil)

	if err == nil {
		t.Fatal("panic in the frame reader was swallowed; the stream reported a clean end")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("stream error does not identify the panic: %v", err)
	}
	if !logs.Contains("recovered panic", "docker frame reader", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}
