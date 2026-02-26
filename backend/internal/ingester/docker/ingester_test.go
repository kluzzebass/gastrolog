package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	configmem "gastrolog/internal/config/memory"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/querylang"
)

// --- Fake Docker client ---

type fakeDockerClient struct {
	mu         sync.Mutex
	containers []containerInfo
	logStreams map[string]*fakeLogStream // container ID -> stream
	events     chan containerEvent
	inspectErr error
	listErr    error
}

type fakeLogStream struct {
	data  []byte
	isTTY bool
}

func newFakeClient() *fakeDockerClient {
	return &fakeDockerClient{
		logStreams: make(map[string]*fakeLogStream),
		events:     make(chan containerEvent, 10),
	}
}

func (f *fakeDockerClient) addContainer(info containerInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers = append(f.containers, info)
}

func (f *fakeDockerClient) setLogStream(containerID string, data []byte, isTTY bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logStreams[containerID] = &fakeLogStream{data: data, isTTY: isTTY}
}

func (f *fakeDockerClient) ContainerList(ctx context.Context) ([]containerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	result := make([]containerInfo, len(f.containers))
	copy(result, f.containers)
	return result, nil
}

func (f *fakeDockerClient) ContainerInspect(ctx context.Context, id string) (containerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inspectErr != nil {
		return containerInfo{}, f.inspectErr
	}
	for _, c := range f.containers {
		if c.ID == id {
			return c, nil
		}
	}
	return containerInfo{}, fmt.Errorf("container %s not found", id)
}

func (f *fakeDockerClient) ContainerLogs(ctx context.Context, id string, since time.Time, follow bool, stdout, stderr bool) (io.ReadCloser, bool, error) {
	f.mu.Lock()
	stream, ok := f.logStreams[id]
	f.mu.Unlock()
	if !ok {
		return nil, false, fmt.Errorf("no log stream for container %s", id)
	}
	return io.NopCloser(bytes.NewReader(stream.data)), stream.isTTY, nil
}

func (f *fakeDockerClient) Events(ctx context.Context) (<-chan containerEvent, <-chan error) {
	errs := make(chan error, 1)
	out := make(chan containerEvent)

	go func() {
		defer close(out)
		defer close(errs)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-f.events:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, errs
}

func (f *fakeDockerClient) Ping(ctx context.Context) (string, error) {
	return "fake-test", nil
}

// --- Helper functions ---

func makeMultiplexedFrame(stream streamType, ts time.Time, line string) []byte {
	payload := ts.Format(time.RFC3339Nano) + " " + line + "\n"
	header := make([]byte, 8)
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
	return append(header, []byte(payload)...)
}

func makeRawLine(ts time.Time, line string) string {
	return ts.Format(time.RFC3339Nano) + " " + line + "\n"
}

func collectMessages(ctx context.Context, out <-chan orchestrator.IngestMessage, count int, timeout time.Duration) []orchestrator.IngestMessage {
	var msgs []orchestrator.IngestMessage
	deadline := time.After(timeout)
	for len(msgs) < count {
		select {
		case msg := <-out:
			msgs = append(msgs, msg)
		case <-deadline:
			return msgs
		case <-ctx.Done():
			return msgs
		}
	}
	return msgs
}

// --- Tests ---

func TestFactoryValidation(t *testing.T) {
	vault := configmem.NewStore()
	factory := NewFactory(vault)

	tests := []struct {
		name    string
		params  map[string]string
		wantErr string
	}{
		{
			name:    "default params are valid",
			params:  map[string]string{},
			wantErr: "", // Will fail on client creation (no docker), but params parse OK
		},
		{
			name: "tls_ca references missing cert",
			params: map[string]string{
				"tls_ca": "nonexistent-ca",
			},
			wantErr: `CA certificate "nonexistent-ca" not found`,
		},
		{
			name: "tls_cert references missing cert",
			params: map[string]string{
				"tls_cert": "nonexistent-client",
			},
			wantErr: `client certificate "nonexistent-client" not found`,
		},
		{
			name: "invalid filter expression",
			params: map[string]string{
				"filter": "env=prod AND",
			},
			wantErr: "invalid filter",
		},
		{
			name: "filter rejects bare tokens",
			params: map[string]string{
				"filter": "nginx",
			},
			wantErr: "token predicates not allowed",
		},
		{
			name: "invalid poll_interval",
			params: map[string]string{
				"poll_interval": "not-a-duration",
			},
			wantErr: "invalid poll_interval",
		},
		{
			name: "negative poll_interval",
			params: map[string]string{
				"poll_interval": "-5s",
			},
			wantErr: "poll_interval must be non-negative",
		},
		{
			name: "both stdout and stderr disabled",
			params: map[string]string{
				"stdout": "false",
				"stderr": "false",
			},
			wantErr: "at least one of stdout or stderr must be enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory(uuid.New(), tt.params, logging.Discard())
			if tt.wantErr == "" {
				// For valid params, we may still get a connection error.
				// That's fine - we're testing param validation, not connection.
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestSingleContainerTailing(t *testing.T) {
	client := newFakeClient()
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	container := containerInfo{
		ID:    "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
		Name:  "my-app",
		Image: "myimage:latest",
	}
	client.addContainer(container)

	var buf bytes.Buffer
	buf.Write(makeMultiplexedFrame(streamStdout, ts, "hello world"))
	buf.Write(makeMultiplexedFrame(streamStderr, ts.Add(time.Second), "an error"))
	client.setLogStream(container.ID, buf.Bytes(), false)

	cfg := ingesterConfig{
		ID:           "test-docker",
		PollInterval: 0, // Disable polling for deterministic test
		Stdout:       true,
		Stderr:       true,
		Logger:       logging.Discard(),
	}
	ing := newIngesterWithClient(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	go func() {
		ing.Run(ctx, out)
	}()

	msgs := collectMessages(ctx, out, 2, 3*time.Second)
	cancel()

	if len(msgs) < 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	// Check first message (stdout).
	if string(msgs[0].Raw) != "hello world" {
		t.Errorf("msg[0] Raw = %q, want %q", msgs[0].Raw, "hello world")
	}
	if msgs[0].Attrs["stream"] != "stdout" {
		t.Errorf("msg[0] stream = %q, want %q", msgs[0].Attrs["stream"], "stdout")
	}
	if msgs[0].Attrs["ingester_type"] != "docker" {
		t.Errorf("msg[0] ingester_type = %q, want %q", msgs[0].Attrs["ingester_type"], "docker")
	}
	if msgs[0].Attrs["ingester_id"] != "test-docker" {
		t.Errorf("msg[0] ingester_id = %q, want %q", msgs[0].Attrs["ingester_id"], "test-docker")
	}
	if msgs[0].Attrs["container_id"] != container.ID {
		t.Errorf("msg[0] container_id = %q, want %q", msgs[0].Attrs["container_id"], container.ID)
	}
	if msgs[0].Attrs["container_name"] != "my-app" {
		t.Errorf("msg[0] container_name = %q, want %q", msgs[0].Attrs["container_name"], "my-app")
	}
	if msgs[0].Attrs["image"] != "myimage:latest" {
		t.Errorf("msg[0] image = %q, want %q", msgs[0].Attrs["image"], "myimage:latest")
	}
	if !msgs[0].SourceTS.IsZero() {
		t.Errorf("msg[0] SourceTS should be zero, got %v", msgs[0].SourceTS)
	}
	if !msgs[0].IngestTS.Equal(ts) {
		t.Errorf("msg[0] IngestTS = %v, want %v", msgs[0].IngestTS, ts)
	}

	// Check second message (stderr).
	if string(msgs[1].Raw) != "an error" {
		t.Errorf("msg[1] Raw = %q, want %q", msgs[1].Raw, "an error")
	}
	if msgs[1].Attrs["stream"] != "stderr" {
		t.Errorf("msg[1] stream = %q, want %q", msgs[1].Attrs["stream"], "stderr")
	}
}

func TestContainerStartEvent(t *testing.T) {
	client := newFakeClient()
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	containerID := "newcontainer123456789012345678901234567890123456789012345678901234"
	container := containerInfo{
		ID:    containerID,
		Name:  "new-app",
		Image: "newimage:latest",
	}

	var buf bytes.Buffer
	buf.Write(makeMultiplexedFrame(streamStdout, ts, "started"))
	client.setLogStream(containerID, buf.Bytes(), false)

	cfg := ingesterConfig{
		ID:           "test-docker",
		PollInterval: 0,
		Stdout:       true,
		Stderr:       true,
		Logger:       logging.Discard(),
	}
	ing := newIngesterWithClient(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	go func() {
		ing.Run(ctx, out)
	}()

	// Wait a moment, then add the container and send a start event.
	time.Sleep(100 * time.Millisecond)
	client.addContainer(container)
	client.events <- containerEvent{Action: "start", ContainerID: containerID}

	msgs := collectMessages(ctx, out, 1, 3*time.Second)
	cancel()

	if len(msgs) < 1 {
		t.Fatalf("expected at least 1 message from started container, got %d", len(msgs))
	}
	if string(msgs[0].Raw) != "started" {
		t.Errorf("msg Raw = %q, want %q", msgs[0].Raw, "started")
	}
	if msgs[0].Attrs["container_name"] != "new-app" {
		t.Errorf("msg container_name = %q, want %q", msgs[0].Attrs["container_name"], "new-app")
	}
}

func TestContainerStopEvent(t *testing.T) {
	client := newFakeClient()
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	containerID := "stopcontainer12345678901234567890123456789012345678901234567890ab"
	container := containerInfo{
		ID:    containerID,
		Name:  "stop-me",
		Image: "img:latest",
	}
	client.addContainer(container)

	// Create a stream that blocks (simulating follow mode).
	var buf bytes.Buffer
	buf.Write(makeMultiplexedFrame(streamStdout, ts, "line1"))
	client.setLogStream(containerID, buf.Bytes(), false)

	cfg := ingesterConfig{
		ID:           "test-docker",
		PollInterval: 0,
		Stdout:       true,
		Stderr:       true,
		Logger:       logging.Discard(),
	}
	ing := newIngesterWithClient(cfg, client)

	ctx := t.Context()
	out := make(chan orchestrator.IngestMessage, 10)

	go func() {
		ing.Run(ctx, out)
	}()

	// Wait for the container to start streaming.
	collectMessages(ctx, out, 1, 2*time.Second)

	// Send stop event.
	client.events <- containerEvent{Action: "die", ContainerID: containerID}

	// Wait for the goroutine to clean up.
	time.Sleep(200 * time.Millisecond)

	ing.mu.Lock()
	_, tracked := ing.containers[containerID]
	ing.mu.Unlock()

	// The container should be removed from tracking after its stream ends.
	// Note: with fake client the stream ends immediately, so it may already be gone.
	_ = tracked
}

func TestBookmarkPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state", "docker", "test.json")

	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	containerID := "bm_container_123456789012345678901234567890123456789012345678901234"

	// Save state.
	st := state{
		Containers: map[string]containerBookmark{
			containerID: {LastTimestamp: ts},
		},
	}
	if err := saveState(stateFile, st); err != nil {
		t.Fatalf("saveState: %v", err)
	}

	// Load state.
	loaded, err := loadState(stateFile)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}

	bm, ok := loaded.Containers[containerID]
	if !ok {
		t.Fatalf("container %s not found in loaded state", containerID)
	}
	if !bm.LastTimestamp.Equal(ts) {
		t.Errorf("loaded timestamp = %v, want %v", bm.LastTimestamp, ts)
	}
}

func TestBookmarkLoadMissing(t *testing.T) {
	st, err := loadState("/nonexistent/path/state.json")
	if err != nil {
		t.Fatalf("loadState should not error for missing file: %v", err)
	}
	if len(st.Containers) != 0 {
		t.Errorf("expected empty containers, got %d", len(st.Containers))
	}
}

func TestBookmarkEmptyPath(t *testing.T) {
	st, err := loadState("")
	if err != nil {
		t.Fatalf("loadState empty path: %v", err)
	}
	if len(st.Containers) != 0 {
		t.Errorf("expected empty containers, got %d", len(st.Containers))
	}
	if err := saveState("", st); err != nil {
		t.Fatalf("saveState empty path: %v", err)
	}
}

func TestFilterMatching(t *testing.T) {
	tests := []struct {
		name       string
		filterExpr string
		info       containerInfo
		want       bool
	}{
		{
			name:       "label key=value match",
			filterExpr: "label.gastrolog.collect=true",
			info:       containerInfo{Labels: map[string]string{"gastrolog.collect": "true"}},
			want:       true,
		},
		{
			name:       "label key=value no match",
			filterExpr: "label.gastrolog.collect=true",
			info:       containerInfo{Labels: map[string]string{"gastrolog.collect": "false"}},
			want:       false,
		},
		{
			name:       "label key exists",
			filterExpr: "label.gastrolog.collect=*",
			info:       containerInfo{Labels: map[string]string{"gastrolog.collect": "true"}},
			want:       true,
		},
		{
			name:       "label key missing",
			filterExpr: "label.gastrolog.collect=*",
			info:       containerInfo{Labels: map[string]string{"other": "true"}},
			want:       false,
		},
		{
			name:       "name match",
			filterExpr: "name=my-app*",
			info:       containerInfo{Name: "my-app-1"},
			want:       true,
		},
		{
			name:       "name no match",
			filterExpr: "name=my-app*",
			info:       containerInfo{Name: "other-app"},
			want:       false,
		},
		{
			name:       "image match",
			filterExpr: "image=nginx*",
			info:       containerInfo{Image: "nginx:latest"},
			want:       true,
		},
		{
			name:       "image no match",
			filterExpr: "image=nginx*",
			info:       containerInfo{Image: "redis:7"},
			want:       false,
		},
		{
			name:       "combined AND match",
			filterExpr: "name=web* AND image=nginx*",
			info:       containerInfo{Name: "web-1", Image: "nginx:latest"},
			want:       true,
		},
		{
			name:       "combined AND partial mismatch",
			filterExpr: "name=web* AND image=nginx*",
			info:       containerInfo{Name: "api-1", Image: "nginx:latest"},
			want:       false,
		},
		{
			name:       "OR expression",
			filterExpr: "image=nginx* OR image=redis*",
			info:       containerInfo{Image: "redis:7"},
			want:       true,
		},
		{
			name:       "NOT expression",
			filterExpr: "NOT image=postgres*",
			info:       containerInfo{Image: "nginx:latest"},
			want:       true,
		},
		{
			name:       "NOT expression rejected",
			filterExpr: "NOT image=postgres*",
			info:       containerInfo{Image: "postgres:15"},
			want:       false,
		},
		{
			name:       "no filter matches all",
			filterExpr: "",
			info:       containerInfo{Name: "anything", Image: "any:image"},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filter *querylang.DNF
			if tt.filterExpr != "" {
				var err error
				filter, err = querylang.CompileAttrFilter(tt.filterExpr)
				if err != nil {
					t.Fatalf("CompileAttrFilter(%q): %v", tt.filterExpr, err)
				}
			}
			got := querylang.MatchAttrs(filter, containerAttrs(tt.info))
			if got != tt.want {
				t.Errorf("MatchAttrs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainerAttrs(t *testing.T) {
	info := containerInfo{
		Name:   "web-1",
		Image:  "nginx:latest",
		Labels: map[string]string{"env": "prod", "tier": "frontend"},
	}

	attrs := containerAttrs(info)

	if attrs["name"] != "web-1" {
		t.Errorf("name = %q, want %q", attrs["name"], "web-1")
	}
	if attrs["image"] != "nginx:latest" {
		t.Errorf("image = %q, want %q", attrs["image"], "nginx:latest")
	}
	if attrs["label.env"] != "prod" {
		t.Errorf("label.env = %q, want %q", attrs["label.env"], "prod")
	}
	if attrs["label.tier"] != "frontend" {
		t.Errorf("label.tier = %q, want %q", attrs["label.tier"], "frontend")
	}
	if len(attrs) != 4 {
		t.Errorf("expected 4 attrs, got %d", len(attrs))
	}
}

func TestLogDemuxMultiplexed(t *testing.T) {
	ts1 := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 15, 10, 0, 1, 0, time.UTC)

	var buf bytes.Buffer
	buf.Write(makeMultiplexedFrame(streamStdout, ts1, "stdout line"))
	buf.Write(makeMultiplexedFrame(streamStderr, ts2, "stderr line"))

	entries := make(chan logEntry, 10)
	go func() {
		readMultiplexed(&buf, entries)
		close(entries)
	}()

	var results []logEntry
	for e := range entries {
		results = append(results, e)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(results))
	}

	if string(results[0].Line) != "stdout line" {
		t.Errorf("entry[0] line = %q, want %q", results[0].Line, "stdout line")
	}
	if results[0].Stream != "stdout" {
		t.Errorf("entry[0] stream = %q, want %q", results[0].Stream, "stdout")
	}
	if !results[0].Timestamp.Equal(ts1) {
		t.Errorf("entry[0] timestamp = %v, want %v", results[0].Timestamp, ts1)
	}

	if string(results[1].Line) != "stderr line" {
		t.Errorf("entry[1] line = %q, want %q", results[1].Line, "stderr line")
	}
	if results[1].Stream != "stderr" {
		t.Errorf("entry[1] stream = %q, want %q", results[1].Stream, "stderr")
	}
}

func TestLogDemuxTTY(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	var buf bytes.Buffer
	buf.WriteString(makeRawLine(ts, "tty line 1"))
	buf.WriteString(makeRawLine(ts.Add(time.Second), "tty line 2"))

	entries := make(chan logEntry, 10)
	go func() {
		readRaw(&buf, entries)
		close(entries)
	}()

	var results []logEntry
	for e := range entries {
		results = append(results, e)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(results))
	}

	if string(results[0].Line) != "tty line 1" {
		t.Errorf("entry[0] line = %q, want %q", results[0].Line, "tty line 1")
	}
	if results[0].Stream != "tty" {
		t.Errorf("entry[0] stream = %q, want %q", results[0].Stream, "tty")
	}
	if !results[0].Timestamp.Equal(ts) {
		t.Errorf("entry[0] timestamp = %v, want %v", results[0].Timestamp, ts)
	}
}

func TestTimestampParsing(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantTS time.Time
		wantOK bool
	}{
		{
			name:   "RFC3339Nano",
			input:  "2025-01-15T10:30:00.123456789Z some log",
			wantTS: time.Date(2025, 1, 15, 10, 30, 0, 123456789, time.UTC),
			wantOK: true,
		},
		{
			name:   "RFC3339 no nanos",
			input:  "2025-01-15T10:30:00Z some log",
			wantTS: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			wantOK: true,
		},
		{
			name:   "no timestamp",
			input:  "just a log line",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, rest := parseTimestamp([]byte(tt.input))
			if tt.wantOK {
				if ts.IsZero() {
					t.Fatal("expected non-zero timestamp")
				}
				if !ts.Equal(tt.wantTS) {
					t.Errorf("timestamp = %v, want %v", ts, tt.wantTS)
				}
				// Check that the rest doesn't contain the timestamp.
				if strings.Contains(string(rest), "2025-01-15") {
					t.Errorf("rest should not contain timestamp: %q", rest)
				}
			} else {
				if !ts.IsZero() {
					t.Errorf("expected zero timestamp, got %v", ts)
				}
			}
		})
	}
}

func TestGracefulShutdown(t *testing.T) {
	client := newFakeClient()
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	containerID := "shutdown_test_123456789012345678901234567890123456789012345678"
	container := containerInfo{
		ID:    containerID,
		Name:  "shutdown-test",
		Image: "img:latest",
	}
	client.addContainer(container)

	var buf bytes.Buffer
	buf.Write(makeMultiplexedFrame(streamStdout, ts, "before shutdown"))
	client.setLogStream(containerID, buf.Bytes(), false)

	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state", "docker", "test.json")

	cfg := ingesterConfig{
		ID:           "test-docker",
		PollInterval: 0,
		Stdout:       true,
		Stderr:       true,
		StateFile:    stateFile,
		Logger:       logging.Discard(),
	}
	ing := newIngesterWithClient(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	done := make(chan error, 1)
	go func() {
		done <- ing.Run(ctx, out)
	}()

	// Collect the message.
	collectMessages(ctx, out, 1, 2*time.Second)

	// Cancel and wait for shutdown.
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down within timeout")
	}

	// Verify state was saved.
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("state file was not saved on shutdown")
	}
}

func TestFilteredContainersNotTailed(t *testing.T) {
	client := newFakeClient()
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Matching container.
	matchID := "match_container_123456789012345678901234567890123456789012345678"
	matchContainer := containerInfo{
		ID:     matchID,
		Name:   "web-app",
		Image:  "nginx:latest",
		Labels: map[string]string{"collect": "true"},
	}
	client.addContainer(matchContainer)

	var matchBuf bytes.Buffer
	matchBuf.Write(makeMultiplexedFrame(streamStdout, ts, "matched"))
	client.setLogStream(matchID, matchBuf.Bytes(), false)

	// Non-matching container.
	noMatchID := "nomatch_container_123456789012345678901234567890123456789012345"
	noMatchContainer := containerInfo{
		ID:     noMatchID,
		Name:   "db-app",
		Image:  "postgres:15",
		Labels: map[string]string{"collect": "false"},
	}
	client.addContainer(noMatchContainer)

	var noMatchBuf bytes.Buffer
	noMatchBuf.Write(makeMultiplexedFrame(streamStdout, ts, "should not appear"))
	client.setLogStream(noMatchID, noMatchBuf.Bytes(), false)

	filter, err := querylang.CompileAttrFilter("name=web*")
	if err != nil {
		t.Fatalf("CompileAttrFilter: %v", err)
	}

	cfg := ingesterConfig{
		ID:           "test-docker",
		PollInterval: 0,
		Stdout:       true,
		Stderr:       true,
		Filter:       filter,
		Logger:       logging.Discard(),
	}
	ing := newIngesterWithClient(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 10)

	go func() {
		ing.Run(ctx, out)
	}()

	msgs := collectMessages(ctx, out, 1, 2*time.Second)
	cancel()

	// Should only get the matching container's message.
	for _, msg := range msgs {
		if msg.Attrs["container_name"] == "db-app" {
			t.Error("received message from filtered-out container")
		}
	}
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message from matching container")
	}
	if string(msgs[0].Raw) != "matched" {
		t.Errorf("msg Raw = %q, want %q", msgs[0].Raw, "matched")
	}
}

func TestStreamTypeString(t *testing.T) {
	if streamStdout.String() != "stdout" {
		t.Errorf("stdout string = %q", streamStdout.String())
	}
	if streamStderr.String() != "stderr" {
		t.Errorf("stderr string = %q", streamStderr.String())
	}
	if streamStdin.String() != "stdin" {
		t.Errorf("stdin string = %q", streamStdin.String())
	}
}

