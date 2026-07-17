package segmentation

// Fault-injection coverage for gastrolog-1c9f5l: a commit (fsync/rotation)
// failure must never silently kill a vault writer. The writer abandons the
// suspect segment for crash recovery, rotates, and keeps serving; when even
// rotation fails it degrades — nacking ack producers immediately and counting
// fire-and-forget drops — instead of wedging the bounded input queue.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

var errInjectedSync = errors.New("injected sync failure")

var errInjectedCreate = errors.New("injected create failure (disk full)")

// failingSyncSegment wraps a real segment file with an always-failing Sync.
type failingSyncSegment struct {
	segmentFile
}

func (f *failingSyncSegment) Sync() error { return errInjectedSync }

// recordingAlerts captures Set/Clear calls.
type recordingAlerts struct {
	mu     sync.Mutex
	sets   []string
	clears []string
}

func (a *recordingAlerts) Raise(typeID, instanceKey, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sets = append(a.sets, typeID+":"+instanceKey)
}

func (a *recordingAlerts) Clear(typeID, instanceKey string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.clears = append(a.clears, typeID+":"+instanceKey)
}

func (a *recordingAlerts) counts() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.sets), len(a.clears)
}

var faultTestSeq atomic.Uint32

func testRecord(t *testing.T) *record.Record {
	t.Helper()
	ts := time.Now().UTC()
	return &record.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
			IngestSeq:  faultTestSeq.Add(1),
		},
		Raw: []byte("fault test"),
	}
}

// syncBuffer is a goroutine-safe log sink for asserting on writer log output.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestTextLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, nil))
}

// sendAck submits an ack-bearing input and returns the ack result, failing the
// test if the ack does not resolve — the whole point of gastrolog-1c9f5l is
// that producers must never hang on a broken writer.
func sendAck(t *testing.T, in chan<- Input, rec *record.Record) error {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case in <- Input{Record: rec, Ack: ack}:
	case <-time.After(5 * time.Second):
		t.Fatal("input queue send blocked: writer is wedged")
	}
	select {
	case err := <-ack:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("ack never resolved: writer is wedged")
		return nil
	}
}

func startManager(t *testing.T, cfg Config, vaultID glid.GLID) (*Manager, chan<- Input) {
	t.Helper()
	mgr, _ := New(cfg)
	in, err := mgr.RegisterVault(vaultID, t.TempDir(), VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return mgr, in
}

// TestCommitFailureRotatesAndKeepsServing: the first working segment's fsync
// fails. The parked ack must be nacked, the writer must rotate to a fresh
// segment and keep accepting records, and the operator alert must be raised
// and cleared.
func TestCommitFailureRotatesAndKeepsServing(t *testing.T) {
	t.Parallel()
	alerts := &recordingAlerts{}
	var created atomic.Int32
	cfg := Config{
		Alerts: alerts,
		newSegmentFile: func(path string, meta segment.Meta) (segmentFile, error) {
			sf, err := segment.Create(path, meta)
			if err != nil {
				return nil, err
			}
			if created.Add(1) == 1 {
				return &failingSyncSegment{segmentFile: sf}, nil
			}
			return sf, nil
		},
	}
	_, in := startManager(t, cfg, glid.New())

	if err := sendAck(t, in, testRecord(t)); !errors.Is(err, errInjectedSync) {
		t.Fatalf("first ack = %v, want injected sync failure", err)
	}
	// The writer rotated to a healthy segment inline; the next record lands.
	if err := sendAck(t, in, testRecord(t)); err != nil {
		t.Fatalf("ack after rotation = %v, want nil", err)
	}
	sets, clears := alerts.counts()
	if sets == 0 || clears == 0 {
		t.Fatalf("alerts sets=%d clears=%d, want both >0 (raised on failure, cleared on recovery)", sets, clears)
	}
}

// TestReopenFailureDegradesThenRecovers: fsync fails AND segment creation
// fails (disk full). Producers must get prompt nacks — never hang — while
// fire-and-forget records are counted as dropped. When creation starts
// succeeding again the backoff reopen recovers the writer and clears the
// alert.
func TestReopenFailureDegradesThenRecovers(t *testing.T) {
	t.Parallel()
	alerts := &recordingAlerts{}
	var created atomic.Int32
	var diskFull atomic.Bool
	cfg := Config{
		Alerts: alerts,
		newSegmentFile: func(path string, meta segment.Meta) (segmentFile, error) {
			if diskFull.Load() {
				return nil, errInjectedCreate
			}
			sf, err := segment.Create(path, meta)
			if err != nil {
				return nil, err
			}
			if created.Add(1) == 1 {
				return &failingSyncSegment{segmentFile: sf}, nil
			}
			return sf, nil
		},
	}
	mgr, in := startManager(t, cfg, glid.New())

	diskFull.Store(true) // reopen after the sync failure must also fail
	if err := sendAck(t, in, testRecord(t)); !errors.Is(err, errInjectedSync) {
		t.Fatalf("first ack = %v, want injected sync failure", err)
	}
	// Degraded: ack producers get an immediate nack instead of hanging.
	if err := sendAck(t, in, testRecord(t)); !errors.Is(err, errWriterDegraded) {
		t.Fatalf("degraded ack = %v, want errWriterDegraded", err)
	}
	// Fire-and-forget records are dropped but COUNTED, never silent.
	in <- Input{Record: testRecord(t)}
	deadline := time.Now().Add(5 * time.Second)
	for mgr.DroppedRecords() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("dropped-records counter never incremented for nil-ack record in degraded mode")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Disk recovers; the backoff reopen must restore service without any new
	// input (timer-driven, not traffic-driven).
	diskFull.Store(false)
	deadline = time.Now().Add(10 * time.Second)
	for {
		if err := sendAck(t, in, testRecord(t)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer never recovered after disk came back")
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, clears := alerts.counts()
	if clears == 0 {
		t.Fatal("alert never cleared after recovery")
	}
}

// TestEncodeFailureCountedAndNonFatal: an unencodable record (nil) must not
// kill the writer. Nil-ack: counted as dropped. With ack: the error reaches
// the producer. Healthy records keep flowing afterward.
func TestEncodeFailureCountedAndNonFatal(t *testing.T) {
	t.Parallel()
	mgr, in := startManager(t, Config{}, glid.New())

	in <- Input{Record: nil} // encode fails: nil record, nil ack → counted drop
	deadline := time.Now().Add(5 * time.Second)
	for mgr.DroppedRecords() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("dropped-records counter never incremented for encode failure")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := sendAck(t, in, nil); err == nil {
		t.Fatal("ack-bearing encode failure returned nil, want error")
	}
	if err := sendAck(t, in, testRecord(t)); err != nil {
		t.Fatalf("healthy record after encode failures = %v, want nil", err)
	}
	if got := mgr.DroppedRecords(); got != 1 {
		t.Fatalf("DroppedRecords = %d, want 1 (ack-bearing failure carries its error, only nil-ack counts)", got)
	}
}

// TestShutdownFlushFailureLogged: a commit failure during writer stop must
// leave a trace in the log, not vanish into discarded error values.
func TestShutdownFlushFailureLogged(t *testing.T) {
	t.Parallel()
	var logBuf syncBuffer
	logger := newTestTextLogger(&logBuf)
	var created atomic.Int32
	cfg := Config{
		Logger: logger,
		// Long windows: nothing commits before stop(), so the shutdown path
		// owns the failing flush.
		SyncBatchSize:   1000,
		SyncBatchWindow: time.Hour,
		newSegmentFile: func(path string, meta segment.Meta) (segmentFile, error) {
			sf, err := segment.Create(path, meta)
			if err != nil {
				return nil, err
			}
			if created.Add(1) == 1 {
				return &failingSyncSegment{segmentFile: sf}, nil
			}
			return sf, nil
		},
	}
	vaultID := glid.New()
	mgr, _ := New(cfg)
	in, err := mgr.RegisterVault(vaultID, t.TempDir(), VaultConfig{})
	if err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Run(ctx)
	}()

	in <- Input{Record: testRecord(t)} // parked in the batch, uncommitted
	// Wait for the record loop to consume (and append) the input; otherwise
	// UnregisterVault can race Run and stop a writer that never started,
	// flushing an empty segment with nothing to fail.
	deadline := time.Now().Add(5 * time.Second)
	for len(in) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("record loop never consumed the input")
		}
		time.Sleep(2 * time.Millisecond)
	}
	mgr.UnregisterVault(vaultID) // stop() → final commit fails → must log
	cancel()
	<-done

	if out := logBuf.String(); !strings.Contains(out, "injected sync failure") {
		t.Fatalf("shutdown flush failure left no log trace; log output:\n%s", out)
	}
}

// TestAppendStatsCounters: the per-vault throughput counters feeding the
// stats broadcast (gastrolog-4eh5ns). After N acked records, appended and
// durable both equal N (acks resolve only after the group-commit fsync), and
// byte counts reflect the appended frame bodies.
func TestAppendStatsCounters(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	mgr, in := startManager(t, Config{}, vaultID)

	const n = 5
	for range n {
		if err := sendAck(t, in, testRecord(t)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	stats := mgr.AppendStats()
	if len(stats) != 1 {
		t.Fatalf("AppendStats len = %d, want 1", len(stats))
	}
	s := stats[0]
	if s.VaultID != vaultID {
		t.Fatalf("vault = %s, want %s", s.VaultID, vaultID)
	}
	if s.RecordsAppended != n {
		t.Fatalf("RecordsAppended = %d, want %d", s.RecordsAppended, n)
	}
	if s.RecordsDurable != n {
		t.Fatalf("RecordsDurable = %d, want %d (ack resolves only after commit)", s.RecordsDurable, n)
	}
	if s.BytesAppended == 0 {
		t.Fatal("BytesAppended = 0, want > 0")
	}
	if s.QueueCap == 0 {
		t.Fatal("QueueCap = 0, want the writer's bounded queue capacity")
	}
}
