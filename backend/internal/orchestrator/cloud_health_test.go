package orchestrator

import (
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
)

// mockCloudBackedChunkManager is a minimal ChunkManager that also implements
// cloudHealthChecker and ChunkCloudUploader for testing cloud health
// evaluation and backfill scheduling.
type mockCloudBackedChunkManager struct {
	chunk.ChunkManager   // embedded nil — only List/UploadToCloud used
	degraded             atomic.Bool
	degradedErr          atomic.Value // string
	cloudStoreConfigured atomic.Bool
	chunks               []chunk.ChunkMeta

	mu          sync.Mutex
	uploadCalls []chunk.ChunkID
}

func (m *mockCloudBackedChunkManager) CloudDegraded() bool { return m.degraded.Load() }
func (m *mockCloudBackedChunkManager) CloudDegradedError() string {
	if v := m.degradedErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}
func (m *mockCloudBackedChunkManager) CloudStoreConfigured() bool {
	return m.cloudStoreConfigured.Load()
}
func (m *mockCloudBackedChunkManager) List() ([]chunk.ChunkMeta, error) {
	return m.chunks, nil
}
func (m *mockCloudBackedChunkManager) UploadToCloud(id chunk.ChunkID) error {
	m.mu.Lock()
	m.uploadCalls = append(m.uploadCalls, id)
	m.mu.Unlock()
	return nil
}

// uploadCallCount returns the number of upload calls under lock.
func (m *mockCloudBackedChunkManager) uploadCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploadCalls)
}

// uploadCallsCopy returns a snapshot of upload calls under lock.
func (m *mockCloudBackedChunkManager) uploadCallsCopy() []chunk.ChunkID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkID, len(m.uploadCalls))
	copy(out, m.uploadCalls)
	return out
}

// waitUploadCount polls until uploadCalls reaches the expected count or the
// deadline passes. Returns the final count.
func waitUploadCount(m *mockCloudBackedChunkManager, want int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		if got := m.uploadCallCount(); got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------- evaluateCloudHealth ----------

func TestEvaluateCloudHealth_SetsAlertWhenDegraded(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	vaultID := glid.New()
	mock := &mockCloudBackedChunkManager{}
	mock.degraded.Store(true)
	mock.degradedErr.Store("connection refused")

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "cloud", Chunks: mock}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	orch.evaluateCloudHealth()

	alerts := ac.Standing()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	wantID := fmt.Sprintf("cloud-store:%s", vaultID)
	if alerts[0].ID != wantID {
		t.Errorf("alert ID = %q, want %q", alerts[0].ID, wantID)
	}
	if alerts[0].Priority != alert.High {
		t.Errorf("priority = %d, want High(%d)", alerts[0].Priority, alert.High)
	}
}

func TestEvaluateCloudHealth_ClearsAlertWhenHealthy(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	vaultID := glid.New()
	mock := &mockCloudBackedChunkManager{}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "cloud", Chunks: mock}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	// Simulate prior degraded alarm.
	ac.Raise("cloud-store", vaultID.String(), "was broken")

	// Now cloud is healthy (degraded=false, default).
	orch.evaluateCloudHealth()

	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("expected 0 alerts after recovery, got %d: %v", len(alerts), alerts)
	}
}

func TestEvaluateCloudHealth_SkipsFileVaultWithoutCloudStore(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	mock := &mockCloudBackedChunkManager{}
	mock.degraded.Store(true)
	mock.degradedErr.Store("boom")

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac

	// File vault without cloud store configured — should be skipped.
	vaultInst := &VaultInstance{VaultID: glid.New(), Type: "file", Chunks: mock}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	orch.evaluateCloudHealth()

	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("expected 0 alerts for file vault without cloud store, got %d", len(alerts))
	}
}

func TestEvaluateCloudHealth_FileVaultWithCloudStore(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	vaultID := glid.New()
	mock := &mockCloudBackedChunkManager{}
	mock.cloudStoreConfigured.Store(true)
	mock.degraded.Store(true)
	mock.degradedErr.Store("connection refused")

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: mock}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	orch.evaluateCloudHealth()

	alerts := ac.Standing()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	wantID := fmt.Sprintf("cloud-store:%s", vaultID)
	if alerts[0].ID != wantID {
		t.Errorf("alert ID = %q, want %q", alerts[0].ID, wantID)
	}
}

func TestEvaluateCloudHealth_NilAlerts(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = nil

	// Should not panic.
	orch.evaluateCloudHealth()
}

// ---------- backfillCloudUploads ----------

func TestBackfillCloudUploads_SchedulesSealedNonCloudBacked(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)

	// Wait for the scheduler job to run.
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("expected 1 upload call, got %d", got)
	}
	calls := mock.uploadCallsCopy()
	if calls[0] != chunkID {
		t.Errorf("uploaded chunk = %s, want %s", calls[0], chunkID)
	}
}

func TestBackfillCloudUploads_FileVaultWithCloudStore(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	mock.cloudStoreConfigured.Store(true)

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("expected 1 upload call, got %d", got)
	}
}

func TestBackfillCloudUploads_SkipsPlacementFollower(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	// Placement follower: no cloud store write access.
	mock.cloudStoreConfigured.Store(false)

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true }, // ctl leader, but not uploader
	}

	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(100 * time.Millisecond)

	if mock.uploadCallCount() != 0 {
		t.Fatalf("expected 0 uploads on placement follower, got %d", mock.uploadCallCount())
	}
}

func TestBackfillCloudUploads_SkipsCloudBacked(t *testing.T) {
	t.Parallel()

	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: true, CloudBacked: true,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(200 * time.Millisecond) // brief grace for scheduler to (not) run

	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("expected 0 upload calls for cloud-backed chunk, got %d", got)
	}
}

func TestBackfillCloudUploads_SkipsUnsealed(t *testing.T) {
	t.Parallel()

	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: false, CloudBacked: false,
				WriteStart: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(200 * time.Millisecond)

	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("expected 0 upload calls for unsealed chunk, got %d", got)
	}
}

func TestBackfillCloudUploads_SkipsWhenFSMOverlaySaysCloudBacked(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
		// FSM overlay says cloud-backed — backfill should skip.
		OverlayFromFSM: func(m chunk.ChunkMeta) chunk.ChunkMeta {
			m.CloudBacked = true
			return m
		},
	}

	orch.backfillCloudUploads(vaultInst)

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(200 * time.Millisecond)

	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("expected 0 uploads when FSM overlay says cloud-backed, got %d", got)
	}
}

// TestBackfillCloudUploadsLeaderOnly verifies backfill runs only on the instance
// Raft leader. See gastrolog-2nngw — followers learn about cloud-backed
// chunks via the FSM, so duplicate backfill on every node is wasteful.
func TestBackfillCloudUploadsLeaderOnly(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac

	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	orch.evaluateCloudHealth()

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	// Poll for the upload — under race detector this can take several seconds.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := mock.uploadCallCount()
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 1 upload on Raft leader with data, got %d", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBackfillCloudUploadsSkippedOnFollower verifies non-leader vaults
// don't run backfill — the leader handles it.
func TestBackfillCloudUploadsSkippedOnFollower(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac

	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return false },
	}
	orch.RegisterVault(NewVault(glid.New(), vaultInst))

	orch.evaluateCloudHealth()

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	// Give the scheduler a moment to (not) run anything.
	time.Sleep(200 * time.Millisecond)

	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("expected 0 uploads on follower, got %d", got)
	}
}

func TestBackfillCloudUploads_DeduplicatesPendingJobs(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	// Call backfill twice — should only schedule once.
	orch.backfillCloudUploads(vaultInst)
	orch.backfillCloudUploads(vaultInst)

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("expected 1 upload (deduped), got %d", got)
	}
	// Brief grace period to ensure no second upload sneaks in.
	time.Sleep(100 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("expected 1 upload (deduped), got %d", got)
	}
}

// ---------- gastrolog-576bm: backfill demoted from 5s primary to edge-driven ----------

// TestEvaluateCloudHealth_SteadyStateHealthyDoesNotResweep pins the demotion:
// the retired 5s tick swept every cloud vault on every tick. Now the periodic
// evaluation fires a catch-up sweep only on an edge — the FIRST observation as
// uploader here — and steady-state healthy re-evaluations do NOT re-sweep. The
// mock keeps reporting the chunk not-cloud-backed, so a sweep WOULD re-upload:
// a stable upload count across repeated evaluations is the proof.
func TestEvaluateCloudHealth_SteadyStateHealthyDoesNotResweep(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))
	defer func() { _ = orch.Scheduler().Stop() }()

	// First observation as uploader → exactly one catch-up sweep uploads it.
	orch.evaluateCloudHealth()
	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("first evaluation should catch up once: want 1 upload, got %d", got)
	}
	// Let the upload job fully settle so a re-sweep would not be blocked by the
	// in-flight dedup guard — isolating the "no edge → no sweep" behavior.
	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultID, chunkID)
	deadline := time.Now().Add(5 * time.Second)
	for orch.Scheduler().HasPendingPrefix(jobName) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	// Steady-state healthy ticks: no edge, no sweep, no re-upload.
	for range 3 {
		orch.evaluateCloudHealth()
	}
	time.Sleep(200 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("steady-state healthy re-evaluations must not re-sweep: want 1 upload, got %d", got)
	}
}

// TestEvaluateCloudHealth_DegradedRecoveryTriggersCatchup pins the
// degraded→healthy recovery edge: chunks that sealed while the cloud store was
// unreachable never got a live upload; the recovery sweep picks them up. While
// degraded, no sweep runs (uploading into an unreachable store is futile).
func TestEvaluateCloudHealth_DegradedRecoveryTriggersCatchup(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	mock.degraded.Store(true)
	mock.degradedErr.Store("connection refused")

	vaultID := glid.New()
	ac := alert.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))
	defer func() { _ = orch.Scheduler().Stop() }()

	// Degraded: alert raised, no sweep.
	orch.evaluateCloudHealth()
	time.Sleep(150 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("no sweep should run while degraded, got %d uploads", got)
	}
	if len(ac.Standing()) != 1 {
		t.Fatalf("degraded vault must raise the cloud-store alert")
	}

	// Recover: degraded→healthy edge fires the catch-up sweep.
	mock.degraded.Store(false)
	orch.evaluateCloudHealth()
	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("recovery must catch up the chunk sealed during the outage, got %d uploads", got)
	}
	if len(ac.Standing()) != 0 {
		t.Fatalf("recovered vault must clear the cloud-store alert")
	}
}

// TestEvaluateCloudHealth_SteadyStateRetriesStuckChunk pins the residual
// periodic retry: a chunk whose upload keeps failing carries a backoff entry,
// and steady-state healthy evaluations keep retrying it (only) — gated by the
// exponential backoff window, not swept blindly every tick.
func TestEvaluateCloudHealth_SteadyStateRetriesStuckChunk(t *testing.T) {
	t.Parallel()

	fixedNow := time.Now()
	clock := func() time.Time { return fixedNow }
	chunkID := chunk.NewChunkID()
	mock := newRegistrarUploaderMock([]chunk.ChunkMeta{
		{ID: chunkID, Sealed: true, CloudBacked: false, WriteStart: fixedNow, WriteEnd: fixedNow},
	})
	mock.alwaysFail = errors.New("cloud store unreachable")

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1", Now: clock})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))
	defer func() { _ = orch.Scheduler().Stop() }()

	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultID, chunkID)

	// First observation → sweep → upload attempt fails → backoff entry recorded.
	orch.evaluateCloudHealth()
	waitBackfillJobDone(t, orch, jobName, mock, 1, 5*time.Second)
	if !orch.vaultHasBackfillFailures(vaultID) {
		t.Fatal("a failed upload must record a backoff entry for retry")
	}

	// Steady-state healthy re-evaluation while inside the backoff window: the
	// vault is still swept (it has a failure), but the chunk is not due, so no
	// new attempt is scheduled.
	orch.evaluateCloudHealth()
	time.Sleep(150 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("a stuck chunk inside its backoff window must not be retried, got %d attempts", got)
	}

	// Advance past the backoff window → the next steady-state evaluation retries.
	fixedNow = fixedNow.Add(unreadableBackoff(1) + time.Second)
	orch.evaluateCloudHealth()
	waitBackfillJobDone(t, orch, jobName, mock, 2, 5*time.Second)
	if got := mock.uploadCallCount(); got != 2 {
		t.Fatalf("a stuck chunk past its backoff window must be retried by the periodic evaluation, got %d attempts", got)
	}
}

// TestOnVaultCtlLeadGained_TriggersCloudUploadCatchup pins the leadership-change
// catch-up: a chunk that sealed while this node was not the uploader never saw
// the live onSeal upload effect here; gaining vault-ctl leadership fires the
// catch-up sweep. This is the placement/ctl-leader change → re-evaluate path.
func TestOnVaultCtlLeadGained_TriggersCloudUploadCatchup(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))
	defer func() { _ = orch.Scheduler().Stop() }()

	orch.onVaultCtlLeadGained(vaultID)

	if got := waitUploadCount(mock, 1, 5*time.Second); got != 1 {
		t.Fatalf("gaining leadership must catch up the sealed chunk, got %d uploads", got)
	}
	calls := mock.uploadCallsCopy()
	if calls[0] != chunkID {
		t.Errorf("uploaded chunk = %s, want %s", calls[0], chunkID)
	}
}

// TestCloudUploadCatchupForVault_SkipsNonUploaderAndUnregistered verifies the
// leadership/snapshot catch-up entry point is a no-op where it must be:
// unregistered vaults, non-cloud vaults, and placement followers.
func TestCloudUploadCatchupForVault_SkipsNonUploaderAndUnregistered(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	defer func() { _ = orch.Scheduler().Stop() }()

	// Unregistered vault — must not panic.
	orch.cloudUploadCatchupForVault(glid.New())

	// Non-cloud file vault (no cloud store): not an uploader.
	plainID := glid.New()
	plainMock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	plainMock.cloudStoreConfigured.Store(false)
	orch.RegisterVault(NewVault(plainID, &VaultInstance{
		VaultID: plainID, Type: "file", Chunks: plainMock,
		IsRaftLeader: func() bool { return true },
	}))
	orch.cloudUploadCatchupForVault(plainID)

	// Placement follower of a cloud-capable vault: has cloud backing but is not
	// the uploader (no CloudStore write access).
	followerID := glid.New()
	followerMock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}
	followerMock.cloudStoreConfigured.Store(false)
	orch.RegisterVault(NewVault(followerID, &VaultInstance{
		VaultID: followerID, Type: "cloud", Chunks: followerMock,
		IsRaftLeader: func() bool { return false },
	}))
	orch.cloudUploadCatchupForVault(followerID)

	time.Sleep(150 * time.Millisecond)
	if got := plainMock.uploadCallCount(); got != 0 {
		t.Fatalf("non-cloud vault must not upload, got %d", got)
	}
	if got := followerMock.uploadCallCount(); got != 0 {
		t.Fatalf("placement follower must not upload, got %d", got)
	}
}

// TestStartCloudHealthAndRateAlerts_RegistersVisibleJob pins operator
// visibility: the retired raw ticker is replaced by a named scheduler job that
// appears in the inspector's job list with a description and a cron schedule.
func TestStartCloudHealthAndRateAlerts_RegistersVisibleJob(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	defer func() { _ = orch.Scheduler().Stop() }()

	if err := orch.startCloudHealthAndRateAlerts(); err != nil {
		t.Fatalf("startCloudHealthAndRateAlerts: %v", err)
	}

	var found *JobInfo
	for _, j := range orch.Scheduler().ListJobs() {
		if j.Name == cloudHealthAndRateAlertsJobName {
			jj := j
			found = &jj
			break
		}
	}
	if found == nil {
		t.Fatalf("cloud-health/rate-alert job %q not registered on the scheduler", cloudHealthAndRateAlertsJobName)
	}
	if found.Description == "" {
		t.Error("scheduler job must carry a human-readable description for the inspector")
	}
	if found.Schedule == "" || found.Schedule == "once" {
		t.Errorf("cloud-health/rate-alert job must be a recurring cron job, got schedule %q", found.Schedule)
	}
}
