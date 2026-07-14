package orchestrator

import (
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

	alerts := ac.Active()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	wantID := fmt.Sprintf("cloud-store:%s", vaultID)
	if alerts[0].ID != wantID {
		t.Errorf("alert ID = %q, want %q", alerts[0].ID, wantID)
	}
	if alerts[0].Severity != alert.Error {
		t.Errorf("severity = %d, want Error(%d)", alerts[0].Severity, alert.Error)
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

	// Simulate prior degraded alert.
	alertID := fmt.Sprintf("cloud-store:%s", vaultID)
	ac.Set(alertID, alert.Error, "cloud", "was broken")

	// Now cloud is healthy (degraded=false, default).
	orch.evaluateCloudHealth()

	if alerts := ac.Active(); len(alerts) != 0 {
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

	if alerts := ac.Active(); len(alerts) != 0 {
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

	alerts := ac.Active()
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
