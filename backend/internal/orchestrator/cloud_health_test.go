package orchestrator

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// mockCloudChunkManager is a minimal ChunkManager that also implements
// cloudHealthChecker and ChunkCloudUploader for testing cloud health
// evaluation and backfill scheduling.
type mockCloudChunkManager struct {
	chunk.ChunkManager // embedded nil — only List/UploadToCloud used
	degraded           atomic.Bool
	degradedErr        atomic.Value // string
	chunks             []chunk.ChunkMeta
	uploadCalls        []chunk.ChunkID
}

func (m *mockCloudChunkManager) CloudDegraded() bool       { return m.degraded.Load() }
func (m *mockCloudChunkManager) CloudDegradedError() string {
	if v := m.degradedErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}
func (m *mockCloudChunkManager) List() ([]chunk.ChunkMeta, error) {
	return m.chunks, nil
}
func (m *mockCloudChunkManager) UploadToCloud(id chunk.ChunkID) error {
	m.uploadCalls = append(m.uploadCalls, id)
	return nil
}

// ---------- evaluateCloudHealth ----------

func TestEvaluateCloudHealth_SetsAlertWhenDegraded(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	tierID := uuid.Must(uuid.NewV7())
	mock := &mockCloudChunkManager{}
	mock.degraded.Store(true)
	mock.degradedErr.Store("connection refused")

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	tier := &TierInstance{TierID: tierID, Type: "cloud", Chunks: mock}
	orch.RegisterVault(NewVault(uuid.Must(uuid.NewV7()), tier))

	orch.evaluateCloudHealth()

	alerts := ac.Active()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	wantID := fmt.Sprintf("cloud-store:%s", tierID)
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
	tierID := uuid.Must(uuid.NewV7())
	mock := &mockCloudChunkManager{}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac
	tier := &TierInstance{TierID: tierID, Type: "cloud", Chunks: mock}
	orch.RegisterVault(NewVault(uuid.Must(uuid.NewV7()), tier))

	// Simulate prior degraded alert.
	alertID := fmt.Sprintf("cloud-store:%s", tierID)
	ac.Set(alertID, alert.Error, "cloud", "was broken")

	// Now cloud is healthy (degraded=false, default).
	orch.evaluateCloudHealth()

	if alerts := ac.Active(); len(alerts) != 0 {
		t.Fatalf("expected 0 alerts after recovery, got %d: %v", len(alerts), alerts)
	}
}

func TestEvaluateCloudHealth_SkipsNonCloudTiers(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	mock := &mockCloudChunkManager{}
	mock.degraded.Store(true)
	mock.degradedErr.Store("boom")

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac

	// Type is "file", not "cloud" — should be skipped.
	tier := &TierInstance{TierID: uuid.Must(uuid.NewV7()), Type: "file", Chunks: mock}
	orch.RegisterVault(NewVault(uuid.Must(uuid.NewV7()), tier))

	orch.evaluateCloudHealth()

	if alerts := ac.Active(); len(alerts) != 0 {
		t.Fatalf("expected 0 alerts for non-cloud tier, got %d", len(alerts))
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
	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	tierID := uuid.Must(uuid.NewV7())
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	tier := &TierInstance{
		TierID:       tierID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(tier)

	// Wait for the scheduler job to run.
	orch.Scheduler().Start()
	time.Sleep(100 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 1 {
		t.Fatalf("expected 1 upload call, got %d", len(mock.uploadCalls))
	}
	if mock.uploadCalls[0] != chunkID {
		t.Errorf("uploaded chunk = %s, want %s", mock.uploadCalls[0], chunkID)
	}
}

func TestBackfillCloudUploads_SkipsCloudBacked(t *testing.T) {
	t.Parallel()

	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: true, CloudBacked: true,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(tier)

	orch.Scheduler().Start()
	time.Sleep(50 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 0 {
		t.Fatalf("expected 0 upload calls for cloud-backed chunk, got %d", len(mock.uploadCalls))
	}
}

func TestBackfillCloudUploads_SkipsUnsealed(t *testing.T) {
	t.Parallel()

	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: false, CloudBacked: false,
				WriteStart: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(tier)

	orch.Scheduler().Start()
	time.Sleep(50 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 0 {
		t.Fatalf("expected 0 upload calls for unsealed chunk, got %d", len(mock.uploadCalls))
	}
}

func TestBackfillCloudUploads_SkipsWhenFSMOverlaySaysCloudBacked(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
		// FSM overlay says cloud-backed — backfill should skip.
		OverlayFromFSM: func(m chunk.ChunkMeta) chunk.ChunkMeta {
			m.CloudBacked = true
			return m
		},
	}

	orch.backfillCloudUploads(tier)

	orch.Scheduler().Start()
	time.Sleep(50 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 0 {
		t.Fatalf("expected 0 uploads when FSM overlay says cloud-backed, got %d", len(mock.uploadCalls))
	}
}

func TestBackfillCloudUploads_RunsOnAnyNode(t *testing.T) {
	t.Parallel()

	ac := alert.New()
	chunkID := chunk.NewChunkID()
	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	tierID := uuid.Must(uuid.NewV7())
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = ac

	// Not the Raft leader — backfill should still run (any node
	// with non-cloud-backed data participates). See gastrolog-68fqk.
	tier := &TierInstance{
		TierID:       tierID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return false },
	}
	orch.RegisterVault(NewVault(uuid.Must(uuid.NewV7()), tier))

	orch.evaluateCloudHealth()

	orch.Scheduler().Start()
	time.Sleep(100 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 1 {
		t.Fatalf("expected 1 upload on non-Raft-leader node with data, got %d", len(mock.uploadCalls))
	}
}

func TestBackfillCloudUploads_DeduplicatesPendingJobs(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()
	mock := &mockCloudChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkID, Sealed: true, CloudBacked: false,
				WriteStart: time.Now(), WriteEnd: time.Now()},
		},
	}

	tierID := uuid.Must(uuid.NewV7())
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	orch.alerts = alert.New()
	tier := &TierInstance{
		TierID:       tierID,
		Type:         "cloud",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	// Call backfill twice — should only schedule once.
	orch.backfillCloudUploads(tier)
	orch.backfillCloudUploads(tier)

	orch.Scheduler().Start()
	time.Sleep(100 * time.Millisecond)
	_ = orch.Scheduler().Stop()

	if len(mock.uploadCalls) != 1 {
		t.Fatalf("expected 1 upload (deduped), got %d", len(mock.uploadCalls))
	}
}
