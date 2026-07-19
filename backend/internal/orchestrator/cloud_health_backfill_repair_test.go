package orchestrator

// Coverage for gastrolog-4ryguo: a sealed chunk with its GLCB on disk and an
// FSM manifest entry but no chunk-manager registration used to be
// permanently unresolvable — cloud backfill retried it every 5s forever,
// failing "chunk not found", with no repair, no backoff, no alarm, and a
// scheduled/completed INFO pair flooding the job journal every cycle.
//
// These tests pin: (1) the registration gap is repaired via the same
// primitive pipeline sealing uses (registerPipelineGLCB) and the upload then
// succeeds; (2) a failure repair cannot fix backs off exponentially instead
// of retrying every 5s; (3) a failure that persists past the catalog's
// DelayOn raises the cloud-backfill-stuck alarm naming the chunk, vault and
// cause; (4) success clears both the backoff state and the alarm; (5) a
// chunk that vanishes (deleted) drops its state and alarm too — no strand;
// (6) the edge case where the GLCB is genuinely absent from disk (deleted
// out from under the manifest entry) is NOT repaired and still backs off —
// it must not tight-loop either.

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// registrarUploaderMock simulates the chunk-manager registration gap this
// issue fixes: UploadToCloud fails with chunk.ErrChunkNotFound until
// RegisterExternalGLCB registers the chunk — mirroring the real
// file.Manager, whose data.glcb can be on disk with a known FSM manifest
// entry but no local registration until registerPipelineGLCB runs.
type registrarUploaderMock struct {
	chunk.ChunkManager // embedded nil — only List/UploadToCloud/RegisterExternalGLCB used
	chunks             []chunk.ChunkMeta

	mu            sync.Mutex
	registered    map[chunk.ChunkID]bool
	uploadCalls   []chunk.ChunkID
	registerCalls []chunk.ChunkID
	// alwaysFail, when set, makes UploadToCloud keep returning this error
	// regardless of registration state — models a failure repair cannot fix.
	alwaysFail error
}

func newRegistrarUploaderMock(metas []chunk.ChunkMeta) *registrarUploaderMock {
	return &registrarUploaderMock{chunks: metas, registered: map[chunk.ChunkID]bool{}}
}

func (m *registrarUploaderMock) List() ([]chunk.ChunkMeta, error) { return m.chunks, nil }

func (m *registrarUploaderMock) CloudStoreConfigured() bool { return true }
func (m *registrarUploaderMock) CloudDegraded() bool        { return false }
func (m *registrarUploaderMock) CloudDegradedError() string { return "" }

func (m *registrarUploaderMock) UploadToCloud(id chunk.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadCalls = append(m.uploadCalls, id)
	if m.alwaysFail != nil {
		return m.alwaysFail
	}
	if !m.registered[id] {
		return chunk.ErrChunkNotFound
	}
	return nil
}

func (m *registrarUploaderMock) RegisterExternalGLCB(id chunk.ChunkID, _ string, _ chunk.ExternalGLCBInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerCalls = append(m.registerCalls, id)
	m.registered[id] = true
	return nil
}

func (m *registrarUploaderMock) uploadCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploadCalls)
}

func (m *registrarUploaderMock) registerCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.registerCalls)
}

// backfillRepairFixture builds a sealed chunk with a known FSM manifest
// entry, wires a real VaultLifecycleReconciler (the repair path calls its
// registerPipelineGLCB), and — when writeGLCB is true — puts a data.glcb
// file on disk under the vault's pipeline chunk root. The chunk manager
// (registrarUploaderMock) starts with no registration for the chunk,
// reproducing the exact post-restart gap gastrolog-4ryguo describes.
func backfillRepairFixture(t *testing.T, writeGLCB bool) (*Orchestrator, *VaultInstance, chunk.ChunkID, *registrarUploaderMock) {
	t.Helper()
	vaultID := glid.New()
	id := chunk.NewChunkID()
	base := t.TempDir()

	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SegmentsDir: base})
	orch.pipelineVaults[vaultID] = pipelineVaultReg{home: true}

	chunkRoot := filepath.Join(base, vaultID.String(), "chunks")
	if writeGLCB {
		chunkDir := filepath.Join(chunkRoot, id.String())
		if err := os.MkdirAll(chunkDir, 0o755); err != nil {
			t.Fatalf("mkdir chunk dir: %v", err)
		}
		if err := os.WriteFile(chunking.ChunkGLCBPath(chunkRoot, id), []byte("not a real glcb; repair falls back to manifest fields on parse failure"), 0o644); err != nil {
			t.Fatalf("write glcb: %v", err)
		}
	}

	now := time.Now()
	entry := vaultctlfsm.ManifestEntry{
		ID:          id,
		State:       chunk.ChunkStateSealed,
		WriteStart:  now,
		WriteEnd:    now,
		SealedAt:    now,
		RecordCount: 10,
		Bytes:       1024,
	}

	mock := newRegistrarUploaderMock([]chunk.ChunkMeta{
		{ID: id, Sealed: true, CloudBacked: false, WriteStart: now, WriteEnd: now},
	})

	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
		ManifestEntry: func(cid chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
			if cid == id {
				return entry, true
			}
			return vaultctlfsm.ManifestEntry{}, false
		},
	}
	vaultInst.Reconciler = NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())

	return orch, vaultInst, id, mock
}

// waitBackfillJobDone polls until the named scheduler job has fully
// completed (not merely started) AND the mock has recorded at least
// minUploads calls. Waiting on HasPendingPrefix==false (not just the call
// count) matters: it guarantees the job closure — including any repair
// retry, markBackfillFailure, or clearBackfillFailure — has finished
// running, not just that UploadToCloud was entered.
func waitBackfillJobDone(t *testing.T, orch *Orchestrator, jobName string, m *registrarUploaderMock, minUploads int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if m.uploadCallCount() >= minUploads && !orch.Scheduler().HasPendingPrefix(jobName) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backfill job %q did not settle with >= %d uploads within %s (got %d)",
				jobName, minUploads, timeout, m.uploadCallCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------- repair (the disease) ----------

func TestBackfillCloudUploads_RepairsRegistrationMissingChunk(t *testing.T) {
	t.Parallel()
	orch, vaultInst, id, mock := backfillRepairFixture(t, true)
	ac := alert.New()
	orch.alerts = ac

	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultInst.VaultID, id)
	waitBackfillJobDone(t, orch, jobName, mock, 2, 5*time.Second)

	if got := mock.uploadCallCount(); got != 2 {
		t.Fatalf("expected exactly 2 UploadToCloud calls (fail, then a repaired retry that succeeds), got %d", got)
	}
	if got := mock.registerCallCount(); got != 1 {
		t.Fatalf("expected exactly 1 RegisterExternalGLCB call (the repair), got %d", got)
	}

	orch.backfillMu.Lock()
	_, stillFailing := orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if stillFailing {
		t.Fatal("a successfully repaired-and-uploaded chunk must carry no backoff state")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a successfully repaired chunk must raise no alarm, got %v", alerts)
	}
}

// ---------- backoff ----------

func TestMarkBackfillFailureBacksOffExponentially(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	id := chunk.NewChunkID()

	orch.markBackfillFailure(vaultID, id, errors.New("cloud store unreachable"))
	orch.backfillMu.Lock()
	first := *orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if first.failCount != 1 {
		t.Fatalf("failCount = %d, want 1 after first failure", first.failCount)
	}

	orch.markBackfillFailure(vaultID, id, errors.New("cloud store unreachable"))
	orch.backfillMu.Lock()
	second := *orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if second.failCount != 2 {
		t.Fatalf("failCount = %d, want 2: backoff must accumulate across failures, not reset", second.failCount)
	}
	if !second.nextRetry.After(first.nextRetry) {
		t.Fatalf("nextRetry did not grow: first=%s second=%s", first.nextRetry, second.nextRetry)
	}
}

func TestBackfillDueRespectsBackoffWindow(t *testing.T) {
	t.Parallel()
	fixedNow := time.Now()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: func() time.Time { return fixedNow }})
	id := chunk.NewChunkID()

	if !orch.backfillDue(id) {
		t.Fatal("a chunk with no failure history must be due immediately")
	}

	orch.markBackfillFailure(glid.New(), id, errors.New("boom"))
	if orch.backfillDue(id) {
		t.Fatal("a freshly-failed chunk must not be due before its backoff window elapses")
	}

	fixedNow = fixedNow.Add(unreadableBackoff(1) + time.Second)
	if !orch.backfillDue(id) {
		t.Fatal("chunk must become due once its backoff window elapses")
	}
}

// TestBackfillCloudUploads_SkipsSchedulingDuringBackoff pins the log-hygiene
// fix directly: while a chunk is inside its backoff window,
// backfillCloudUploads must not even call scheduler.RunOnce for it — that is
// what stops the schedule/complete INFO pair from repeating every 5s.
func TestBackfillCloudUploads_SkipsSchedulingDuringBackoff(t *testing.T) {
	t.Parallel()
	fixedNow := time.Now()
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	mock := newRegistrarUploaderMock([]chunk.ChunkMeta{
		{ID: chunkID, Sealed: true, CloudBacked: false, WriteStart: fixedNow, WriteEnd: fixedNow},
	})
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: func() time.Time { return fixedNow }})
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"))

	vaultInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: mock, IsRaftLeader: func() bool { return true }}
	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(100 * time.Millisecond)

	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultID, chunkID)
	if orch.Scheduler().HasPendingPrefix(jobName) {
		t.Fatal("a chunk inside its backoff window must not have a job scheduled")
	}
	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("expected 0 upload attempts while backing off, got %d", got)
	}
}

// ---------- alarm ----------

func TestBackfillPersistentFailureRaisesAlarm(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	id := chunk.NewChunkID()

	typ, ok := alert.TypeByID("cloud-backfill-stuck")
	if !ok {
		t.Fatal("cloud-backfill-stuck must be registered in the alarm catalog")
	}

	orch.markBackfillFailure(vaultID, id, errors.New("boom"))
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a single failure must not yet annunciate (DelayOn not elapsed): got %v", alerts)
	}

	// Advance past the catalog's DelayOn and re-raise (a later failure) —
	// re-raises refresh detail but do not restart the suppression window.
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("boom again"))

	alerts := ac.Standing()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 standing alarm past DelayOn, got %d: %v", len(alerts), alerts)
	}
	if alerts[0].TypeID != "cloud-backfill-stuck" {
		t.Fatalf("alarm type = %q, want cloud-backfill-stuck", alerts[0].TypeID)
	}
	wantID := fmt.Sprintf("cloud-backfill-stuck:%s", id)
	if alerts[0].ID != wantID {
		t.Fatalf("alarm ID = %q, want %q", alerts[0].ID, wantID)
	}
	if !strings.Contains(alerts[0].Detail, id.String()) || !strings.Contains(alerts[0].Detail, vaultID.String()) {
		t.Fatalf("alarm detail must name the chunk and vault: %q", alerts[0].Detail)
	}
	if !strings.Contains(alerts[0].Detail, "boom again") {
		t.Fatalf("alarm detail must name the cause: %q", alerts[0].Detail)
	}
	if alerts[0].Priority != alert.High {
		t.Fatalf("priority = %v, want High", alerts[0].Priority)
	}
}

// ---------- success clears / vanished chunk clears ----------

func TestClearBackfillFailureDropsStateAndAlarm(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	id := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	orch.markBackfillFailure(vaultID, id, errors.New("boom"))
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("boom again"))
	if len(ac.Standing()) != 1 {
		t.Fatal("setup: expected the alarm to be standing before clearing")
	}

	orch.clearBackfillFailure(id)

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("backoff state must be dropped on success")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("alarm must clear on success, got %v", alerts)
	}
}

func TestPruneVanishedBackfillFailuresDropsDeletedChunkState(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	stillThere := chunk.NewChunkID()
	deleted := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	for _, id := range []chunk.ChunkID{stillThere, deleted} {
		orch.markBackfillFailure(vaultID, id, errors.New("boom"))
	}
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	for _, id := range []chunk.ChunkID{stillThere, deleted} {
		orch.markBackfillFailure(vaultID, id, errors.New("boom again"))
	}
	if len(ac.Standing()) != 2 {
		t.Fatalf("setup: want 2 standing alarms, got %d", len(ac.Standing()))
	}

	// Only stillThere is present in the vault's current raw candidate view —
	// deleted has been retired (e.g. by retention) and no longer appears.
	orch.pruneVanishedBackfillFailures(vaultID, []chunk.ChunkMeta{{ID: stillThere}})

	orch.backfillMu.Lock()
	_, stillPresent := orch.backfillFailures[stillThere]
	_, deletedPresent := orch.backfillFailures[deleted]
	orch.backfillMu.Unlock()
	if !stillPresent {
		t.Fatal("a chunk still in the candidate list must keep its backoff state")
	}
	if deletedPresent {
		t.Fatal("a chunk no longer in the candidate list (deleted) must drop its backoff state — no strand")
	}

	alerts := ac.Standing()
	if len(alerts) != 1 || alerts[0].ID != fmt.Sprintf("cloud-backfill-stuck:%s", stillThere) {
		t.Fatalf("expected only stillThere's alarm to remain standing, got %v", alerts)
	}
}

// ---------- edge: GLCB deleted out from under the manifest entry ----------

// TestBackfillCloudUploads_GLCBMissingDoesNotRepairAndBacksOff pins the
// acceptance edge case: a manifest entry says sealed, but the GLCB bytes
// are genuinely gone from disk (deleted out from under it). This is not
// repairable — repairAndRetryBackfill must not call registerPipelineGLCB —
// and it must still back off rather than tight-looping every 5s.
func TestBackfillCloudUploads_GLCBMissingDoesNotRepairAndBacksOff(t *testing.T) {
	t.Parallel()
	orch, vaultInst, id, mock := backfillRepairFixture(t, false) // no GLCB written
	orch.alerts = alert.New()

	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultInst.VaultID, id)
	waitBackfillJobDone(t, orch, jobName, mock, 1, 5*time.Second)

	// Give a moment past settlement to make sure no second attempt sneaks in.
	time.Sleep(100 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("expected exactly 1 upload attempt (no repair retry when bytes are absent), got %d", got)
	}
	if got := mock.registerCallCount(); got != 0 {
		t.Fatalf("a genuinely-absent GLCB must not trigger a repair registration, got %d calls", got)
	}

	orch.backfillMu.Lock()
	entry := orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if entry == nil || entry.failCount != 1 {
		t.Fatalf("expected backoff state recorded after the unrepairable failure, got %+v", entry)
	}

	// A second sweep before the backoff window elapses must not reschedule
	// the job — this is the tight-loop the edge case guards against.
	orch.backfillCloudUploads(vaultInst)
	time.Sleep(100 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("chunk must not be retried before its backoff window elapses, got %d upload calls", got)
	}
}
