package orchestrator

// Coverage for gastrolog-4ryguo's failure-tracking machinery. The original
// registration gap — a sealed chunk with its GLCB on disk and an FSM manifest
// entry but no chunk-manager registration, permanently unresolvable, cloud
// backfill retrying every 5s forever — is now closed at the source:
// Manager.uploadToCloud resolves the chunk through the lazy on-miss GLCB
// resolver (lookupMeta) instead of a raw m.metas read, so a freshly-sealed
// on-disk external chunk uploads with no register-first step and no repair
// (gastrolog-34kmv retired the eager repairAndRetryBackfill). The multi-node
// self-resolving upload is pinned in pipeline_cloud_upload_test.go.
//
// These tests pin the backoff/alarm accounting that remains: (1) EVERY failure
// — GLCB on disk or GLCB-absent (build-lag / genuinely deleted) alike — backs
// off exponentially instead of retrying every 5s, one map, one strand-safe
// lifecycle; (2) only a GLCB-on-disk failure that persists past the catalog's
// DelayOn raises the cloud-backfill-stuck alarm naming the chunk, vault and
// cause — a GLCB-absent failure backs off the same way but NEVER alarms, since
// build-lag and a genuinely-deleted GLCB are indistinguishable by an os.Stat
// and neither should page an operator; (3) success clears both the backoff
// state and the alarm (if any), whether the chunk resolved through
// backfillCloudUploads itself or was uploaded by the PRIMARY path
// (schedulePipelineCloudUpload/onSeal) and observed cloud-backed on the next
// sweep; (4) a chunk that vanishes (deleted), or a vault this node stops
// running backfill for (leadership handoff, teardown), drops all state and
// alarms too — no strand.

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

// The former TestBackfillCloudUploads_RepairsRegistrationMissingChunk is
// retired with the eager repair (gastrolog-34kmv): a freshly-sealed on-disk
// external chunk now uploads with no register-first step because
// Manager.uploadToCloud self-resolves it via the lazy on-miss GLCB resolver.
// That self-resolving upload is pinned end-to-end against a real chunk
// manager in pipeline_cloud_upload_test.go
// (TestSchedulePipelineCloudUpload_LeaderUploadsWithoutEagerRegistration).

// ---------- backoff ----------

func TestMarkBackfillFailureBacksOffExponentially(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	id := chunk.NewChunkID()

	orch.markBackfillFailure(vaultID, id, errors.New("cloud store unreachable"), true)
	orch.backfillMu.Lock()
	first := *orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if first.failCount != 1 {
		t.Fatalf("failCount = %d, want 1 after first failure", first.failCount)
	}

	orch.markBackfillFailure(vaultID, id, errors.New("cloud store unreachable"), true)
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

	orch.markBackfillFailure(glid.New(), id, errors.New("boom"), true)
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
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"), true)

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

	orch.markBackfillFailure(vaultID, id, errors.New("boom"), true)
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a single failure must not yet annunciate (DelayOn not elapsed): got %v", alerts)
	}

	// Advance past the catalog's DelayOn and re-raise (a later failure) —
	// re-raises refresh detail but do not restart the suppression window.
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("boom again"), true)

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

	orch.markBackfillFailure(vaultID, id, errors.New("boom"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("boom again"), true)
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
		orch.markBackfillFailure(vaultID, id, errors.New("boom"), true)
	}
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	for _, id := range []chunk.ChunkID{stillThere, deleted} {
		orch.markBackfillFailure(vaultID, id, errors.New("boom again"), true)
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

// ---------- build-lag / genuinely-deleted GLCB: back off, never alarm (review follow-up #2) ----------
//
// A sealed-in-FSM chunk whose local GLCB build hasn't finished yet fails
// UploadToCloud with the same chunk.ErrChunkNotFound as the genuine
// registration-missing case — os.Stat alone cannot tell "will exist in a
// few seconds" from "gone forever" (a GLCB genuinely deleted out from under
// the manifest entry). A first review pass excluded both from the failure
// track entirely; that over-corrected — a permanently-missing GLCB then
// tight-loops at the 5s sweep cadence forever with no failure entry to ever
// gate it, which the issue's acceptance explicitly forbids. The corrected
// shape: BOTH cases get a failure entry with the same exponential backoff —
// one map, one strand-safe lifecycle (cross-path clear, the vault-scoped
// purges, and the vanished-candidate prune all apply regardless). The
// ONLY distinction is alarm eligibility: only a registration-missing
// failure (GLCB verifiably on disk) escalates to the cloud-backfill-stuck
// alarm. A GLCB-absent failure backs off the same way but never alarms —
// build-lag entries clear entirely via the chunkIsCloudBacked cross-path
// once the primary upload (schedulePipelineCloudUpload/onSeal) lands, and a
// genuinely-deleted GLCB backs off to the cap without ever flooding the
// scheduler journal (backfillDue still gates scheduling) or paging an
// operator for state the primary path owns.

// TestBackfillCloudUploads_GLCBAbsentBacksOffWithoutAlarm pins the
// corrected build-lag/deleted-GLCB shape end-to-end: no repair attempt, a
// backoff entry IS created and grows across repeated sweeps, the chunk
// stops being scheduled once inside its backoff window (no tight loop), and
// no alarm is ever raised — even once escalated well past where an
// alarm-eligible failure would annunciate.
func TestBackfillCloudUploads_GLCBAbsentBacksOffWithoutAlarm(t *testing.T) {
	t.Parallel()
	orch, vaultInst, id, mock := backfillRepairFixture(t, false) // no GLCB written
	ac := alert.New()
	orch.alerts = ac

	orch.backfillCloudUploads(vaultInst)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	jobName := fmt.Sprintf("cloud-backfill:%s:%s", vaultInst.VaultID, id)
	waitBackfillJobDone(t, orch, jobName, mock, 1, 5*time.Second)

	if got := mock.registerCallCount(); got != 0 {
		t.Fatalf("a GLCB absent from disk must not trigger a repair registration, got %d calls", got)
	}

	orch.backfillMu.Lock()
	first := orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if first == nil || first.failCount != 1 {
		t.Fatalf("expected a backoff entry recorded after the first failure, got %+v", first)
	}
	if first.alarmEligible {
		t.Fatal("a GLCB-absent failure must not be alarm-eligible")
	}
	if orch.backfillDue(id) {
		t.Fatal("a freshly-failed chunk must not be due before its backoff window elapses — this is the tight-loop guard")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a GLCB-absent failure must never alarm, got %v", alerts)
	}

	// A second sweep before the backoff window elapses must not reschedule
	// the job — no tight loop.
	orch.backfillCloudUploads(vaultInst)
	time.Sleep(100 * time.Millisecond)
	if got := mock.uploadCallCount(); got != 1 {
		t.Fatalf("chunk must not be retried before its backoff window elapses, got %d upload calls", got)
	}

	// Force the entry well up the backoff ladder directly (no
	// wall-clock races) — the same escalation depth that would annunciate
	// an alarm-eligible failure via the catalog's DelayOn. Still no alarm.
	for range 5 {
		orch.markBackfillFailure(vaultInst.VaultID, id, errors.New("still no glcb"), false)
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a GLCB-absent failure must never alarm even after repeated escalation, got %v", alerts)
	}
	orch.backfillMu.Lock()
	escalated := orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if escalated.failCount != 6 {
		t.Fatalf("failCount = %d, want 6: backoff must still accumulate for the non-alarming case", escalated.failCount)
	}
}

// TestMarkBackfillFailureAlarmEligibleGatesAlarmNotBackoff is the direct
// unit-level pin for the same distinction: backoff state is identical
// regardless of alarmEligible; only the alarm differs.
func TestMarkBackfillFailureAlarmEligibleGatesAlarmNotBackoff(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	id := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	orch.markBackfillFailure(vaultID, id, errors.New("no glcb"), false)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("still no glcb"), false)

	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("alarmEligible=false must never raise the alarm, got %v", alerts)
	}
	orch.backfillMu.Lock()
	entry := *orch.backfillFailures[id]
	orch.backfillMu.Unlock()
	if entry.failCount != 2 {
		t.Fatalf("failCount = %d, want 2: backoff accumulates the same regardless of alarm eligibility", entry.failCount)
	}
	if entry.alarmEligible {
		t.Fatal("entry.alarmEligible must reflect the latest (false) observation")
	}

	// A later failure whose onDisk observation flips to true (e.g. the
	// build finished but something else — cloud store down — now fails)
	// must become alarm-eligible from that point on. The alarm's own
	// DelayOn only starts counting from its first Raise (the two prior
	// alarmEligible=false calls never raised anything for this ID), so a
	// second alarm-eligible failure past that fresh window is what actually
	// annunciates.
	orch.markBackfillFailure(vaultID, id, errors.New("cloud store down"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, id, errors.New("cloud store still down"), true)
	if alerts := ac.Standing(); len(alerts) != 1 {
		t.Fatalf("a failure that becomes alarm-eligible must raise the alarm once its own DelayOn elapses, got %d standing", len(alerts))
	}
}

// ---------- strand fix 1: cross-path success (review follow-up) ----------

// TestBackfillCloudUploads_CrossPathSuccessClearsEntryAndAlarm pins the
// first Critical strand: a chunk with a standing backfill failure entry and
// alarm gets uploaded by the PRIMARY path (schedulePipelineCloudUpload /
// onSeal, not backfillCloudUploads) and becomes CloudBacked. The next
// backfillCloudUploads sweep sees it via chunkIsCloudBacked and must clear
// the entry+alarm right there — that continue is the only place the sweep
// ever visits an already-resolved chunk again; without an explicit clear at
// that point nothing else would ever remove the stranded state.
func TestBackfillCloudUploads_CrossPathSuccessClearsEntryAndAlarm(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	mock := newRegistrarUploaderMock([]chunk.ChunkMeta{
		{ID: chunkID, Sealed: true, CloudBacked: false, WriteStart: clockNow, WriteEnd: clockNow},
	})
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	typ, _ := alert.TypeByID("cloud-backfill-stuck")
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom again"), true)
	if len(ac.Standing()) != 1 {
		t.Fatal("setup: expected the alarm to be standing before the cross-path success")
	}

	// The chunk now reads cloud-backed — how the sweep learns the primary
	// path already resolved it. (The grounding of cloud-backed truth from the
	// FSM on a follower is covered by grounded_meta_test.go and the multi-node
	// manifest reliability tests; here the mock reports it directly.)
	// vaultInst.Chunks itself is never told to upload it.
	mock.chunks[0].CloudBacked = true
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("a chunk resolved by the primary path must have its backoff state cleared on the next sweep")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("a chunk resolved by the primary path must have its alarm cleared, got %v", alerts)
	}
	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("an already cloud-backed chunk must not be re-uploaded by the sweep, got %d calls", got)
	}
}

// TestBackfillCloudUploads_CrossPathSuccessClearsBuildLagEntry is the
// GLCB-absent sibling of the above: a build-lag entry (alarmEligible=false,
// so no alarm was ever raised for it) must clear the same way once the
// primary path resolves the chunk — the chunkIsCloudBacked cross-path clear
// in backfillCloudUploads is unconditional on alarmEligible, since it's the
// ONLY thing that ever removes a build-lag entry (it has no DelayOn
// escalation to fall back on, unlike the alarm-eligible case).
func TestBackfillCloudUploads_CrossPathSuccessClearsBuildLagEntry(t *testing.T) {
	t.Parallel()
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	now := time.Now()
	mock := newRegistrarUploaderMock([]chunk.ChunkMeta{
		{ID: chunkID, Sealed: true, CloudBacked: false, WriteStart: now, WriteEnd: now},
	})
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	ac := alert.New()
	orch.alerts = ac

	orch.markBackfillFailure(vaultID, chunkID, errors.New("no glcb yet"), false)
	orch.backfillMu.Lock()
	_, hadEntry := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if !hadEntry {
		t.Fatal("setup: expected a backoff entry recorded before the cross-path success")
	}
	if len(ac.Standing()) != 0 {
		t.Fatal("setup: a build-lag entry must never have raised an alarm")
	}

	// The chunk now reads cloud-backed (primary path resolved it). Grounding
	// of this truth from the FSM is covered separately (see the sibling test).
	mock.chunks[0].CloudBacked = true
	vaultInst := &VaultInstance{
		VaultID:      vaultID,
		Type:         "file",
		Chunks:       mock,
		IsRaftLeader: func() bool { return true },
	}

	orch.backfillCloudUploads(vaultInst)

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("a build-lag entry resolved by the primary path must clear on the next sweep — same as an alarm-eligible entry")
	}
	if got := mock.uploadCallCount(); got != 0 {
		t.Fatalf("an already cloud-backed chunk must not be re-uploaded by the sweep, got %d calls", got)
	}
}

// ---------- strand fix 2: vault leaves this node (review follow-up) ----------

// TestEvaluateCloudHealth_PurgesBackfillFailuresForRemovedVault pins the
// second Critical strand's simplest form: a vault with a standing backfill
// failure+alarm is not registered on this node at all (removed from
// config, or placement/leadership moved it entirely away before this test
// even runs). backfillCloudUploads is never called for it again, so nothing
// but evaluateCloudHealth's per-sweep GC could ever clear the stranded
// state.
func TestEvaluateCloudHealth_PurgesBackfillFailuresForRemovedVault(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom again"), true)
	if len(ac.Standing()) != 1 {
		t.Fatal("setup: expected the alarm to be standing before the vault stops being visited")
	}

	// No vault registered for vaultID at all.
	orch.evaluateCloudHealth()

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("a vault this node no longer runs backfill for must have its stranded backoff entry purged")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("the alarm for a vault no longer visited must clear, got %v", alerts)
	}
}

// TestEvaluateCloudHealth_PurgesBackfillFailuresForNonLeaderVault covers the
// leadership-handoff shape of the same strand: the vault is still
// registered on this node, but this node is no longer the placement
// leader/uploader for it (CloudStoreConfigured()==false, e.g. a follower
// after a handoff) — vaultInstRunsCloudBackfill is false, so
// backfillCloudUploads is skipped for it just as if it were removed.
func TestEvaluateCloudHealth_PurgesBackfillFailuresForNonLeaderVault(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom again"), true)
	if len(ac.Standing()) != 1 {
		t.Fatal("setup: expected the alarm to be standing before leadership moved away")
	}

	mock := &mockCloudBackedChunkManager{}
	mock.cloudStoreConfigured.Store(false) // follower now — no upload access
	followerInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: mock, IsRaftLeader: func() bool { return true }}
	orch.RegisterVault(NewVault(glid.New(), followerInst))

	orch.evaluateCloudHealth()

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("a vault this node lost backfill leadership for must have its stranded backoff entry purged")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("the alarm for a vault this node no longer uploads for must clear, got %v", alerts)
	}
}

// ---------- strand fix 3: vault teardown (review follow-up minor) ----------

// TestTeardownVaultPurgesBackfillFailures pins the reviewer's Minor: vault
// teardown must cancel pending cloud-backfill jobs and purge that vault's
// failure entries immediately, the same as it already does for
// post-seal/compress/index-build jobs — not wait for the next
// evaluateCloudHealth sweep.
func TestTeardownVaultPurgesBackfillFailures(t *testing.T) {
	t.Parallel()
	clockNow := time.Now()
	clock := func() time.Time { return clockNow }
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Now: clock})
	ac := alert.NewWithClock(clock)
	orch.alerts = ac

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	typ, _ := alert.TypeByID("cloud-backfill-stuck")

	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom"), true)
	clockNow = clockNow.Add(typ.DelayOn + time.Second)
	orch.markBackfillFailure(vaultID, chunkID, errors.New("boom again"), true)
	if len(ac.Standing()) != 1 {
		t.Fatal("setup: expected the alarm to be standing before teardown")
	}

	inst := newMemoryInstance(t, vaultID)
	v := &Vault{ID: vaultID, Instance: inst}
	orch.vaults[vaultID] = v

	// A pending cloud-backfill job for this vault must be cancelled too —
	// block it on an unclosed channel so it's still pending when teardown runs.
	jobName := "cloud-backfill:" + vaultID.String() + ":" + chunkID.String()
	blocked := make(chan struct{})
	defer close(blocked) // let the job's goroutine exit; avoid leaking it
	if err := orch.Scheduler().RunOnce(jobName, func() { <-blocked }); err != nil {
		t.Fatalf("schedule blocking job: %v", err)
	}
	if !orch.Scheduler().HasPendingPrefix(jobName) {
		t.Fatal("setup: job should be pending immediately after RunOnce")
	}

	orch.teardownVault(vaultID, v)

	if orch.Scheduler().HasPendingPrefix(jobName) {
		t.Fatal("teardownVault must cancel pending cloud-backfill jobs for the vault")
	}

	orch.backfillMu.Lock()
	_, present := orch.backfillFailures[chunkID]
	orch.backfillMu.Unlock()
	if present {
		t.Fatal("teardownVault must purge the vault's backfill failure entries immediately")
	}
	if alerts := ac.Standing(); len(alerts) != 0 {
		t.Fatalf("teardownVault must clear the vault's backfill alarms immediately, got %v", alerts)
	}
}
