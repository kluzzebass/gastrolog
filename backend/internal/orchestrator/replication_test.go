package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"fmt"
	"os"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// ---------- fake instance replicator that records operations ----------

type replicationFakeReplicator struct {
	replicatedChunks []chunk.ChunkID
}

func (m *replicationFakeReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, chunkID chunk.ChunkID, _ chunk.RecordIterator) error {
	m.replicatedChunks = append(m.replicatedChunks, chunkID)
	return nil
}
func (m *replicationFakeReplicator) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

// ---------- helpers ----------

func newReplicationInstance(t *testing.T, vaultID glid.GLID, followers []system.ReplicationTarget, isFollower bool, leaderNodeID string) *VaultInstance {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &VaultInstance{
		VaultID:         vaultID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           query.New(cm, im, nil),
		IsFollower:      isFollower,
		LeaderNodeID:    leaderNodeID,
		FollowerTargets: followers,
	}
}

func testRecord(raw string) chunk.Record {
	return chunk.Record{
		SourceTS: time.Now(),
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
	}
}

// TestScheduleReplicationDescribesBeforeScheduling checks the real call site
// follows the ordering the scheduler contract requires (see
// TestDescribeBeforeRunOnceLabelsEventAndReleases): the replication job's
// label must be on its Scheduled event, and the descriptions entry must be
// gone once the job finishes. Describing after RunOnce lost the label and
// leaked one entry per replicated chunk — unbounded on a busy leader.
func TestScheduleReplicationDescribesBeforeScheduling(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	sub, cancel := orch.Scheduler().Events().Subscribe()
	defer cancel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	name := fmt.Sprintf("replicate:%s:%s", vaultID, chunkID)

	// No transferrer is wired, so the job body returns immediately — this
	// test is about the registration order, not the replication itself.
	orch.scheduleReplication(vaultID, chunkID, []system.ReplicationTarget{{NodeID: "node-2"}})

	info := awaitSchedulerJobScheduled(t, sub, name)
	if info.Description == "" {
		t.Error("replication job's Scheduled event carries no description — Describe ran after scheduling")
	}

	awaitSchedulerJobDone(t, sub, name)
	if hasDescription(orch.Scheduler(), name) {
		t.Error("replication job's description survived completion — one leaked entry per chunk")
	}
}

// ================================================================
// SEAL ACTIVE CHUNK TESTS
// ================================================================

func TestSealActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	vault.Name = "seal-test"
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("seal-me")); err != nil {
		t.Fatal(err)
	}

	active := vault.Instance.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	chunkID := active.ID

	if err := orch.SealActiveChunk(vaultID, chunkID); err != nil {
		t.Fatal(err)
	}

	newActive := vault.Instance.Chunks.Active()
	if newActive != nil && newActive.ID == chunkID {
		t.Error("expected active chunk to change after seal")
	}
}

func TestSealActiveChunkMismatchSkipsSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	vault.Name = "mismatch"
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// Seal with a wrong chunk ID — should be a no-op (the expected chunk
	// was already rotated by the follower's own rotation policy).
	wrongID := chunkIDAt(time.Now().Add(-1 * time.Hour))
	if err := orch.SealActiveChunk(vaultID, wrongID); err != nil {
		t.Fatal(err)
	}

	metas, _ := vault.Instance.Chunks.List()
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed != 0 {
		t.Error("expected NO seal when chunk ID doesn't match — seal should be skipped")
	}
}

func TestSealActiveChunkNoActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)

	// No records appended — no active chunk.
	err := orch.SealActiveChunk(vaultID, chunk.ChunkID{})
	if err != nil {
		t.Errorf("expected nil error for no active chunk, got %v", err)
	}
}

// ================================================================
// CATCHUP TESTS
// ================================================================

func TestCatchupSecondaryNoSealedChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// No sealed chunks — catchup should be a no-op.
	err := orch.catchupFollower(context.Background(), vaultID, "node-2")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCatchupSecondaryOnlyPrimary(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	// This is a follower — should not initiate catchup.
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, true, "node-2"))
	orch.RegisterVault(vault)

	err := orch.catchupFollower(context.Background(), vaultID, "node-3")
	if err != nil {
		t.Fatalf("expected nil (no-op) for follower, got %v", err)
	}
}

func TestCatchupSecondaryNoTransferrer(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)
	// No transferrer set.

	err := orch.catchupFollower(context.Background(), vaultID, "node-2")
	if err == nil {
		t.Fatal("expected error for missing transferrer")
	}
}

// TestCatchupSkipsFSMRetiredChunks guards the FSM-manifest filter in
// catchupFollower. The leader's on-disk chunk list is not an authoritative
// set: it can include chunks the instance Raft FSM has already retired
// (DeleteChunk applied) but whose local file hasn't been unlinked yet.
// Shipping those orphans to the follower means the follower's reconcile loop
// deletes them within ~1 minute. Net result: catchup work wasted, follower
// under-replicated, repeat forever.
//
// catchupFollower therefore filters the catchup list by
// instance.ListManifest() — the FSM's authoritative view of what should
// exist. This test populates an instance with 3 sealed chunks, configures
// ListManifest to return only 2 of them (simulating the FSM having retired
// the third), and asserts that catchup transferred only the 2
// manifest-included chunks.
func TestCatchupSkipsFSMRetiredChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vaultInst := newReplicationInstance(t, vaultID, nil, false, "")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal three chunks, capturing each chunk ID.
	var ids []chunk.ChunkID
	for i := 0; i < 3; i++ {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := vaultInst.Chunks.Active()
		if active == nil {
			t.Fatalf("chunk %d: no active chunk after append", i)
		}
		id := active.ID
		if err := orch.SealActiveChunk(vaultID, id); err != nil {
			t.Fatalf("seal chunk %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 sealed chunks, got %d", len(ids))
	}

	// Confirm all 3 chunks are present on disk (the leader's local view).
	metas, err := vaultInst.Chunks.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	sealedCount := 0
	for _, m := range metas {
		if m.Sealed {
			sealedCount++
		}
	}
	if sealedCount != 3 {
		t.Fatalf("expected 3 sealed chunks on disk, got %d", sealedCount)
	}

	// Configure the FSM manifest to return only chunks 0 and 2, simulating
	// chunk 1 being retired by the FSM (DeleteChunk applied) while still
	// existing on disk in the brief window before unlink.
	vaultInst.ListManifest = func() []chunk.ChunkID {
		return []chunk.ChunkID{ids[0], ids[2]}
	}

	if err := orch.catchupFollower(context.Background(), vaultID, "node-2"); err != nil {
		t.Fatalf("catchupFollower: %v", err)
	}

	// Catchup must have transferred ONLY the 2 manifest-included chunks.
	// The retired chunk (ids[1]) must NOT have been transferred — sending
	// it would re-create the orphan-and-reconcile-delete cycle.
	if len(mock.replicatedChunks) != 2 {
		t.Fatalf("expected 2 chunks transferred, got %d (%v)",
			len(mock.replicatedChunks), mock.replicatedChunks)
	}
	transferred := make(map[chunk.ChunkID]bool, len(mock.replicatedChunks))
	for _, id := range mock.replicatedChunks {
		transferred[id] = true
	}
	if !transferred[ids[0]] {
		t.Errorf("chunk ids[0] %s should have been transferred (in manifest)", ids[0])
	}
	if transferred[ids[1]] {
		t.Errorf("chunk ids[1] %s should NOT have been transferred (FSM-retired)", ids[1])
	}
	if !transferred[ids[2]] {
		t.Errorf("chunk ids[2] %s should have been transferred (in manifest)", ids[2])
	}
}

// TestCatchupNilManifestUsesAllChunks verifies that when ListManifest is nil
// (e.g., an instance with no Raft group, or a memory instance without FSM tracking),
// catchupFollower falls back to the leader's on-disk list — the pre-fix
// behaviour. This is the backward-compatibility guarantee.
func TestCatchupNilManifestUsesAllChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vaultInst := newReplicationInstance(t, vaultID, nil, false, "")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal two chunks.
	for i := 0; i < 2; i++ {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := vaultInst.Chunks.Active()
		if err := orch.SealActiveChunk(vaultID, active.ID); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	// ListManifest is nil — catchup must fall back to disk list.
	vaultInst.ListManifest = nil

	if err := orch.catchupFollower(context.Background(), vaultID, "node-2"); err != nil {
		t.Fatalf("catchupFollower: %v", err)
	}

	if len(mock.replicatedChunks) != 2 {
		t.Errorf("expected 2 chunks transferred (nil manifest = all sealed), got %d",
			len(mock.replicatedChunks))
	}
}

// Symmetric peer-to-peer catchup. A follower with the chunk locally must
// be allowed to push it to a requester that lacks it, regardless of which
// side is the placement leader. Pre-fix this errored with "not placement
// leader for vault X (follower)", which is exactly what blocked the
// leader-from-follower backfill path needed to recover after leadership
// transferred to a node that didn't have historical chunks.
func TestCatchupSelectedChunksFromFollowerSucceeds(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-follower"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	// Build the instance as a FOLLOWER. Pre-fix, this would cause
	// CatchupSelectedChunks to reject the request outright.
	vaultInst := newReplicationInstance(t, vaultID, nil, true, "node-leader")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("rec-0")); err != nil {
		t.Fatalf("append: %v", err)
	}
	active := vaultInst.Chunks.Active()
	if err := orch.SealActiveChunk(vaultID, active.ID); err != nil {
		t.Fatalf("seal: %v", err)
	}

	scheduled, err := orch.CatchupSelectedChunks(
		context.Background(), vaultID, "node-leader-requester", []chunk.ChunkID{active.ID})
	if err != nil {
		t.Fatalf("expected nil error from follower-side catchup, got %v", err)
	}
	if scheduled != 1 {
		t.Errorf("scheduled = %d, want 1 (the single sealed chunk)", scheduled)
	}
}

// blockingCatchupReplicator blocks ImportSealedChunk until release is closed
// so tests can observe in-flight catchup deduplication.
type blockingCatchupReplicator struct {
	release chan struct{}
	imports atomic.Int32
}

func (b *blockingCatchupReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	b.imports.Add(1)
	<-b.release
	return nil
}
func (b *blockingCatchupReplicator) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

// CatchupSelectedChunks must not stack a second async push batch for the
// same (vault, requester) while the first is still importing.
func TestCatchupSelectedChunksSkipsDuplicateWhileInFlight(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-peer"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vaultInst := newReplicationInstance(t, vaultID, nil, false, "")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("rec-0")); err != nil {
		t.Fatalf("append: %v", err)
	}
	active := vaultInst.Chunks.Active()
	if err := orch.SealActiveChunk(vaultID, active.ID); err != nil {
		t.Fatalf("seal: %v", err)
	}

	block := make(chan struct{})
	mock := &blockingCatchupReplicator{release: block}
	orch.SetChunkReplicator(mock)

	requester := "node-requester"
	scheduled1, err := orch.CatchupSelectedChunks(
		context.Background(), vaultID, requester, []chunk.ChunkID{active.ID})
	if err != nil {
		t.Fatalf("first catchup: %v", err)
	}
	if scheduled1 != 1 {
		t.Fatalf("scheduled1 = %d, want 1", scheduled1)
	}

	deadline := time.After(2 * time.Second)
	for mock.imports.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for catchup import to start")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	scheduled2, err := orch.CatchupSelectedChunks(
		context.Background(), vaultID, requester, []chunk.ChunkID{active.ID})
	if err != nil {
		t.Fatalf("second catchup: %v", err)
	}
	if scheduled2 != 0 {
		t.Errorf("scheduled2 = %d, want 0 while first batch in flight", scheduled2)
	}

	close(block)
}

// ==========================================================================
// Multi-node file-backed replication tests
//
// These use setupCluster (from transition_test.go) with directTransferrer
// to test real in-process replication between file-backed orchestrators.
// ==========================================================================

// TestClusterReplicationSealedChunksArriveOnFollowers verifies that when the
// leader seals chunks (via burst ingestion with rotation policy), calling
// replicateSealedChunk delivers the chunks to all follower nodes. Verified
// via cursor reads AND filesystem directory checks on each follower.
func TestClusterReplicationSealedChunksArriveOnFollowers(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node convergence test")
	}
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	inst0 := leaderNode.instances[0]

	// Burst ingest 1K records → 10 sealed chunks via 100-record rotation.
	const totalRecords = 1_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "repl-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Seal remaining active chunk.
	if active := inst0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = inst0.Chunks.Seal()
	}

	// Get sealed chunks on leader.
	metas, _ := inst0.Chunks.List()
	if len(metas) < 5 {
		t.Fatalf("expected many sealed chunks, got %d", len(metas))
	}
	t.Logf("leader: %d sealed chunks to replicate", len(metas))

	// Run PostSealProcess on each chunk (compress + index) — required before
	// replication because replicateToFollower opens a cursor which needs the
	// chunk to be readable.
	processor, ok := inst0.Chunks.(chunk.ChunkPostSealProcessor)
	if ok {
		for _, m := range metas {
			if err := processor.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("PostSealProcess(%s): %v", m.ID, err)
			}
		}
	}

	// Replicate each sealed chunk to all followers.
	followerTargets := inst0.FollowerTargets
	ctx := context.Background()
	for _, m := range metas {
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, m.ID, followerTargets)
	}

	// ---- Verify: each follower has all records (cursor-verified) ----
	for _, fid := range []string{"f1", "f2", "f3"} {
		followerCM := h.nodes[fid].instances[0].Chunks
		count := cursorCountRecords(t, followerCM)
		if count != totalRecords {
			t.Errorf("follower %s: cursor read %d records, expected %d", fid, count, totalRecords)
		}
	}

	// ---- Verify: followers have chunk directories on disk ----
	for _, fid := range []string{"f1", "f2", "f3"} {
		dir := h.nodes[fid].instanceDirs[0]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		var chunkDirs int
		for _, e := range entries {
			if e.IsDir() && len(e.Name()) == 26 {
				chunkDirs++
			}
		}
		if chunkDirs == 0 {
			t.Errorf("follower %s: no chunk directories on disk after replication", fid)
		}
		t.Logf("follower %s: %d chunk directories on disk", fid, chunkDirs)
	}
}

// TestClusterReplicationSealedIdxWriteTSMatchesLeader verifies that after
// sealed-chunk replication, follower idx.log entries match the leader for
// WriteTS and IngestTS (offline read — same contract as instance ImportSealed).
func TestClusterReplicationSealedIdxWriteTSMatchesLeader(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderInst := leaderNode.instances[0]

	// Fewer records than rotation threshold → single active chunk, then one
	// sealed chunk after explicit Seal().
	const totalRecords = 50
	t0 := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "idxcmp-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if active := leaderInst.Chunks.Active(); active != nil && active.RecordCount > 0 {
		if err := leaderInst.Chunks.Seal(); err != nil {
			t.Fatal(err)
		}
	}

	metas, err := leaderInst.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed && m.RecordCount == totalRecords {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatalf("no sealed chunk with %d records", totalRecords)
	}

	processor, ok := leaderInst.Chunks.(chunk.ChunkPostSealProcessor)
	if !ok {
		t.Fatal("leader vaultInst chunks must implement ChunkPostSealProcessor")
	}
	if err := processor.PostSealProcess(context.Background(), sealedID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	ctx := context.Background()
	leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, sealedID, leaderInst.FollowerTargets)

	leaderEntries := chunkRecordTimestamps(t, leaderInst.Chunks, sealedID)
	if len(leaderEntries) != totalRecords {
		t.Fatalf("leader entries: want %d got %d", totalRecords, len(leaderEntries))
	}

	for _, fid := range []string{"f1", "f2"} {
		got := chunkRecordTimestamps(t, h.nodes[fid].instances[0].Chunks, sealedID)
		if len(got) != len(leaderEntries) {
			t.Fatalf("follower %s: entries %d, leader %d", fid, len(got), len(leaderEntries))
		}
		for i := range leaderEntries {
			if !got[i].WriteTS.Equal(leaderEntries[i].WriteTS) {
				t.Errorf("follower %s pos %d WriteTS: leader=%s follower=%s",
					fid, i, leaderEntries[i].WriteTS.UTC(), got[i].WriteTS.UTC())
			}
			if !got[i].IngestTS.Equal(leaderEntries[i].IngestTS) {
				t.Errorf("follower %s pos %d IngestTS: leader=%s follower=%s",
					fid, i, leaderEntries[i].IngestTS.UTC(), got[i].IngestTS.UTC())
			}
		}
	}
}

// chunkRecordTimestamps opens a cursor on the given chunk and collects each
// record's IngestTS / WriteTS pair. Routes through cm.OpenCursor so tests
// that assert per-record timestamp invariants aren't coupled to the sealed
// chunk's on-disk format.
func chunkRecordTimestamps(t *testing.T, cm chunk.ChunkManager, id chunk.ChunkID) []recordTimestamps {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("open cursor for %s: %v", id, err)
	}
	defer func() { _ = cursor.Close() }()
	var out []recordTimestamps
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return out
		}
		if err != nil {
			t.Fatalf("cursor next on %s: %v", id, err)
		}
		out = append(out, recordTimestamps{IngestTS: rec.IngestTS, WriteTS: rec.WriteTS})
	}
}

type recordTimestamps struct {
	IngestTS time.Time
	WriteTS  time.Time
}

// TestClusterReplicationDeletePropagation verifies that ChunkReplicator.DeleteChunk
// removes the chunk from the follower's chunk manager AND its filesystem
// directory.
func TestClusterReplicationDeletePropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node convergence test")
	}
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderInst := leaderNode.instances[0]

	// Ingest 500 records (5 sealed chunks).
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "del-prop-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderInst.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderInst.Chunks.Seal()
	}

	metas, _ := leaderInst.Chunks.List()
	t.Logf("leader: %d sealed chunks", len(metas))

	// Post-seal process and replicate to followers.
	processor, ok := leaderInst.Chunks.(chunk.ChunkPostSealProcessor)
	if ok {
		for _, m := range metas {
			_ = processor.PostSealProcess(context.Background(), m.ID)
		}
	}
	ctx := context.Background()
	for _, m := range metas {
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, m.ID, leaderInst.FollowerTargets)
	}

	// Verify followers have chunks.
	for _, fid := range []string{"f1", "f2", "f3"} {
		count := cursorCountRecords(t, h.nodes[fid].instances[0].Chunks)
		if count == 0 {
			t.Fatalf("follower %s has 0 records before delete test — replication failed", fid)
		}
	}

	// Delete all chunks cluster-wide via the receipt protocol. The leader's
	// reconciler proposes CmdRequestDelete with every node in expectedFrom;
	// setupCluster's fake FSM applier fulfills the obligation on each node.
	for _, m := range metas {
		if err := leaderInst.Reconciler.deleteChunk(m.ID, "test-delete", h.allNodeIDs()); err != nil {
			t.Errorf("deleteChunk(%s): %v", m.ID, err)
		}
	}

	// ---- Verify: all nodes have 0 cursor-readable records ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].instances[0].Chunks)
		if count != 0 {
			t.Errorf("%s: cursor read %d records after delete (should be 0)", nid, count)
		}
	}

	// ---- Verify: no chunk directories on disk on ANY node ----
	h.assertVaultDirEmpty(t, 0)
}
