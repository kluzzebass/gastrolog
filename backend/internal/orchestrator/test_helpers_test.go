package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
	"time"

	"errors"
	"fmt"

	"os"
	"path/filepath"
	"strings"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// syntheticPlacements creates a Placements slice with a leader using a synthetic storage ID.
func syntheticPlacements(nodeID string) []system.TierPlacement {
	return []system.TierPlacement{{StorageID: system.SyntheticStorageID(nodeID), Leader: true}}
}

// ---------- config loader adapter ----------

type transitionSystemLoader struct {
	store  *sysmem.Store
	nodeID string // defaults to "test-node" if empty
}

func (l *transitionSystemLoader) Load(ctx context.Context) (*system.System, error) {
	sys, err := l.store.Load(ctx)
	if err != nil || sys == nil {
		return sys, err
	}
	// Auto-populate placements for test tiers that don't have them.
	nodeID := l.nodeID
	if nodeID == "" {
		nodeID = "test-node"
	}
	if sys.Runtime.TierPlacements == nil {
		sys.Runtime.TierPlacements = make(map[glid.GLID][]system.TierPlacement)
	}
	for _, tier := range sys.Config.Tiers {
		if _, ok := sys.Runtime.TierPlacements[tier.ID]; !ok {
			sys.Runtime.TierPlacements[tier.ID] = []system.TierPlacement{
				{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
			}
		}
	}
	return sys, nil
}

// newTestStore creates a memory store and populates it with config entities
// AND runtime placements for single-node tests. Each tier gets a synthetic
// leader placement for nodeID.
func newTestStore(cfg *system.Config, nodeID string) *sysmem.Store {
	store := sysmem.NewStore()
	ctx := context.Background()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(ctx, v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(ctx, tc)
		_ = store.SetTierPlacements(ctx, tc.ID, []system.TierPlacement{
			{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
		})
	}
	for _, rt := range cfg.Routes {
		_ = store.PutRoute(ctx, rt)
	}
	return store
}

// ---------- helpers ----------

func makeRecord(raw string) chunk.Record {
	return chunk.Record{
		SourceTS: time.Now(),
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
	}
}

// newTestOrch creates an Orchestrator and registers t.Cleanup to stop the
// scheduler. Without this, leaked gocron goroutines cause massive race
// detector overhead (168 orchestrators × background cron jobs).
func newTestOrch(t *testing.T, cfg Config) *Orchestrator {
	t.Helper()
	orch, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)
	return orch
}

func newMemoryTierInstance(t *testing.T, tierID glid.GLID) *VaultInstance {
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
		TierID:  tierID,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}
}

// setupTestStoreRuntime populates the test store with runtime state that tests
// need — tier placements and node storage config. Most tests use memory tiers
// with a single test-node, so placements use synthetic storage IDs.
func setupTestStoreRuntime(store *sysmem.Store, nodeID string, tierIDs ...glid.GLID) {
	ctx := context.Background()
	for _, tid := range tierIDs {
		_ = store.SetTierPlacements(ctx, tid, []system.TierPlacement{
			{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
		})
	}
}


func newTestRetentionRunner(orch *Orchestrator, vaultID, tierID glid.GLID, cm chunk.ChunkManager, im index.IndexManager) *retentionRunner {
	return &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   tierID,
		cm:       cm,
		im:       im,
		orch:     orch,
		now:      time.Now,
		logger:   slog.Default(),
	}
}








// ---------- cross-node tests (mock transferrer) ----------

type transitionFakeTransferrer struct {
	calls       []transitionTransferCall
	streamCalls []transitionStreamCall
	failErr     error
}

type transitionTransferCall struct {
	nodeID  string
	vaultID glid.GLID
	tierID  glid.GLID
	records []chunk.Record
}

type transitionStreamCall struct {
	nodeID  string
	vaultID glid.GLID
	tierID  glid.GLID
	count   int
}

func (m *transitionFakeTransferrer) TransferRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.RecordIterator) error {
	return nil
}
func (m *transitionFakeTransferrer) ForwardAppend(_ context.Context, _ string, _ glid.GLID, _ []chunk.Record) error {
	return nil
}
func (m *transitionFakeTransferrer) WaitVaultReady(_ context.Context, _ string, _ glid.GLID) error {
	return nil
}





// TestTransitionCloudTierTTLSweep verifies that the retention sweep with a TTL
// policy correctly transitions cloud-backed sealed chunks to the next tier.
// Reproduces gastrolog-9umo2: 3m TTL on cloud tier, chunks sit for 10+ minutes.

// TestCloudTierLeaderPreservesCloudBacking verifies that a cloud tier leader
// built through the production code path (buildLeaderTierInstance →
// buildTierInstanceForStorage) retains the sealed_backing parameter so that
// PostSealProcess uploads chunks to cloud storage.
//
// Regression test: buildTierInstanceForStorage previously stripped sealed_backing
// unconditionally (with the comment "always follower"), even when called for the
// leader. This caused cloud tier leaders to have CloudStore=nil, silently
// preventing all cloud uploads and breaking the entire archival lifecycle.

// TestTransitionCloudTierFollowerDoesNotOverwriteBlob verifies that the
// follower's PostSealProcess does NOT upload to cloud storage, preventing
// it from overwriting the leader's blob with a different-sized version.
// This was the root cause of gastrolog-9umo2: the follower's upload changed
// the blob size, corrupting the leader's stored diskBytes and breaking all
// future cloud cursor reads (S3 416 Range Not Satisfiable).

func testIterFromRecords(recs []chunk.Record) chunk.RecordIterator {
	i := 0
	return func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
	}
}

// keepNPolicy is a test-only retention policy that matches all sealed chunks
// beyond the first N.
type keepNPolicy struct{ n int }

func (p *keepNPolicy) Apply(state chunk.VaultState) []chunk.ChunkID {
	if len(state.Chunks) <= p.n {
		return nil
	}
	var ids []chunk.ChunkID
	for _, c := range state.Chunks[:len(state.Chunks)-p.n] {
		ids = append(ids, c.ID)
	}
	return ids
}

func (m *transitionFakeTransferrer) StreamToTier(_ context.Context, nodeID string, vaultID, tierID glid.GLID, next chunk.RecordIterator) error {
	if m.failErr != nil {
		return m.failErr
	}
	var count int
	for {
		if _, err := next(); err != nil {
			break
		}
		count++
	}
	m.streamCalls = append(m.streamCalls, transitionStreamCall{
		nodeID: nodeID, vaultID: vaultID, tierID: tierID, count: count,
	})
	return nil
}

// ---------- cloud tier transition test ----------

// newCloudFileTier creates a file-backed VaultInstance with cloud storage.
// Sealed chunks are uploaded to the in-memory blobstore and local files deleted,
// matching production cloud tier behavior.
func newCloudFileTier(t *testing.T, tierID glid.GLID, vaultID glid.GLID, store blobstore.Store) (*VaultInstance, string) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)
	return &VaultInstance{
		TierID:  tierID,
		Type:    "cloud",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}, dir
}

// TestTransitionCloudTierToNextTier verifies that sealed cloud-backed chunks
// are read back from object storage and streamed to the next tier. This is
// the exact scenario from gastrolog-9umo2: FILE → FILE → CLOUD → FILE chain
// where the cloud tier's sealed chunks never transition to tier 4.

// TestTransitionCloudTierSweepDispatch verifies that the retention sweep
// correctly picks up cloud-backed sealed chunks and transitions them.
// This tests the full sweep() path rather than calling transitionChunk directly.

// ---------- helpers for new tests ----------

// newFileTierInstance creates a file-backed VaultInstance without cloud storage.
// Returns the tier instance and its filesystem directory for post-test verification.
func newFileTierInstance(t *testing.T, tierID glid.GLID) (*VaultInstance, string) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)
	return &VaultInstance{
		TierID:  tierID,
		Type:    "file",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}, dir
}

// assertNoDirsOnDisk verifies no chunk subdirectories remain in a tier directory.
func assertNoDirsOnDisk(t *testing.T, label, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Errorf("%s: ReadDir(%s): %v", label, dir, err)
		return
	}
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			t.Errorf("%s: chunk directory %s still on disk at %s", label, e.Name(), dir)
		}
	}
}

// countAllTierRecords counts all records across both sealed and active chunks.
func countAllTierRecords(tb testing.TB, cm chunk.ChunkManager) int64 {
	tb.Helper()
	metas, _ := cm.List()
	var total int64
	for _, m := range metas {
		total += m.RecordCount
	}
	active := cm.Active()
	if active != nil {
		listed := false
		for _, m := range metas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			total += active.RecordCount
		}
	}
	return total
}

// readAllRecords reads every record from a chunk manager (all sealed + active).
func readAllRecords(t *testing.T, cm chunk.ChunkManager) []chunk.Record {
	t.Helper()
	var all []chunk.Record
	metas, _ := cm.List()

	// Collect chunk IDs to read (sealed chunks).
	ids := make([]chunk.ChunkID, 0, len(metas))
	for _, m := range metas {
		ids = append(ids, m.ID)
	}
	// Include active chunk if not already in the list.
	if active := cm.Active(); active != nil {
		found := false
		for _, m := range metas {
			if m.ID == active.ID {
				found = true
				break
			}
		}
		if !found {
			ids = append(ids, active.ID)
		}
	}

	for _, id := range ids {
		cursor, err := cm.OpenCursor(id)
		if err != nil {
			t.Fatalf("OpenCursor(%s): %v", id, err)
		}
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				_ = cursor.Close()
				t.Fatalf("cursor.Next: %v", err)
			}
			all = append(all, rec.Copy())
		}
		_ = cursor.Close()
	}
	return all
}

// makeRecordWithEventID creates a record with an explicit EventID for testing preservation.
func makeRecordWithEventID(raw string, ingesterID glid.GLID, seq uint32) chunk.Record {
	now := time.Now()
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			IngestTS:   now,
			IngestSeq:  seq,
		},
		Attrs: chunk.Attributes{"msg": raw},
		Raw:   []byte(raw),
	}
}

// ---------- 3-tier chain transition tests ----------

// TestTransitionThreeTierChainMemory verifies that a 3-tier chain
// (memory→memory→memory) preserves exact record count with no duplication.

// TestTransitionThreeTierChainFileFileCloud verifies the production-like
// file→file→cloud chain preserves all records without N× duplication.
// This is the exact scenario from the gastrolog-1rv42 session bugs.

// ---------- EventID preservation tests ----------

// TestTransitionEventIDPreserved verifies that EventIDs survive local tier transitions.

// TestTransitionEventIDPreservedThroughCloudTier verifies EventIDs survive
// transitions through a cloud-backed tier (the full round-trip: ingest → seal
// → cloud upload → cloud cursor read → transition to next tier).

// ---------- Record count accuracy tests ----------

// TestTransitionRecordCountAccuracy verifies that chunk metadata RecordCount
// matches the actual number of records readable via cursor at each tier stage.

// ---------- Cloud search after transition ----------

// TestTransitionCloudSearchAfterTransition verifies that records in a cloud
// tier are searchable via the query engine after transition and upload.

// ---------- Cloud upload idempotency ----------

// TestTransitionCloudUploadOnlyOneBlob verifies that uploading a sealed chunk
// to cloud produces exactly one blob in the blobstore, and that the blob
// contains all records. This guards against duplicate uploads from racing nodes.

// ==========================================================================
// Multi-node cluster transition tests
//
// These wire up multiple full orchestrators with in-process RemoteTransferrers,
// multi-tier vaults with leader/follower replication, rotation policies that
// create many sealed chunks, and burst ingestion to stress the transition +
// replication pipeline under realistic conditions.
// ==========================================================================

// directTransferrer implements RemoteTransferrer by calling directly into
// the target orchestrator. This is the in-process equivalent of the gRPC
// transferrer used in production — same operations, no network.
type directTransferrer struct {
	nodes map[string]*Orchestrator
}

func (d *directTransferrer) StreamToTier(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	return orch.StreamAppendToTier(ctx, vaultID, tierID, next)
}

func (d *directTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	for _, rec := range records {
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *directTransferrer) TransferRecords(ctx context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
}

func (d *directTransferrer) WaitVaultReady(_ context.Context, _ string, _ glid.GLID) error {
	return nil
}

// directChunkReplicator implements ChunkReplicator by calling directly into the
// target orchestrator. In-process equivalent of the gRPC ChunkReplicator.
type directChunkReplicator struct {
	nodes map[string]*Orchestrator
}

func (d *directChunkReplicator) AppendRecords(_ context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	for _, rec := range records {
		if err := orch.AppendToVault(vaultID, tierID, chunkID, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *directChunkReplicator) SealVault(_ context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	return orch.SealActiveTier(vaultID, tierID, chunkID)
}

func (d *directChunkReplicator) ImportSealedChunk(ctx context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	i := 0
	iter := func() (chunk.Record, error) {
		if i >= len(records) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		rec := records[i]
		i++
		return rec, nil
	}
	return orch.ImportToVault(ctx, vaultID, tierID, chunkID, iter)
}

func (d *directChunkReplicator) DeleteChunk(_ context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	return orch.DeleteChunkFromTier(vaultID, tierID, chunkID)
}

func (d *directChunkReplicator) RequestReplicaCatchup(ctx context.Context, leaderNodeID string, vaultID, tierID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
	orch, ok := d.nodes[leaderNodeID]
	if !ok {
		return 0, fmt.Errorf("directChunkReplicator: unknown leader %q", leaderNodeID)
	}
	return orch.CatchupSelectedChunks(ctx, vaultID, tierID, requesterNodeID, chunkIDs)
}

// newClusterRetentionRunner creates a retention runner with follower targets
// for proper cross-node delete forwarding.
//
// Wires the reconciler so retention-ttl flows through the receipt protocol
// (gastrolog-51gme step 4): CmdRequestDelete → onRequestDelete on every node
// → CmdAckDelete from each → CmdFinalizeDelete on the leader. Without this,
// expireChunk falls through to the legacy direct-delete fallback which
// doesn't replicate, and the cluster retention assertions fail.
func newClusterRetentionRunner(orch *Orchestrator, vaultID, tierID glid.GLID, tier *VaultInstance) *retentionRunner {
	return &retentionRunner{
		isLeader:        true,
		vaultID:         vaultID,
		tierID:          tierID,
		cm:              tier.Chunks,
		im:              tier.Indexes,
		orch:            orch,
		followerTargets: tier.FollowerTargets,
		reconciler:      tier.Reconciler,
		now:             time.Now,
		logger:          slog.Default(),
	}
}

// clusterTestNode is one node in a multi-node cluster test.
type clusterTestNode struct {
	nodeID   string
	orch     *Orchestrator
	tiers    []*VaultInstance // all tier instances on this node
	tierDirs []string        // filesystem directories, one per tier
}

// clusterHarness holds the full multi-node cluster.
type clusterHarness struct {
	nodes    map[string]*clusterTestNode
	cfgStore *sysmem.Store
	vaultID  glid.GLID
	tierIDs  []glid.GLID
}

// allNodeIDs returns sorted node IDs.
func (h *clusterHarness) allNodeIDs() []string {
	ids := make([]string, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	return ids
}

// cursorCountRecords opens cursors on every chunk (sealed + active) and counts
// records by actually reading them. Does NOT trust ChunkMeta.RecordCount.
func cursorCountRecords(t *testing.T, cm chunk.ChunkManager) int64 {
	t.Helper()
	return int64(len(readAllRecords(t, cm)))
}

// countRecordsOnNode counts all cursor-verified records across all tiers on a node.
func (h *clusterHarness) countRecordsOnNode(t *testing.T, nodeID string) int64 {
	t.Helper()
	node := h.nodes[nodeID]
	var total int64
	for _, tier := range node.tiers {
		total += cursorCountRecords(t, tier.Chunks)
	}
	return total
}

// countRecordsOnTier counts cursor-verified records in a specific tier across ALL nodes.
func (h *clusterHarness) countRecordsOnTier(t *testing.T, tierIdx int) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64)
	for nodeID, node := range h.nodes {
		if tierIdx < len(node.tiers) {
			counts[nodeID] = cursorCountRecords(t, node.tiers[tierIdx].Chunks)
		}
	}
	return counts
}

// countChunksOnTier counts sealed chunks in a specific tier across ALL nodes.
func (h *clusterHarness) countChunksOnTier(t *testing.T, tierIdx int) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for nodeID, node := range h.nodes {
		if tierIdx < len(node.tiers) {
			metas, _ := node.tiers[tierIdx].Chunks.List()
			counts[nodeID] = len(metas)
		}
	}
	return counts
}

// setupCluster creates a multi-node cluster with a shared vault using
// file-backed chunk managers with real filesystem directories.
//
// Layout:
//   - nodeIDs[0] is the leader for all tiers
//   - nodeIDs[1:] are followers for all tiers
//   - Each tier gets its own TempDir per node (real filesystem I/O)
//   - rotationRecords controls the rotation policy (e.g., 100 = seal every 100 records)
//   - The leader's tiers have FollowerTargets pointing to all followers
//   - Every node has a directTransferrer wired to all other nodes
//
// newClusterLifecycleLogger returns a slog.Logger that writes ALL levels
// (including Debug) to /tmp/gastrolog-lifecycle-<testname>-<pid>-<ts>.log.
// Path is outside t.TempDir() so the log survives test cleanup for
// post-mortem inspection. On test failure, the log path is dumped to t.Log.
func newClusterLifecycleLogger(t *testing.T) *slog.Logger {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	logPath := filepath.Join("/tmp", fmt.Sprintf("gastrolog-lifecycle-%s-%d-%d.log",
		name, os.Getpid(), time.Now().UnixNano()))
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create lifecycle log: %v", err)
	}
	t.Cleanup(func() {
		_ = f.Close()
		t.Logf("lifecycle log: %s", logPath)
	})
	handler := slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(handler)
}

func setupCluster(t *testing.T, nodeIDs []string, tierCount int, rotationRecords uint64) *clusterHarness {
	t.Helper()
	if len(nodeIDs) < 2 {
		t.Fatal("setupCluster needs at least 2 nodes")
	}
	leaderID := nodeIDs[0]
	vaultID := glid.New()
	tierIDs := make([]glid.GLID, tierCount)
	for i := range tierCount {
		tierIDs[i] = glid.New()
	}

	// Create config store.
	store := sysmem.NewStore()
	tierCfgs := make([]system.TierConfig, tierCount)
	for i := range tierCount {
		placements := make([]system.TierPlacement, 0, len(nodeIDs))
		placements = append(placements, system.TierPlacement{
			StorageID: system.SyntheticStorageID(leaderID), Leader: true,
		})
		for _, fid := range nodeIDs[1:] {
			placements = append(placements, system.TierPlacement{
				StorageID: system.SyntheticStorageID(fid), Leader: false,
			})
		}
		tierCfgs[i] = system.TierConfig{
			ID:       tierIDs[i],
			Name:     fmt.Sprintf("tier-%d", i),
			Type:     system.VaultTypeFile,
			VaultID:  vaultID,
			Position: uint32(i),
		}
		_ = store.PutTier(context.Background(), tierCfgs[i])
		_ = store.SetTierPlacements(context.Background(), tierIDs[i], placements)
	}
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "cluster-vault",
	})

	// Build follower targets for the leader.
	followerTargets := make([]system.ReplicationTarget, 0, len(nodeIDs)-1)
	for _, fid := range nodeIDs[1:] {
		followerTargets = append(followerTargets, system.ReplicationTarget{NodeID: fid})
	}

	// Create all orchestrators with file-backed tiers.
	orchs := make(map[string]*Orchestrator)
	nodes := make(map[string]*clusterTestNode)

	logger := newClusterLifecycleLogger(t)

	for _, nid := range nodeIDs {
		nodeLogger := logger.With("node", nid)
		orch := newTestOrch(t, Config{LocalNodeID: nid, Logger: nodeLogger})
		orch.sysLoader = &transitionSystemLoader{store: store}
		orchs[nid] = orch

		isLeader := nid == leaderID
		tiers := make([]*VaultInstance, tierCount)
		tierDirs := make([]string, tierCount)
		for i := range tierCount {
			dir := t.TempDir()
			tierDirs[i] = dir
			cm, cmErr := chunkfile.NewManager(chunkfile.Config{
				Dir:            dir,
				Now:            time.Now,
				RotationPolicy: chunk.NewRecordCountPolicy(rotationRecords),
				Logger:         nodeLogger.With("tier", fmt.Sprintf("tier-%d", i)),
			})
			if cmErr != nil {
				t.Fatal(cmErr)
			}
			im := indexfile.NewManager(dir, nil, nil)
			tier := &VaultInstance{
				TierID:  tierIDs[i],
				Type:    "file",
				Chunks:  cm,
				Indexes: im,
				Query:   query.New(cm, im, nil),
			}
			if isLeader {
				tier.FollowerTargets = followerTargets
			} else {
				tier.IsFollower = true
			}
			tiers[i] = tier
		}

		// Phase 2 (gastrolog-3iy5l): vaults are single-instance. Use the
		// first tier as the vault's instance; tierCount > 1 is unused
		// post-collapse (callers that asked for it were transition tests
		// that have been removed).
		vault := NewVault(vaultID, tiers[0])
		vault.Name = "cluster-vault"
		orch.RegisterVault(vault)

		nodes[nid] = &clusterTestNode{
			nodeID:   nid,
			orch:     orch,
			tiers:    tiers,
			tierDirs: tierDirs,
		}
	}

	// Wire directTransferrer and directChunkReplicator: each node can reach all other nodes.
	for _, nid := range nodeIDs {
		remotes := make(map[string]*Orchestrator)
		for _, other := range nodeIDs {
			if other != nid {
				remotes[other] = orchs[other]
			}
		}
		orchs[nid].SetRemoteTransferrer(&directTransferrer{nodes: remotes})
		orchs[nid].SetChunkReplicator(&directChunkReplicator{nodes: remotes})
	}

	t.Cleanup(func() {
		// Close file managers BEFORE t.TempDir cleanup removes their directories.
		// Stop orchestrators first (stops schedulers), then close chunk managers.
		for _, n := range nodes {
			n.orch.Stop()
		}
		for _, n := range nodes {
			for _, tier := range n.tiers {
				_ = tier.Chunks.Close()
			}
		}
	})

	return &clusterHarness{
		nodes:    nodes,
		cfgStore: store,
		vaultID:  vaultID,
		tierIDs:  tierIDs,
	}
}

// sealAndReplicate seals the active chunk on the leader AND propagates the
// seal to followers, then drains the leader scheduler so the post-seal
// pipeline (compression → scheduleReplication → ImportSealedChunk on
// followers) completes BEFORE any caller-side delete fires. Without this
// drain, a late ImportSealedChunk would recreate the chunk on the follower
// after retention deleted it. Plain Chunks.Seal() only seals the leader —
// followers' active chunks would stay active, causing forwardDelete to
// fail with ErrActiveChunk. The leader's production seal-on-rotation path
// uses sealRemoteFollowers; tests that manually seal must do the same.
func (h *clusterHarness) sealAndReplicate(t *testing.T, leaderNode *clusterTestNode, tierIdx int) {
	t.Helper()
	tier := leaderNode.tiers[tierIdx]
	active := tier.Chunks.Active()
	if active == nil || active.RecordCount == 0 {
		return
	}
	chunkID := active.ID
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatalf("seal tier %d: %v", tierIdx, err)
	}
	// Propagate seal to all follower nodes.
	for _, nid := range h.allNodeIDs() {
		if nid == leaderNode.nodeID {
			continue
		}
		ftier := h.nodes[nid].tiers[tierIdx]
		if active := ftier.Chunks.Active(); active != nil && active.ID == chunkID {
			if err := ftier.Chunks.Seal(); err != nil {
				t.Fatalf("seal follower %s tier %d: %v", nid, tierIdx, err)
			}
		}
	}
	// Drain post-seal + replication jobs for the newly-sealed chunk.
	// A late ImportSealedChunk would recreate the chunk on a follower
	// after the transition delete has fired.
	leaderNode.orch.Scheduler().WaitIdle(30 * time.Second)
}

// assertTierDirEmpty verifies that a tier's filesystem directory contains no
// chunk subdirectories on ANY node. This goes below the chunk manager API —
// it checks the actual filesystem to catch silent delete failures, leaked
// directories, and stale files.
func (h *clusterHarness) assertTierDirEmpty(t *testing.T, tierIdx int) {
	t.Helper()
	// Poll briefly — async chunk deletion may lag under CPU contention.
	deadline := time.Now().Add(60 * time.Second)
	for {
		allEmpty := true
		for _, nid := range h.allNodeIDs() {
			if len(h.chunkDirsOnNode(nid, tierIdx)) > 0 {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, nid := range h.allNodeIDs() {
		chunkDirs := h.chunkDirsOnNode(nid, tierIdx)
		if len(chunkDirs) > 0 {
			t.Errorf("tier %d on %s: %d chunk directories still on disk: %v",
				tierIdx, nid, len(chunkDirs), chunkDirs)
		}
	}
}

// assertTierEmptyAllNodes polls until all nodes report zero records on the
// given tier, or fails after 10s. Follower chunk deletion is async.
func (h *clusterHarness) assertTierEmptyAllNodes(t *testing.T, tierIdx int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		allEmpty := true
		for _, nid := range h.allNodeIDs() {
			if cursorCountRecords(t, h.nodes[nid].tiers[tierIdx].Chunks) > 0 {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].tiers[tierIdx].Chunks)
		if count != 0 {
			t.Errorf("tier %d on %s: cursor read %d records after full chain (should be 0)", tierIdx, nid, count)
		}
	}
}

func (h *clusterHarness) chunkDirsOnNode(nid string, tierIdx int) []string {
	dir := h.nodes[nid].tierDirs[tierIdx]
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs
}

// listChunkDirsOnNode returns the chunk directory names in a tier dir on a node.
func (h *clusterHarness) listChunkDirsOnNode(t *testing.T, nodeID string, tierIdx int) []string {
	t.Helper()
	dir := h.nodes[nodeID].tierDirs[tierIdx]
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs
}

// TestClusterTransitionBurstNoOrphans creates a 4-node cluster with 2 tiers,
// bursts 10K records with a 100-record rotation policy (100 sealed chunks),
// transitions all chunks from tier 0 → tier 1, and verifies:
//   - All records arrive in tier 1 on the LEADER
//   - Source tier 0 is empty on ALL nodes
//   - No records are lost or duplicated
//   - Record count matches on the leader

// TestClusterTransitionThreeTierChainBurst creates a 4-node cluster with
// 3 tiers and bursts 10K records through the full tier chain with 100-record
// rotation. Verifies no orphans on any node and exact record preservation.

// TestClusterTransitionEventIDPreservedAcrossNodes verifies that EventIDs
// survive transitions in a multi-node cluster with replication.

// TestClusterTransitionLargeBurst ingests a large burst (10K records) through
// the serialized AppendToVault path and verifies no data loss after transition.
// The burst creates ~100 sealed chunks via the 100-record rotation policy.
//
// NOTE: concurrent Append through the file chunk manager's attr.log writer
// is not safe (see gastrolog-3l7ow findings). Production serializes through
// the digest loop. This test uses sequential ingestion to match that model.

// TestClusterTransitionNoChunksLeftBehindOnFollowers verifies that after
// transition, the source tier's sealed chunks are cleaned up on follower nodes
// (via deleteFromFollowers), not just on the leader.

// ==========================================================================
// Multi-node drain tests
// ==========================================================================

// waitForDrainJob polls the scheduler until the drain job completes or times out.
// Uses ListJobs which returns snapshots — no race with the scheduler goroutine.
func waitForDrainJob(t *testing.T, orch *Orchestrator, vaultID glid.GLID, timeout time.Duration) {
	t.Helper()
	jobName := "drain:" + vaultID.String()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// ListJobs returns snapshot copies under the scheduler's lock.
		for _, j := range orch.Scheduler().ListJobs() {
			if j.Name != jobName {
				continue
			}
			snap := j.Snapshot()
			if snap.Progress != nil && snap.Progress.Status == JobStatusCompleted {
				return
			}
			if snap.Progress != nil && snap.Progress.Status == JobStatusFailed {
				t.Fatalf("drain job failed: %s", snap.Progress.Error)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("drain job did not complete within %s", timeout)
}

// TestClusterDrainVaultRecordsArriveOnDestination drains a file-backed vault
// from node-A to node-B via directTransferrer. Verifies:
//   - All records arrive on node-B (cursor-verified)
//   - Source vault unregistered on node-A
//   - Source chunk directories removed from disk

// --- Memory budget enforcement ---




// TestExplicitStorageLeaderGetsRotationPolicy verifies that a tier built via
// buildTierInstanceForStorage (explicit placement path) applies the rotation
// policy from system. Regression test for a gap where applyRotationPolicy was
// only called in buildTierInstance but not buildTierInstanceForStorage.

// waitForTransitions polls until all transition:* jobs in the scheduler
// have completed. Transitions run as one-shot scheduler jobs since
// gastrolog-4913n, so tests that call sweep() need to wait.
func waitForTransitions(t *testing.T, orch *Orchestrator, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !orch.scheduler.HasPendingPrefix("transition:") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("transition jobs did not complete within timeout")
}
