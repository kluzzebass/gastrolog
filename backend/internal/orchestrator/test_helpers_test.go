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
func syntheticPlacements(nodeID string) []system.VaultPlacement {
	return []system.VaultPlacement{{StorageID: system.SyntheticStorageID(nodeID), Leader: true}}
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
	// Auto-populate placements for test vaults that don't have them.
	nodeID := l.nodeID
	if nodeID == "" {
		nodeID = "test-node"
	}
	if sys.Runtime.VaultPlacements == nil {
		sys.Runtime.VaultPlacements = make(map[glid.GLID][]system.VaultPlacement)
	}
	for _, v := range sys.Config.Vaults {
		if _, ok := sys.Runtime.VaultPlacements[v.ID]; !ok {
			sys.Runtime.VaultPlacements[v.ID] = []system.VaultPlacement{
				{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
			}
		}
	}
	return sys, nil
}

// newTestStore creates a memory store and populates it with config entities
// AND runtime placements for single-node tests. Each instance gets a synthetic
// leader placement for nodeID.
func newTestStore(cfg *system.Config, nodeID string) *sysmem.Store {
	store := sysmem.NewStore()
	ctx := context.Background()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(ctx, v)
	}
	for _, v := range cfg.Vaults {
		_ = store.SetVaultPlacements(ctx, v.ID, []system.VaultPlacement{
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
	if cfg.SegmentsDir == "" {
		cfg.SegmentsDir = filepath.Join(t.TempDir(), "segments")
	}
	orch, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)
	return orch
}

func newMemoryInstance(t *testing.T, vaultID glid.GLID) *VaultInstance {
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
		VaultID: vaultID,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}
}

// setupTestStoreRuntime populates the test store with runtime state that tests
// need — vault placements and node storage config. Most tests use memory vaults
// with a single test-node, so placements use synthetic storage IDs.
func setupTestStoreRuntime(store *sysmem.Store, nodeID string, vaultIDs ...glid.GLID) {
	ctx := context.Background()
	for _, tid := range vaultIDs {
		_ = store.SetVaultPlacements(ctx, tid, []system.VaultPlacement{
			{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
		})
	}
}

func newTestRetentionRunner(orch *Orchestrator, vaultID glid.GLID, cm chunk.ChunkManager, im index.IndexManager) *retentionRunner {
	return &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
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
	records []chunk.Record
}

type transitionStreamCall struct {
	nodeID  string
	vaultID glid.GLID
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

// ---------- cloud instance transition test ----------

// newCloudFileInstance creates a file-backed VaultInstance with cloud storage.
// Sealed chunks are uploaded to the in-memory blobstore and local files deleted,
// matching production cloud instance behavior.
func newCloudFileInstance(t *testing.T, vaultID glid.GLID, store blobstore.Store) (*VaultInstance, string) {
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
	im := indexfile.NewManager(dir, nil, nil, cm)
	return &VaultInstance{
		VaultID: vaultID,
		Type:    "cloud",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}, dir
}

// ---------- helpers for new tests ----------

// newFileInstance creates a file-backed VaultInstance without cloud storage.
// Returns the vault instance and its filesystem directory for post-test verification.
func newFileInstance(t *testing.T, vaultID glid.GLID) (*VaultInstance, string) {
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
	im := indexfile.NewManager(dir, nil, nil, cm)
	return &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}, dir
}

// assertNoDirsOnDisk verifies no chunk subdirectories remain in a vault directory.
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

// countAllInstanceRecords counts all records across both sealed and active chunks.
func countAllInstanceRecords(tb testing.TB, cm chunk.ChunkManager) int64 {
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

// ==========================================================================
// Multi-node cluster transition tests
//
// These wire up multiple full orchestrators with in-process RemoteTransferrers,
// multi-instance vaults with leader/follower replication, rotation policies that
// create many sealed chunks, and burst ingestion to stress the transition +
// replication pipeline under realistic conditions.
// ==========================================================================

// directTransferrer implements RemoteTransferrer by calling directly into
// the target orchestrator. This is the in-process equivalent of the gRPC
// transferrer used in production — same operations, no network.
type directTransferrer struct {
	nodes map[string]*Orchestrator
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
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, rec); err != nil {
			return err
		}
	}
}

func (d *directTransferrer) WaitVaultReady(ctx context.Context, nodeID string, vaultID glid.GLID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	// In-process equivalent of the gRPC ForwardWaitVaultReady handler: block
	// on the target orchestrator's real event-driven readiness signal so the
	// multi-node drain tests exercise the consumer path. See gastrolog-3sdnn.
	return orch.WaitVaultReady(ctx, vaultID)
}

// directChunkReplicator implements ChunkReplicator by calling directly into the
// target orchestrator. In-process equivalent of the gRPC ChunkReplicator.
type directChunkReplicator struct {
	nodes map[string]*Orchestrator
}

func (d *directChunkReplicator) AppendRecords(_ context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	for _, rec := range records {
		if err := orch.AppendToVault(vaultID, chunkID, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *directChunkReplicator) SealVault(_ context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	return orch.SealActiveChunk(vaultID, chunkID)
}

func (d *directChunkReplicator) ImportSealedChunk(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	return orch.ImportToVault(ctx, vaultID, chunkID, next)
}

func (d *directChunkReplicator) DeleteChunk(_ context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directChunkReplicator: unknown node %q", nodeID)
	}
	return orch.DeleteChunk(vaultID, chunkID)
}

func (d *directChunkReplicator) RequestReplicaCatchup(ctx context.Context, leaderNodeID string, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
	orch, ok := d.nodes[leaderNodeID]
	if !ok {
		return 0, fmt.Errorf("directChunkReplicator: unknown leader %q", leaderNodeID)
	}
	return orch.CatchupSelectedChunks(ctx, vaultID, requesterNodeID, chunkIDs)
}

// newClusterRetentionRunner creates a retention runner with follower targets
// for proper cross-node delete forwarding.
//
// Wires the reconciler so retention-ttl flows through the receipt protocol
// (gastrolog-51gme step 4): CmdRequestDelete → onRequestDelete on every node
// → CmdAckDelete from each → CmdFinalizeDelete on the leader. Without this,
// expireChunk falls through to the legacy direct-delete fallback which
// doesn't replicate, and the cluster retention assertions fail.
func newClusterRetentionRunner(orch *Orchestrator, vaultID glid.GLID, vaultInst *VaultInstance) *retentionRunner {
	return &retentionRunner{
		isLeader:        true,
		vaultID:         vaultID,
		cm:              vaultInst.Chunks,
		im:              vaultInst.Indexes,
		orch:            orch,
		followerTargets: vaultInst.FollowerTargets,
		reconciler:      vaultInst.Reconciler,
		now:             time.Now,
		logger:          slog.Default(),
	}
}

// clusterTestNode is one node in a multi-node cluster test.
type clusterTestNode struct {
	nodeID       string
	orch         *Orchestrator
	instances    []*VaultInstance // all vault instances on this node
	instanceDirs []string         // filesystem directories, one per instance
}

// clusterHarness holds the full multi-node cluster.
type clusterHarness struct {
	nodes    map[string]*clusterTestNode
	cfgStore *sysmem.Store
	vaultID  glid.GLID
	vaultIDs []glid.GLID
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

// countRecordsOnNode counts all cursor-verified records across all vaults on a node.
func (h *clusterHarness) countRecordsOnNode(t *testing.T, nodeID string) int64 {
	t.Helper()
	node := h.nodes[nodeID]
	var total int64
	for _, vaultInst := range node.instances {
		total += cursorCountRecords(t, vaultInst.Chunks)
	}
	return total
}

// countRecordsOnInstance counts cursor-verified records in a specific instance across ALL nodes.
func (h *clusterHarness) countRecordsOnInstance(t *testing.T, vaultIdx int) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64)
	for nodeID, node := range h.nodes {
		if vaultIdx < len(node.instances) {
			counts[nodeID] = cursorCountRecords(t, node.instances[vaultIdx].Chunks)
		}
	}
	return counts
}

// countChunksOnInstance counts sealed chunks in a specific instance across ALL nodes.
func (h *clusterHarness) countChunksOnInstance(t *testing.T, vaultIdx int) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for nodeID, node := range h.nodes {
		if vaultIdx < len(node.instances) {
			metas, _ := node.instances[vaultIdx].Chunks.List()
			counts[nodeID] = len(metas)
		}
	}
	return counts
}

// setupCluster creates a multi-node cluster with a shared vault using
// file-backed chunk managers with real filesystem directories.
//
// Layout:
//   - nodeIDs[0] is the leader for all vaults
//   - nodeIDs[1:] are followers for all vaults
//   - Each instance gets its own TempDir per node (real filesystem I/O)
//   - rotationRecords controls the rotation policy (e.g., 100 = seal every 100 records)
//   - The leader's vaults have FollowerTargets pointing to all followers
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

func setupCluster(t *testing.T, nodeIDs []string, vaultCount int, rotationRecords uint64) *clusterHarness {
	t.Helper()
	if len(nodeIDs) < 2 {
		t.Fatal("setupCluster needs at least 2 nodes")
	}
	leaderID := nodeIDs[0]
	vaultID := glid.New()
	// Single instance per vault sharing the vault's ID. vaultCount is
	// always 1 in current callers; the slice survives only for legacy
	// fixture wiring.
	vaultIDs := make([]glid.GLID, vaultCount)
	for i := range vaultCount {
		vaultIDs[i] = vaultID
	}

	// Create config store.
	store := sysmem.NewStore()
	for i := range vaultCount {
		placements := make([]system.VaultPlacement, 0, len(nodeIDs))
		placements = append(placements, system.VaultPlacement{
			StorageID: system.SyntheticStorageID(leaderID), Leader: true,
		})
		for _, fid := range nodeIDs[1:] {
			placements = append(placements, system.VaultPlacement{
				StorageID: system.SyntheticStorageID(fid), Leader: false,
			})
		}
		_ = store.SetVaultPlacements(context.Background(), vaultIDs[i], placements)
	}
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID:   vaultID,
		Name: "cluster-vault",
		Type: system.VaultTypeFile,
	})

	// Build follower targets for the leader.
	followerTargets := make([]system.ReplicationTarget, 0, len(nodeIDs)-1)
	for _, fid := range nodeIDs[1:] {
		followerTargets = append(followerTargets, system.ReplicationTarget{NodeID: fid})
	}

	// Create all orchestrators with file-backed vaults.
	orchs := make(map[string]*Orchestrator)
	nodes := make(map[string]*clusterTestNode)

	logger := newClusterLifecycleLogger(t)

	for _, nid := range nodeIDs {
		nodeLogger := logger.With("node", nid)
		orch := newTestOrch(t, Config{LocalNodeID: nid, Logger: nodeLogger})
		orch.sysLoader = &transitionSystemLoader{store: store}
		orchs[nid] = orch

		isLeader := nid == leaderID
		instances := make([]*VaultInstance, vaultCount)
		instanceDirs := make([]string, vaultCount)
		for i := range vaultCount {
			dir := t.TempDir()
			instanceDirs[i] = dir
			cm, cmErr := chunkfile.NewManager(chunkfile.Config{
				Dir:            dir,
				Now:            time.Now,
				RotationPolicy: chunk.NewRecordCountPolicy(rotationRecords),
				Logger:         nodeLogger.With("vault", fmt.Sprintf("vault-%d", i)),
			})
			if cmErr != nil {
				t.Fatal(cmErr)
			}
			im := indexfile.NewManager(dir, nil, nil, cm)
			vaultInst := &VaultInstance{
				VaultID: vaultIDs[i],
				Type:    "file",
				Chunks:  cm,
				Indexes: im,
				Query:   query.New(cm, im, nil),
			}
			if isLeader {
				vaultInst.FollowerTargets = followerTargets
			} else {
				vaultInst.IsFollower = true
			}
			instances[i] = vaultInst
		}

		// Phase 2 (gastrolog-3iy5l): vaults are single-instance. Use the
		// first instance as the vault's instance; vaultCount > 1 is unused
		// post-collapse (callers that asked for it were transition tests
		// that have been removed).
		vault := NewVault(vaultID, instances[0])
		vault.Name = "cluster-vault"
		orch.RegisterVault(vault)

		nodes[nid] = &clusterTestNode{
			nodeID:       nid,
			orch:         orch,
			instances:    instances,
			instanceDirs: instanceDirs,
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
			for _, vaultInst := range n.instances {
				_ = vaultInst.Chunks.Close()
			}
		}
	})

	return &clusterHarness{
		nodes:    nodes,
		cfgStore: store,
		vaultID:  vaultID,
		vaultIDs: vaultIDs,
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
func (h *clusterHarness) sealAndReplicate(t *testing.T, leaderNode *clusterTestNode, vaultIdx int) {
	t.Helper()
	vaultInst := leaderNode.instances[vaultIdx]
	active := vaultInst.Chunks.Active()
	if active == nil || active.RecordCount == 0 {
		return
	}
	chunkID := active.ID
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatalf("seal vaultInst %d: %v", vaultIdx, err)
	}
	// Propagate seal to all follower nodes.
	for _, nid := range h.allNodeIDs() {
		if nid == leaderNode.nodeID {
			continue
		}
		followerVault := h.nodes[nid].instances[vaultIdx]
		if active := followerVault.Chunks.Active(); active != nil && active.ID == chunkID {
			if err := followerVault.Chunks.Seal(); err != nil {
				t.Fatalf("seal follower %s vaultInst %d: %v", nid, vaultIdx, err)
			}
		}
	}
	// Drain post-seal + replication jobs for the newly-sealed chunk.
	// A late ImportSealedChunk would recreate the chunk on a follower
	// after the transition delete has fired.
	leaderNode.orch.Scheduler().WaitIdle(30 * time.Second)
}

// assertVaultDirEmpty verifies that an instance's filesystem directory contains no
// chunk subdirectories on ANY node. This goes below the chunk manager API —
// it checks the actual filesystem to catch silent delete failures, leaked
// directories, and stale files.
func (h *clusterHarness) assertVaultDirEmpty(t *testing.T, vaultIdx int) {
	t.Helper()
	// Poll briefly — async chunk deletion may lag under CPU contention.
	deadline := time.Now().Add(60 * time.Second)
	for {
		allEmpty := true
		for _, nid := range h.allNodeIDs() {
			if len(h.chunkDirsOnNode(nid, vaultIdx)) > 0 {
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
		chunkDirs := h.chunkDirsOnNode(nid, vaultIdx)
		if len(chunkDirs) > 0 {
			t.Errorf("vault %d on %s: %d chunk directories still on disk: %v",
				vaultIdx, nid, len(chunkDirs), chunkDirs)
		}
	}
}

func (h *clusterHarness) chunkDirsOnNode(nid string, vaultIdx int) []string {
	dir := h.nodes[nid].instanceDirs[vaultIdx]
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

// listChunkDirsOnNode returns the chunk directory names in an instance dir on a node.
func (h *clusterHarness) listChunkDirsOnNode(t *testing.T, nodeID string, vaultIdx int) []string {
	t.Helper()
	dir := h.nodes[nodeID].instanceDirs[vaultIdx]
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

// TestExplicitStorageLeaderGetsRotationPolicy verifies that an instance built via
// buildInstanceForStorage (explicit placement path) applies the rotation
// policy from system. Regression test for a gap where applyRotationPolicy was
// only called in buildInstance but not buildInstanceForStorage.

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
