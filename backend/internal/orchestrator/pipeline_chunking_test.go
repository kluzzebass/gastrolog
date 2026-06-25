package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// These tests exercise the Rubicon D chunking lifecycle wired the way
// buildPipelineVaultSpec wires it in production: a real per-vault vault-ctl FSM,
// the leader-forwarding applier seam (fsmApplier), the production
// chunking.VaultSegmentLocator over a home's head/ then completed/, and real
// segment bytes produced by the origin pipeline (originFixture, defined in
// pipeline_collection_test.go). The FSM is shared between the planning leader
// and the building homes — a single replicated state machine is exactly what
// Raft converges every node to — so a shared instance models the cross-node
// manifest without standing up Raft.
//
// Builds are driven synchronously via PlanOnce/RotateCron/BuildOnce so the
// lifecycle is deterministic; the manager's async Run path (callback-driven) is
// covered in the chunking package's own manager_test.go.

// collectorFunc adapts closures to chunking's SegmentCollector.
type collectorFunc struct {
	once   func(context.Context) error
	byID   func(context.Context, []glid.GLID) error
}

func (f collectorFunc) CollectOnce(ctx context.Context) error {
	if f.once != nil {
		return f.once(ctx)
	}
	return nil
}

func (f collectorFunc) CollectSegments(ctx context.Context, segmentIDs []glid.GLID) error {
	if f.byID != nil {
		return f.byID(ctx, segmentIDs)
	}
	return nil
}

// copyCompletedToHead copies the origin's completed segment file into a home's
// head/ directory, modeling a segment this home already holds (post-collection).
func copyCompletedToHead(t *testing.T, originRoot, homeRoot string, segID glid.GLID) {
	t.Helper()
	if err := paths.EnsureHeadDir(homeRoot); err != nil {
		t.Fatalf("ensure head dir: %v", err)
	}
	data, err := os.ReadFile(paths.CompletedSegment(originRoot, segID))
	if err != nil {
		t.Fatalf("read completed segment: %v", err)
	}
	if err := os.WriteFile(paths.HeadSegment(homeRoot, segID), data, 0o600); err != nil {
		t.Fatalf("write head segment: %v", err)
	}
}

// planUntilOpenRef drives the leader planner until the open manifest holds at
// least one segment ref (Open then AddRef), then returns. Fails on timeout.
func planUntilOpenRef(t *testing.T, ctx context.Context, mgr *chunking.Manager, fsm *vaultctlfsm.FSM, vaultID glid.GLID) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
		if open := fsm.OpenChunk(); open != nil && len(open.Refs) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("planner never added a segment ref to the open manifest")
}

// chunkingSpec mirrors the chunking VaultConfig that buildPipelineVaultSpec
// produces for a home with a vault-ctl handle.
func chunkingSpec(home string, fsm *vaultctlfsm.FSM, isLeader func() bool) chunking.VaultConfig {
	return chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.VaultSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  isLeader,
	}
}

// TestPipelineChunkingCronSealsIdleManifest: with no size/age/record threshold,
// an open manifest with content never auto-rotates; a scheduler-driven
// RotateCron pass (CronDue) seals it on the leader, after which the leader
// builds the GLCB and announces SealChunk (which clears the sealed manifest).
// A non-leader RotateCron is a no-op.
func TestPipelineChunkingCronSealsIdleManifest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	home := t.TempDir()
	copyCompletedToHead(t, origin.root, home, segID)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	planUntilOpenRef(t, ctx, mgr, fsm, vaultID)

	// No rotation policy -> idle planner never seals on its own.
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatalf("PlanOnce idle: %v", err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("manifest sealed without a threshold or cron trigger")
	}

	// Cron forces the seal.
	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("cron rotation did not seal the open manifest")
	}

	// Leader build produces the GLCB and announces SealChunk (clears manifest).
	chunkID := fsm.SealedManifest().ChunkID
	if err := mgr.BuildOnce(ctx, vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)); err != nil {
		t.Fatalf("GLCB not built after cron seal + build: %v", err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after the leader announces SealChunk")
	}
}

// TestPipelineChunkingCronNonLeaderNoOp: a RotateCron pass on a non-leader home
// never seals, even with a non-empty open manifest — only the vault-ctl leader
// proposes manifest edits.
func TestPipelineChunkingCronNonLeaderNoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	leaderHome := t.TempDir()
	copyCompletedToHead(t, origin.root, leaderHome, segID)

	// Open + AddRef as leader so the FSM holds a non-empty open manifest.
	leader := chunking.New(chunking.Config{})
	if err := leader.RegisterVault(vaultID, chunkingSpec(leaderHome, fsm, func() bool { return true })); err != nil {
		t.Fatalf("leader RegisterVault: %v", err)
	}
	planUntilOpenRef(t, ctx, leader, fsm, vaultID)

	// A follower's RotateCron must not seal the shared manifest.
	followerHome := t.TempDir()
	copyCompletedToHead(t, origin.root, followerHome, segID)
	follower := chunking.New(chunking.Config{})
	if err := follower.RegisterVault(vaultID, chunkingSpec(followerHome, fsm, func() bool { return false })); err != nil {
		t.Fatalf("follower RegisterVault: %v", err)
	}
	if err := follower.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("follower RotateCron: %v", err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("non-leader RotateCron sealed the manifest")
	}
}

// TestPipelineChunkingLeaderGatedPlanningAndFailover: planning is gated on the
// live vault-ctl leader closure. A follower plans nothing; once it becomes
// leader it opens and fills the manifest; if it loses leadership mid-manifest
// planning halts without sealing; on regaining leadership a cron pass resumes
// and seals.
func TestPipelineChunkingLeaderGatedPlanningAndFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	home := t.TempDir()
	copyCompletedToHead(t, origin.root, home, segID)

	var isLeader atomic.Bool
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunkingSpec(home, fsm, isLeader.Load)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	// Follower: planning is a no-op, nothing is opened.
	for range 5 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("follower PlanOnce: %v", err)
		}
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("follower opened a chunk manifest")
	}

	// Becomes leader: opens and fills the manifest.
	isLeader.Store(true)
	planUntilOpenRef(t, ctx, mgr, fsm, vaultID)

	// Loses leadership mid-manifest: planning halts, no seal.
	isLeader.Store(false)
	for range 5 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("demoted PlanOnce: %v", err)
		}
	}
	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("demoted RotateCron: %v", err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("a demoted node sealed the manifest")
	}

	// Regains leadership: cron resumes planning to seal.
	isLeader.Store(true)
	if err := mgr.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("re-promoted RotateCron: %v", err)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("re-promoted leader did not resume to seal the manifest")
	}
}

// TestPipelineChunkingByteIdenticalAcrossHomes: every home that holds the same
// segments and the same sealed manifest builds a byte-identical GLCB. The
// leader plans and seals once; two follower homes each build from their own
// head/ copy and the resulting chunk blobs are byte-for-byte equal.
func TestPipelineChunkingByteIdenticalAcrossHomes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	home1 := t.TempDir()
	home2 := t.TempDir()
	copyCompletedToHead(t, origin.root, home1, segID)
	copyCompletedToHead(t, origin.root, home2, segID)

	// Leader (using home1's copy to plan) opens, fills, and cron-seals.
	leader := chunking.New(chunking.Config{})
	if err := leader.RegisterVault(vaultID, chunkingSpec(home1, fsm, func() bool { return true })); err != nil {
		t.Fatalf("leader RegisterVault: %v", err)
	}
	planUntilOpenRef(t, ctx, leader, fsm, vaultID)
	if err := leader.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	sealed := fsm.SealedManifest()
	if sealed == nil {
		t.Fatal("manifest not sealed")
	}
	chunkID := sealed.ChunkID

	// Two follower homes build from their own copies. Use no Applier so each
	// build does not clear the shared sealed manifest before the next home runs.
	build := func(home string) []byte {
		mgr := chunking.New(chunking.Config{})
		spec := chunkingSpec(home, fsm, func() bool { return false })
		spec.Applier = nil
		if err := mgr.RegisterVault(vaultID, spec); err != nil {
			t.Fatalf("home %s RegisterVault: %v", home, err)
		}
		if err := mgr.BuildOnce(ctx, vaultID); err != nil {
			t.Fatalf("home %s BuildOnce: %v", home, err)
		}
		data, err := os.ReadFile(chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID))
		if err != nil {
			t.Fatalf("home %s read GLCB: %v", home, err)
		}
		return data
	}

	g1 := build(home1)
	g2 := build(home2)
	if len(g1) == 0 {
		t.Fatal("empty GLCB")
	}
	if string(g1) != string(g2) {
		t.Fatalf("GLCB not byte-identical across homes: %d vs %d bytes", len(g1), len(g2))
	}
}

// TestPipelineChunkingMaterializesSegmentsBeforeBuild: GLCB build collects
// manifest-referenced segment bytes before attempting merge.
func TestPipelineChunkingMaterializesSegmentsBeforeBuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	home := t.TempDir()
	copyCompletedToHead(t, origin.root, home, segID)

	// Plan + cron-seal while the segment is present.
	leader := chunking.New(chunking.Config{})
	if err := leader.RegisterVault(vaultID, chunkingSpec(home, fsm, func() bool { return true })); err != nil {
		t.Fatalf("leader RegisterVault: %v", err)
	}
	planUntilOpenRef(t, ctx, leader, fsm, vaultID)
	if err := leader.RotateCron(ctx, vaultID); err != nil {
		t.Fatalf("RotateCron: %v", err)
	}
	chunkID := fsm.SealedManifest().ChunkID

	// Remove the local copy: build must materialize before merge.
	if err := os.Remove(paths.HeadSegment(home, segID)); err != nil {
		t.Fatalf("remove head segment: %v", err)
	}

	var collects atomic.Int32
	cfg := chunkingSpec(home, fsm, func() bool { return false })
	cfg.Collector = collectorFunc{
		byID: func(_ context.Context, _ []glid.GLID) error {
			collects.Add(1)
			copyCompletedToHead(t, origin.root, home, segID)
			return nil
		},
	}
	buildMgr := chunking.New(chunking.Config{})
	if err := buildMgr.RegisterVault(vaultID, cfg); err != nil {
		t.Fatalf("buildMgr RegisterVault: %v", err)
	}

	if err := buildMgr.BuildOnce(ctx, vaultID); err != nil {
		t.Fatalf("BuildOnce after segment materialization: %v", err)
	}
	if got := collects.Load(); got != 1 {
		t.Fatalf("segment collects = %d, want 1", got)
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)); err != nil {
		t.Fatalf("GLCB not built after materialization: %v", err)
	}
}
