package orchestrator

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

const reregLocalNode = "node-local"

// homedVaultSystem configures each vault as homed on reregLocalNode: its
// leader placement sits on a file storage of that node, so every reload
// registers it with the pipeline as a home.
func homedVaultSystem(vaultIDs ...glid.GLID) *system.System {
	fsID := glid.New()
	sys := &system.System{Runtime: system.Runtime{
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID:       reregLocalNode,
			FileStorages: []system.FileStorage{{ID: fsID, StorageClass: 1, Name: "fs"}},
		}},
		VaultPlacements: make(map[glid.GLID][]system.VaultPlacement, len(vaultIDs)),
	}}
	for _, vid := range vaultIDs {
		sys.Config.Vaults = append(sys.Config.Vaults, system.VaultConfig{
			ID: vid, Name: "homed-" + vid.String(), Type: system.VaultTypeFile, Enabled: true,
		})
		sys.Runtime.VaultPlacements[vid] = []system.VaultPlacement{{StorageID: fsID.String(), Leader: true}}
	}
	return sys
}

// agedSealedManifest is a vault-ctl manifest holding sealed chunks far past
// any grace period, none of whose bytes the reconciler's chunk manager holds.
func agedSealedManifest(t *testing.T, n int) (*vaultctlfsm.FSM, []chunk.ChunkID) {
	t.Helper()
	fsm := vaultctlfsm.New()
	old := time.Now().Add(-48 * time.Hour)
	ids := make([]chunk.ChunkID, 0, n)
	for range n {
		id := chunk.NewChunkID()
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, old, old, old)}); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, old, 1, 1, old, old, old, false, old)}); err != nil {
			t.Fatalf("seal: %v", err)
		}
		ids = append(ids, id)
	}
	return fsm, ids
}

// deleteRecorder captures every CmdRequestDelete a reconciler proposes.
type deleteRecorder struct {
	mu  sync.Mutex
	ids []chunk.ChunkID
}

func (d *deleteRecorder) propose(id chunk.ChunkID, _ string, _ []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ids = append(d.ids, id)
	return nil
}

func (d *deleteRecorder) proposed() []chunk.ChunkID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]chunk.ChunkID(nil), d.ids...)
}

// leaderReconciler is the reconciler of a vault's placement-leader instance:
// not a follower, its vault-ctl group has a leader, and its delete proposals
// are recorded rather than applied. Its chunk manager holds no local chunks.
func leaderReconciler(orch *Orchestrator, vaultID glid.GLID, fsm *vaultctlfsm.FSM) (*VaultLifecycleReconciler, *deleteRecorder) {
	rec := &deleteRecorder{}
	inst := &VaultInstance{
		VaultID: vaultID,
		Chunks:  &reconcilerFakeChunkManager{},
		RaftLeadershipFacet: RaftLeadershipFacet{
			HasRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{ApplyRaftRequestDelete: rec.propose},
	}
	r := NewVaultLifecycleReconciler(orch, vaultID, inst, reregLocalNode, slog.Default())
	r.fsm.Store(fsm)
	return r, rec
}

// breakSegmentationRoot makes the vault's segmentation working dir a regular
// file, so the supervisor's next RegisterVault for the vault fails the way a
// damaged volume fails it. Returns the repair.
func breakSegmentationRoot(t *testing.T, orch *Orchestrator, vaultID glid.GLID) func() {
	t.Helper()
	working := paths.WorkingDir(pipelineVaultRoot(orch.segmentsDir, vaultID))
	if err := os.MkdirAll(filepath.Dir(working), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(working); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(working, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Remove(working); err != nil {
			t.Fatal(err)
		}
	}
}

// The vault-ctl handle arriving after boot re-registers a homed vault. While
// the supervisor holds no registration for it, every lock-free reader must
// still see a pipeline vault, and a reconcile tick landing in that window
// must not propose deleting the vault's chunks.
func TestPipelineReregistrationNeverUnpublishesTheVault(t *testing.T) {
	vaultID := glid.New()
	orch := newTestOrch(t, Config{
		LocalNodeID:  reregLocalNode,
		SystemLoader: &staticSystemLoader{sys: homedVaultSystem(vaultID)},
	})
	ctx := context.Background()

	// Boot: no vault-ctl group yet, so the vault registers without a handle.
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("boot ReloadFilters: %v", err)
	}
	if reg, ok := orch.lookupPipelineVault(vaultID); !ok || reg.hasHandle || !reg.home {
		t.Fatalf("premise: boot registers the vault as a home without a handle; got %+v ok=%v", reg, ok)
	}

	fsm, ids := agedSealedManifest(t, 3)
	rec, deletes := leaderReconciler(orch, vaultID, fsm)

	var windows int
	orch.pipelineReregisterHook = func(vid glid.GLID) {
		if vid != vaultID {
			return
		}
		windows++
		if !orch.isPipelineIngestVault(vaultID) {
			t.Error("vault is not a pipeline vault while its registration is being replaced")
		}
		if _, ok := orch.pipelineVaultChunkRoot(vaultID); !ok {
			t.Error("vault has no pipeline chunk root while its registration is being replaced")
		}
		// The reload holds o.mu; the tick runs on its own goroutine, as the
		// scheduler runs it, and the window stays open until it returns.
		done := make(chan struct{})
		go func() {
			defer close(done)
			rec.ReconcileTick()
		}()
		<-done
	}

	// The handle arrives: the next reload re-registers the vault with it.
	orch.groupMgr = singleNodeVaultCtlGroup(t, reregLocalNode, vaultID)
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("handle-arrival ReloadFilters: %v", err)
	}
	if windows != 1 {
		t.Fatalf("premise: re-registration ran the hook %d times, want 1", windows)
	}
	if reg, ok := orch.lookupPipelineVault(vaultID); !ok || !reg.hasHandle || reg.failed {
		t.Fatalf("after re-registration: got %+v ok=%v, want a live registration with the handle", reg, ok)
	}
	if got := deletes.proposed(); len(got) != 0 {
		t.Fatalf("reconcile tick inside the re-registration window proposed deleting pipeline chunks %v (manifest %v)", got, ids)
	}
}

// A re-registration the supervisor refuses leaves the vault published as a
// pipeline vault, marked failed, and the next reload retries it.
func TestPipelineFailedReregistrationKeepsTheVaultAPipelineVault(t *testing.T) {
	vaultID := glid.New()
	orch := newTestOrch(t, Config{
		LocalNodeID:  reregLocalNode,
		SystemLoader: &staticSystemLoader{sys: homedVaultSystem(vaultID)},
	})
	ctx := context.Background()
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("boot ReloadFilters: %v", err)
	}

	repair := breakSegmentationRoot(t, orch, vaultID)
	orch.groupMgr = singleNodeVaultCtlGroup(t, reregLocalNode, vaultID)
	if err := orch.ReloadFilters(ctx); err == nil {
		t.Fatal("premise: re-registration onto a damaged segmentation root must fail")
	}

	reg, ok := orch.lookupPipelineVault(vaultID)
	if !ok {
		t.Fatal("a failed re-registration dropped the vault from the pipeline registrations")
	}
	if !reg.failed || !reg.home || !reg.hasHandle {
		t.Fatalf("registration after failure = %+v, want the wanted roles marked failed", reg)
	}
	if !orch.isPipelineIngestVault(vaultID) {
		t.Fatal("a failed re-registration made the vault stop being a pipeline vault")
	}
	if _, ok := orch.pipelineVaultChunkRoot(vaultID); !ok {
		t.Fatal("a failed re-registration hid the vault's chunk root")
	}

	fsm, _ := agedSealedManifest(t, 3)
	rec, deletes := leaderReconciler(orch, vaultID, fsm)
	rec.ReconcileTick()
	if got := deletes.proposed(); len(got) != 0 {
		t.Fatalf("reconcile tick after a failed re-registration proposed deletes %v", got)
	}

	// A failed registration never matches the wanted one, so the reload
	// after the repair registers the vault for real.
	repair()
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters after repair: %v", err)
	}
	if reg, ok := orch.lookupPipelineVault(vaultID); !ok || reg.failed {
		t.Fatalf("registration after repair = %+v ok=%v, want a live registration", reg, ok)
	}
	if !orch.pipeline.ChunkingRegistered(vaultID) {
		t.Fatal("the retried registration did not register chunking for the home")
	}
}

// A vault whose very first registration fails is still published, and its
// failure does not keep the reload from registering the other vaults.
func TestPipelineFirstRegistrationFailurePublishesVaultAndSparesOthers(t *testing.T) {
	broken, healthy := glid.New(), glid.New()
	orch := newTestOrch(t, Config{
		LocalNodeID:  reregLocalNode,
		SystemLoader: &staticSystemLoader{sys: homedVaultSystem(broken, healthy)},
	})
	breakSegmentationRoot(t, orch, broken)

	if err := orch.ReloadFilters(context.Background()); err == nil {
		t.Fatal("premise: registering onto a damaged segmentation root must fail")
	}
	if reg, ok := orch.lookupPipelineVault(broken); !ok || !reg.failed {
		t.Fatalf("broken vault registration = %+v ok=%v, want published and marked failed", reg, ok)
	}
	if !orch.isPipelineIngestVault(broken) {
		t.Fatal("a vault whose registration failed is not treated as a pipeline vault")
	}
	if reg, ok := orch.lookupPipelineVault(healthy); !ok || reg.failed {
		t.Fatalf("healthy vault registration = %+v ok=%v, want a live registration", reg, ok)
	}
}

// Every configured vault's chunks come from the pipeline, so no reconcile
// pass retires a manifest entry because the bytes are missing locally —
// including on a vault this node holds an instance for without a pipeline
// registration: one whose placement leader resolves to no node, where every
// node builds a leader instance and none is a home.
func TestReconcileTickNeverRetiresChunksMissingLocally(t *testing.T) {
	vaultID := glid.New()
	sys := &system.System{
		Config: system.Config{Vaults: []system.VaultConfig{{
			ID: vaultID, Name: "unresolved-leader", Type: system.VaultTypeFile, Enabled: true,
		}}},
		Runtime: system.Runtime{VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {{StorageID: glid.New().String(), Leader: true}},
		}},
	}
	orch := newTestOrch(t, Config{
		LocalNodeID:  reregLocalNode,
		SystemLoader: &staticSystemLoader{sys: sys},
	})
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if _, ok := orch.lookupPipelineVault(vaultID); ok {
		t.Fatal("premise: a vault homed nowhere and routed nowhere has no pipeline registration")
	}

	fsm, _ := agedSealedManifest(t, 3)
	rec, deletes := leaderReconciler(orch, vaultID, fsm)
	rec.ReconcileTick()
	rec.ReconcileMembershipCatchup()
	if got := deletes.proposed(); len(got) != 0 {
		t.Fatalf("reconcile proposed deleting chunks only because their bytes are not local: %v", got)
	}
	if n := len(fsm.List()); n != 3 {
		t.Fatalf("manifest holds %d entries, want all 3", n)
	}
}
