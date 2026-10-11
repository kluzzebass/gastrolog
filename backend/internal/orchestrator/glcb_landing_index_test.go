package orchestrator

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/query"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/record"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

const landingHomeNode = "node-home"

func registerPipelineHome(o *Orchestrator, vaultID glid.GLID, hasHandle bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: hasHandle})
}

// placeGLCBOnHome copies a built GLCB into the vault's chunk root on o, where
// a build, pull or transfer copy would have put it.
func placeGLCBOnHome(t *testing.T, o *Orchestrator, vaultID glid.GLID, fx sealedGLCBFixture) string {
	t.Helper()
	root, ok := o.pipelineVaultChunkRoot(vaultID)
	if !ok {
		t.Fatalf("premise: vault %s is a pipeline home on this node", vaultID)
	}
	dst := chunking.ChunkGLCBPath(root, fx.sealed.ID)
	data, err := os.ReadFile(fx.glcbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

// fileVaultInstance builds a file chunk manager and the production index
// manager over a fresh directory.
func fileVaultInstance(t *testing.T, vaultID glid.GLID) (*VaultInstance, *chunkfile.Manager, index.IndexManager) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	im, err := indexfile.NewFactory()(map[string]string{"dir": dir}, cm, nil)
	if err != nil {
		t.Fatalf("index manager: %v", err)
	}
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	ti := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: cm, Indexes: im, Query: query.New(cm, im, nil)}
	return ti, cm, im
}

func applySeal(t *testing.T, fsm *vaultctlfsm.FSM, e vaultctlfsm.ManifestEntry) {
	t.Helper()
	for _, cmd := range sealCommands(e) {
		if err := fsm.Apply(&hraft.Log{Data: cmd}); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
}

func sealCommands(e vaultctlfsm.ManifestEntry) [][]byte {
	return [][]byte{
		vaultctlfsm.MarshalCreateChunk(e.ID, e.WriteStart, e.IngestStart, e.SourceStart),
		vaultctlfsm.MarshalSealChunk(e.ID, e.WriteEnd, e.RecordCount, e.Bytes, e.IngestStart, e.IngestEnd, e.SourceEnd, e.IngestTSMonotonic, e.SealedAt, e.Hash),
	}
}

func indexJobsScheduled(o *Orchestrator) []string {
	var names []string
	for _, j := range o.Scheduler().ListJobs() {
		if strings.HasPrefix(j.Name, "index-rebuild:") {
			names = append(names, j.Name)
		}
	}
	return names
}

// sealApplyHome is a pipeline home whose vault instance and reconciler are up,
// with the reconciler wired to a vault-ctl FSM that does not know the chunk yet.
func sealApplyHome(t *testing.T, ctx context.Context) (*Orchestrator, sealedGLCBFixture, *chunkfile.Manager, index.IndexManager, *vaultctlfsm.FSM) {
	t.Helper()
	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api", "level": "info"})
	orch := newTestOrch(t, Config{LocalNodeID: landingHomeNode})
	registerPipelineHome(orch, fx.vaultID, false)
	ti, cm, im := fileVaultInstance(t, fx.vaultID)
	orch.RegisterVault(NewVault(fx.vaultID, ti))
	fsm := vaultctlfsm.New()
	ti.Reconciler = NewVaultLifecycleReconciler(orch, fx.vaultID, ti, landingHomeNode, slog.Default())
	ti.Reconciler.Wire(fsm)
	return orch, fx, cm, im, fsm
}

// A restart replays every seal committed after the last vault-ctl snapshot,
// and no build or pull fires again for the copies the home already holds. The
// seal apply is what indexes them.
func TestSealApplyIndexesTheCopyTheHomeAlreadyHolds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orch, fx, cm, im, fsm := sealApplyHome(t, ctx)
	glcbPath := placeGLCBOnHome(t, orch, fx.vaultID, fx)
	if err := cm.RegisterExternalGLCB(fx.sealed.ID, glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	applySeal(t, fsm, fx.sealed)
	awaitIndexes(t, orch, im, fx.sealed.ID)
}

// A seal that commits before this home's build finishes has no copy to index:
// it schedules nothing, and the build that lands the copy indexes it.
func TestSealApplyBeforeTheBuildLeavesIndexingToTheBuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orch, fx, cm, im, fsm := sealApplyHome(t, ctx)

	applySeal(t, fsm, fx.sealed)
	if jobs := indexJobsScheduled(orch); len(jobs) != 0 {
		t.Fatalf("seal apply scheduled index builds for a copy this home does not hold: %v", jobs)
	}

	glcbPath := placeGLCBOnHome(t, orch, fx.vaultID, fx)
	if err := cm.RegisterExternalGLCB(fx.sealed.ID, glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	orch.onPipelineChunkBuilt(fx.vaultID, fsm, fx.sealed.ID)
	awaitIndexes(t, orch, im, fx.sealed.ID)
}

// A retention transfer whose source and target are both homed on this node
// copies the GLCB across vault directories; the target's copy is indexed like
// any other GLCB that lands on a home.
func TestLocalTransferCopyIndexesTheTargetCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api", "level": "info"})
	orch := newTestOrch(t, Config{LocalNodeID: landingHomeNode})
	sourceID := fx.vaultID
	targetID := glid.New()
	registerPipelineHome(orch, sourceID, false)
	registerPipelineHome(orch, targetID, false)
	placeGLCBOnHome(t, orch, sourceID, fx)

	targetRoot, _ := orch.pipelineVaultChunkRoot(targetID)
	ti, _, im := fileVaultInstance(t, targetID)
	installGLCBResolverOn(ti, func() (*vaultctlfsm.FSM, string, bool) { return fx.fsm, targetRoot, true })
	orch.RegisterVault(NewVault(targetID, ti))

	r := &retentionRunner{orch: orch, vaultID: sourceID, logger: slog.Default()}
	r.tryLocalTransferCopy(targetID, fx.sealed.ID, fx.sealed)
	if _, err := os.Stat(chunking.ChunkGLCBPath(targetRoot, fx.sealed.ID)); err != nil {
		t.Fatalf("premise: the transfer copied the GLCB into the target's chunk root: %v", err)
	}
	awaitIndexes(t, orch, im, fx.sealed.ID)
}

// placedHome is a node placed as the only home of a file vault whose vault-ctl
// group has already committed the seal of a chunk built elsewhere, with no
// vault instance or pipeline registration on the node yet.
type placedHome struct {
	orch      *Orchestrator
	fx        sealedGLCBFixture
	vaultFSM  *vaultctlfsm.FSM
	factories Factories
}

func newPlacedHome(t *testing.T, ctx context.Context) placedHome {
	t.Helper()
	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api", "level": "info"})
	vaultID := fx.vaultID
	storageID := glid.New()
	sys := &system.System{
		Config: system.Config{Vaults: []system.VaultConfig{{ID: vaultID, Name: "landing", Enabled: true, Type: system.VaultTypeFile, StorageClass: 1}}},
		Runtime: system.Runtime{
			NodeStorageConfigs: []system.NodeStorageConfig{{
				NodeID:       landingHomeNode,
				FileStorages: []system.FileStorage{{ID: storageID, StorageClass: 1, Name: "disk", Path: t.TempDir()}},
			}},
			VaultPlacements: map[glid.GLID][]system.VaultPlacement{vaultID: {{StorageID: storageID.String(), Leader: true}}},
		},
	}
	orch := newTestOrch(t, Config{LocalNodeID: landingHomeNode, SystemLoader: &staticSystemLoader{sys: sys}})
	gm := singleNodeVaultCtlGroup(t, landingHomeNode, vaultID)
	orch.groupMgr = gm
	g := gm.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	for _, cmd := range sealCommands(fx.sealed) {
		if err := g.Raft.Apply(vaultraft.MarshalVaultChunkCommand(vaultID, cmd), 10*time.Second).Error(); err != nil {
			t.Fatalf("raft apply: %v", err)
		}
	}
	vaultFSM, _, _, ok := orch.vaultCtlHandle(vaultID)
	if !ok || vaultFSM.Get(fx.sealed.ID) == nil {
		t.Fatal("premise: the vault-ctl FSM holds the sealed chunk")
	}
	orch.RegisterVault(NewVault(vaultID, nil))
	return placedHome{
		orch:     orch,
		fx:       fx,
		vaultFSM: vaultFSM,
		factories: Factories{
			GroupManager:  gm,
			ChunkManagers: map[string]chunk.ManagerFactory{"file": chunkfile.NewFactory()},
			IndexManagers: map[string]index.ManagerFactory{"file": indexfile.NewFactory()},
		},
	}
}

func (p placedHome) addInstance(t *testing.T, ctx context.Context) *VaultInstance {
	t.Helper()
	if err := p.orch.AddVaultInstance(ctx, p.fx.vaultID, p.factories); err != nil {
		t.Fatalf("AddVaultInstance: %v", err)
	}
	ti := p.orch.findLocalVaultInstance(p.fx.vaultID)
	if ti == nil {
		t.Fatal("premise: AddVaultInstance built the instance")
	}
	return ti
}

// A home registration precedes the vault instance when a placement adds this
// node: chunking can finish a build before the instance exists, and that
// build's own landing finds no instance to index into. The instance's arrival
// indexes it.
func TestGLCBBuiltBeforeTheInstanceIsIndexedWhenTheInstanceArrives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPlacedHome(t, ctx)

	registerPipelineHome(p.orch, p.fx.vaultID, true)
	placeGLCBOnHome(t, p.orch, p.fx.vaultID, p.fx)
	p.orch.onPipelineChunkBuilt(p.fx.vaultID, p.vaultFSM, p.fx.sealed.ID)
	if jobs := indexJobsScheduled(p.orch); len(jobs) != 0 {
		t.Fatalf("premise: a build with no vault instance indexes nothing, got %v", jobs)
	}

	ti := p.addInstance(t, ctx)
	awaitIndexes(t, p.orch, ti.Indexes, p.fx.sealed.ID)
}

// At boot the vault instance comes up before the vault's home registration,
// and the vault-ctl FSM already holds the seals its snapshot carries. The home
// registration is what makes those GLCBs visible to the instance, so it is
// what indexes them.
func TestHomeRegistrationIndexesGLCBsTheInstanceAlreadyHas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPlacedHome(t, ctx)

	ti := p.addInstance(t, ctx)
	if _, _, ok := p.orch.pipelineGLCBSource(p.fx.vaultID); ok {
		t.Fatal("premise: the vault is not registered as a pipeline home before the reload")
	}
	root := pipelineChunkRoot(pipelineVaultRoot(p.orch.segmentsDir, p.fx.vaultID))
	data, err := os.ReadFile(p.fx.glcbPath)
	if err != nil {
		t.Fatal(err)
	}
	dst := chunking.ChunkGLCBPath(root, p.fx.sealed.ID)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := p.orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if _, _, ok := p.orch.pipelineGLCBSource(p.fx.vaultID); !ok {
		t.Fatal("premise: the reload registers the vault as a pipeline home with a vault-ctl handle")
	}
	awaitIndexes(t, p.orch, ti.Indexes, p.fx.sealed.ID)
}
