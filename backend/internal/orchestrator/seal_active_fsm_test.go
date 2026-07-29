package orchestrator

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// postSealDrainBudget bounds the scheduler drain these tests use to wait out
// the async post-seal job. It is a give-up budget, not a timing assertion:
// the job is in-process and finishes in milliseconds, and a test that needs
// the whole budget has found a wedge, not a slow machine.
const postSealDrainBudget = 30 * time.Second

// directCtlApplier applies vault-ctl commands straight to a local FSM,
// standing in for the Raft round trip. Announcer.apply swallows errors, so
// returning them here only surfaces in the warn log — assertions read the
// FSM instead.
type directCtlApplier struct{ fsm *vaultctlfsm.FSM }

func (d directCtlApplier) Apply(data []byte) error {
	d.fsm.Apply(&hraft.Log{Data: data})
	return nil
}

// sealActiveFixture wires a file-backed vault instance whose chunk manager
// announces onto a local vault-ctl FSM, registered on the orchestrator as a
// pipeline vault (which is what every route destination and every
// home-placed vault is in production — see reloadPipelineFromConfig).
func newSealActiveFixture(t *testing.T) (*Orchestrator, glid.GLID, *vaultctlfsm.FSM) {
	t.Helper()
	vaultID := glid.New()
	fsm := vaultctlfsm.New()
	inst, _ := newFileInstance(t, vaultID)
	setter, ok := inst.Chunks.(chunk.AnnouncerSetter)
	if !ok {
		t.Fatalf("file chunk manager does not implement chunk.AnnouncerSetter")
	}
	setter.SetAnnouncer(vaultctlfsm.NewAnnouncer(directCtlApplier{fsm}, nil, slog.Default()))

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.RegisterVault(NewVault(vaultID, inst))
	orch.mu.Lock()
	orch.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})
	orch.mu.Unlock()
	return orch, vaultID, fsm
}

func fsmState(t *testing.T, fsm *vaultctlfsm.FSM, id chunk.ChunkID) chunk.ChunkState {
	t.Helper()
	e := fsm.Get(id)
	if e == nil {
		t.Fatalf("chunk %s absent from vault-ctl FSM", id)
	}
	return e.State
}

// TestSealActivePromotesFSMEntryToSealed pins the invariant that a seal of a
// chunk-manager active chunk drives the vault-ctl manifest entry all the way
// to Sealed. A half-sealed entry parked in Sealing is never promoted, so
// grounded reads and retention see a chunk that is neither writable nor
// durable-complete.
func TestSealActivePromotesFSMEntryToSealed(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm := newSealActiveFixture(t)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	active := orch.vaults[vaultID].Instance.Chunks.Active()
	if active == nil {
		t.Fatal("no active chunk after append")
	}
	chunkID := active.ID
	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateActive {
		t.Fatalf("pre-seal FSM state = %s, want active", got)
	}

	sealed, err := orch.SealActive(vaultID)
	if err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	if sealed != 1 {
		t.Fatalf("SealActive sealed %d vaults, want 1", sealed)
	}

	// The Sealing → Sealed half of the transition rides the post-seal job
	// (GLCB assembly, then AnnounceSeal). Drain the scheduler rather than
	// asserting on the intermediate state.
	requireIdle(t, orch.scheduler, postSealDrainBudget)

	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateSealed {
		t.Fatalf("post-seal FSM state = %s, want sealed (entry stranded mid-seal)", got)
	}
}

// TestSealActiveWithoutVaultCtlGroupIsNoop separates the two outcomes that look
// alike from the outside. A vault with no vault-ctl group (memory instance,
// single-node config) has no announcer at all, so a seal announces nothing and
// there is no manifest entry to promote — silence there is correct. That is not
// the same defect as an entry that WAS committed as Sealing and then never
// promoted, which is what TestSealActivePromotesFSMEntryToSealed covers.
func TestSealActiveWithoutVaultCtlGroupIsNoop(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	if _, ok := inst.Chunks.(chunk.AnnouncerSetter); ok {
		t.Fatal("memory chunk manager unexpectedly supports SetAnnouncer; fixture assumption broken")
	}
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.RegisterVault(NewVault(vaultID, inst))
	orch.mu.Lock()
	orch.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})
	orch.mu.Unlock()

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	sealed, err := orch.SealActive(vaultID)
	if err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	if sealed != 1 {
		t.Fatalf("SealActive sealed %d vaults, want 1", sealed)
	}
	requireIdle(t, orch.scheduler, postSealDrainBudget)

	metas, err := inst.Chunks.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 1 || !metas[0].Sealed {
		t.Fatalf("local chunks = %+v, want exactly one sealed chunk", metas)
	}
}

// TestLocalTeardownSealDoesNotStrandManifestEntry covers the teardown arm.
// sealAndDeleteAllChunks demotes the active chunk only so it can be deleted
// locally; it discards the bytes rather than assembling a sealed form, so it
// must not announce a cluster-wide Active → Sealing transition that nothing
// will ever complete. The manifest entry must be left exactly as it was.
func TestLocalTeardownSealDoesNotStrandManifestEntry(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm := newSealActiveFixture(t)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	vaultInst := orch.vaults[vaultID].Instance
	chunkID := vaultInst.Chunks.Active().ID
	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateActive {
		t.Fatalf("pre-teardown FSM state = %s, want active", got)
	}

	orch.sealAndDeleteAllChunks(vaultInst, "teardown-test", vaultID)
	requireIdle(t, orch.scheduler, postSealDrainBudget)

	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateActive {
		t.Fatalf("post-teardown FSM state = %s, want active — a local teardown must not "+
			"announce a lifecycle transition it never completes", got)
	}
	metas, err := vaultInst.Chunks.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("local chunks after teardown = %+v, want none", metas)
	}
}
