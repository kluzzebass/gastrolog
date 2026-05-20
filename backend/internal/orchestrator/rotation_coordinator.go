// FSM-mediated rotation coordinator for the fan-out data plane
// (gastrolog-3yre7). Implements chunk.RotationCoordinator by
// proposing CmdBeginSeal(oldID) + CmdCreateChunk(newID) through
// vault-ctl Raft and returning the canonical new Active chunk ID.
//
// Under fan-out every replica's chunk manager can fire its local
// rotation policy concurrently. Raft serializes the proposals; the
// FSM's single-Active invariant (vaultctlfsm.ErrActiveChunkExists)
// discriminates winners from losers. The losing proposer's candidate
// chunk ID is leak-free — no records ever associated with it — and
// the coordinator reads the FSM's canonical Active for the losing
// replica so its chunk manager opens a chunk that aligns with every
// other replica.

package orchestrator

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// rotationCoordinator round-trips rotation proposals through vault-ctl
// Raft. It implements chunk.RotationCoordinator.
//
// The coordinator carries the orchestrator's per-vault Receiving
// snapshot so the CmdCreateChunk payload stamps the initial placement
// on the new chunk. The snapshot is refreshed by the orchestrator on
// every placement change via SetReceiving — kept lock-free for the
// read path since rotation is read-heavy.
type rotationCoordinator struct {
	vaultID glid.GLID
	applier vaultctlfsm.Applier
	fsm     *vaultctlfsm.FSM
	now     func() time.Time

	mu        sync.RWMutex
	receiving []string // snapshot of placement node IDs; stamped on every new chunk
}

var _ chunk.RotationCoordinator = (*rotationCoordinator)(nil)

// newRotationCoordinator constructs a coordinator bound to a specific
// vault's Raft group + FSM. The applier must be a vaultCtlApplier (or
// equivalent) that wraps payloads as vault control-plane chunk
// commands.
func newRotationCoordinator(vaultID glid.GLID, applier vaultctlfsm.Applier, fsm *vaultctlfsm.FSM, now func() time.Time, receiving []string) *rotationCoordinator {
	if now == nil {
		now = time.Now
	}
	cp := append([]string(nil), receiving...)
	return &rotationCoordinator{
		vaultID:   vaultID,
		applier:   applier,
		fsm:       fsm,
		now:       now,
		receiving: cp,
	}
}

// SetReceiving updates the snapshot stamped on subsequent
// BeginRotation proposals. Called from the orchestrator on every
// placement edit so the next CmdCreateChunk carries the current
// member list.
func (c *rotationCoordinator) SetReceiving(receiving []string) {
	cp := append([]string(nil), receiving...)
	c.mu.Lock()
	c.receiving = cp
	c.mu.Unlock()
}

// BeginRotation proposes CmdBeginSeal(oldID) + CmdCreateChunk(newID)
// via vault-ctl Raft and returns the canonical new chunk ID after
// both entries commit. The caller then seals oldID locally and opens
// the returned ID without further announcing.
//
// Race semantics: if this caller's CmdCreateChunk is rejected by the
// FSM single-Active invariant — another replica's rotation got there
// first — the coordinator reads the FSM's current Active and returns
// that ID instead. Either path ends with the caller using the same
// chunk ID every other replica is using.
//
// oldID may be the zero value when there's no current Active to
// transition (first chunk for a vault, or a vault that just had its
// active deleted). In that case CmdBeginSeal is skipped.
func (c *rotationCoordinator) BeginRotation(oldID chunk.ChunkID) (chunk.ChunkID, error) {
	if c.applier == nil {
		return chunk.ChunkID{}, errors.New("rotation coordinator: no applier configured")
	}

	// Step 1: BeginSeal the old chunk so the single-Active slot is
	// freed before we propose the new chunk. Idempotent if the chunk
	// is already Sealing (e.g., another replica's BeginSeal won the
	// race and applied first).
	if oldID != (chunk.ChunkID{}) {
		if err := c.applier.Apply(vaultctlfsm.MarshalBeginSeal(oldID)); err != nil {
			return chunk.ChunkID{}, fmt.Errorf("begin-seal: %w", err)
		}
	}

	// Step 2: propose CmdCreateChunk for our candidate new ID, stamped
	// with the current Receiving snapshot.
	newID := chunk.NewChunkID()
	now := c.now()

	c.mu.RLock()
	receiving := append([]string(nil), c.receiving...)
	c.mu.RUnlock()

	var createPayload []byte
	if len(receiving) > 0 {
		createPayload = vaultctlfsm.MarshalCreateChunkWithReceiving(newID, now, now, now, receiving)
	} else {
		createPayload = vaultctlfsm.MarshalCreateChunk(newID, now, now, now)
	}

	err := c.applier.Apply(createPayload)
	if err == nil {
		return newID, nil // our proposal won
	}

	// Lost the race: another replica's CmdCreateChunk applied first
	// and the FSM rejected ours. Read the FSM for the canonical
	// Active and return that ID. The caller's chunk manager opens
	// locally with the returned ID so cluster-wide chunk identity
	// stays aligned.
	if !errors.Is(err, vaultctlfsm.ErrActiveChunkExists) {
		return chunk.ChunkID{}, fmt.Errorf("create-chunk: %w", err)
	}

	if c.fsm == nil {
		return chunk.ChunkID{}, errors.New("rotation coordinator: lost race but no FSM to read canonical Active")
	}
	for _, e := range c.fsm.List() {
		if e.State == chunk.ChunkStateActive {
			return e.ID, nil
		}
	}
	// No Active in the FSM — the winning proposer's chunk transitioned
	// out (e.g., immediate seal) between our rejection and our read.
	// Treat this as a transient failure; the caller can retry.
	return chunk.ChunkID{}, errors.New("rotation coordinator: lost race but FSM has no canonical Active to align to")
}

// activeChunkIDs returns the IDs of every chunk currently in Active
// state in the supplied FSM list. Test helper, mirrors the canonical
// fan-out invariant (single-Active).
func activeChunkIDs(entries []vaultctlfsm.ManifestEntry) []chunk.ChunkID {
	var out []chunk.ChunkID
	for _, e := range entries {
		if e.State == chunk.ChunkStateActive {
			out = append(out, e.ID)
		}
	}
	return out
}

// receivingFromPlacement collects unique node IDs from a placement
// list — small helper used by tests + the orchestrator wiring path.
func receivingFromPlacement(nodeIDs []string) []string {
	out := make([]string, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		if n != "" && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

// wireRotationCoordinator builds a rotationCoordinator for the given
// vault and injects it into the chunk manager via the optional
// chunk.RotationCoordinatorSetter interface. Called from
// buildVaultInstance immediately after applyFanOutConfig.
//
// Skipped silently when:
//   - The chunk manager doesn't implement RotationCoordinatorSetter
//     (memory / jsonl managers — no cross-replica state to align).
//   - No vault-ctl Raft group exists for this vault (single-node
//     setups, or test harnesses without a GroupManager).
//
// The coordinator captures references to the FSM + applier at build
// time. Subsequent placement edits should call SetReceiving on the
// coordinator (looked up via the chunk manager's getter — Phase 6 of
// gastrolog-3yre7) to keep the snapshot fresh.
func (o *Orchestrator) wireRotationCoordinator(cm chunk.ChunkManager, vaultID glid.GLID, placements []system.VaultPlacement, nscs []system.NodeStorageConfig, factories Factories) {
	setter, ok := cm.(chunk.RotationCoordinatorSetter)
	if !ok {
		return
	}
	if factories.GroupManager == nil {
		return
	}
	groupID := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := factories.GroupManager.GetGroup(groupID)
	if g == nil {
		return
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return
	}
	fsm := vfsm.EnsureVaultFSM(vaultID)
	if fsm == nil {
		return
	}

	applier := vaultctlfsm.Applier(&vaultCtlApplier{o: o, vaultID: vaultID})

	receiving := make([]string, 0, len(placements))
	for _, sid := range system.StorageIDs(placements) {
		nid := system.NodeIDForStorage(sid, nscs)
		if nid != "" && !slices.Contains(receiving, nid) {
			receiving = append(receiving, nid)
		}
	}

	coord := newRotationCoordinator(vaultID, applier, fsm, o.now, receiving)
	setter.SetRotationCoordinator(coord)
}

// wireUploadGate injects a cloud-upload gate that returns true iff
// this node is the current vault-ctl Raft leader for the given vault.
// Under the fan-out data plane every Receiver runs PostSealProcess
// locally but only the Raft leader pushes the sealed GLCB to the
// shared blob store; the other Receivers adopt the blob when
// CmdUploadChunk propagates via OnUpload's RegisterCloudChunk
// projection. See gastrolog-4t3rs.
//
// Skipped silently when the chunk manager doesn't implement
// UploadGateSetter (memory / jsonl managers — no cloud upload),
// when no vault-ctl Raft group exists for this vault (single-node
// setups), or when the orchestrator has no GroupManager factory
// (test harnesses).
func (o *Orchestrator) wireUploadGate(cm chunk.ChunkManager, vaultID glid.GLID, factories Factories) {
	setter, ok := cm.(chunk.UploadGateSetter)
	if !ok {
		return
	}
	if factories.GroupManager == nil {
		return
	}
	groupID := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := factories.GroupManager.GetGroup(groupID)
	if g == nil {
		return
	}
	r := g.Raft
	if r == nil {
		return
	}
	gate := func() bool {
		return r.State() == hraft.Leader
	}
	setter.SetUploadGate(gate)
}
