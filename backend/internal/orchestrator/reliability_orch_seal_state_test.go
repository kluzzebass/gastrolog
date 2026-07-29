package orchestrator_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
)

// chunkStatesOnNode returns the lifecycle state of every manifest entry in the
// default vault's vault-ctl FSM on a node. Complements chunkIDsOnNode, which
// only reports presence — an entry stranded in Sealing is present but never
// promoted, so presence alone cannot catch a half-finished seal.
func (h *orchRelHarness) chunkStatesOnNode(id string) map[chunk.ChunkID]chunk.ChunkState {
	n := h.nodes[id]
	if n == nil || n.groupMgr == nil {
		return nil
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(h.vaultID))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	sub := vfsm.VaultFSM(h.vaultID)
	if sub == nil {
		return map[chunk.ChunkID]chunk.ChunkState{}
	}
	entries := sub.List()
	out := make(map[chunk.ChunkID]chunk.ChunkState, len(entries))
	for _, e := range entries {
		out[e.ID] = e.State
	}
	return out
}

// Sealing the chunk manager's active chunk must drive the vault-ctl manifest
// entry all the way to Sealed on every voter, not park it in Sealing.
//
// Seal() announces only Active → Sealing; the matching CmdSealChunk rides the
// post-seal pipeline. When that pipeline is skipped the entry never advances,
// and the chunk is neither writable nor durable-complete: grounded reads and
// retention both see a half-sealed entry forever. The single-node reproduction
// lives in TestSealActivePromotesFSMEntryToSealed; this pins the cluster-visible
// half, where the stranded state replicates to every node.
func TestOrchRel_SealActive_PromotesEveryNodeToSealed(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 20
	now := time.Now()
	for i := range records {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("seal-state-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)

	h.waitProgress("manifest entries reaching Sealed", 50*time.Millisecond, func() (string, bool) {
		var views []string
		allSealed := true
		for _, id := range h.nodeIDs {
			states := h.chunkStatesOnNode(id)
			sealed := 0
			for _, st := range states {
				if st == chunk.ChunkStateSealed {
					sealed++
				}
			}
			if len(states) == 0 || sealed != len(states) {
				allSealed = false
			}
			views = append(views, fmt.Sprintf("%s=%d/%d", h.nodes[id].label, sealed, len(states)))
		}
		return fmt.Sprintf("%v", views), allSealed
	}, func() {
		for _, id := range h.nodeIDs {
			for cid, st := range h.chunkStatesOnNode(id) {
				h.t.Logf("%s: chunk %s state=%s", h.nodes[id].label, cid, st)
			}
			h.logPostSealJobs(id)
		}
	})
}

// logPostSealJobs reports what the node's scheduler thinks of the post-seal
// pipeline. An entry stranded in Sealing has exactly three explanations, and
// this distinguishes them: the job is absent (it ran and the Sealed announce
// did not land, or it was never scheduled), pending (scheduled but starved
// behind the concurrency limit), or running (in-flight and slow). Without this
// the stall dump shows only the symptom, which is what made this member of
// gastrolog-231ik unfalsifiable across runs.
func (h *orchRelHarness) logPostSealJobs(id string) {
	n := h.nodes[id]
	if n == nil || n.orch == nil {
		return
	}
	found := false
	for _, j := range n.orch.Scheduler().ListJobs() {
		if !strings.HasPrefix(j.Name, "post-seal:") {
			continue
		}
		found = true
		status := "no progress record"
		if snap := j.Snapshot(); snap.Progress != nil {
			status = fmt.Sprintf("status=%s started=%s", snap.Progress.Status, snap.Progress.StartedAt)
		}
		h.t.Logf("%s: job %s lastRun=%s %s", n.label, j.Name, j.LastRun, status)
	}
	if !found {
		h.t.Logf("%s: no post-seal job in the scheduler registry", n.label)
	}
}
