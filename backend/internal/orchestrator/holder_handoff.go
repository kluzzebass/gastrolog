package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// HolderLoss names a vault whose records would lose a copy they still depend
// on if a node and its files left the cluster: sealed chunks and unreleased
// segments that the rest of the placement holds below their holder quorum.
type HolderLoss struct {
	VaultID   glid.GLID
	VaultName string
	Chunks    int
	Segments  int
}

// Holdings names what of the vault depends on the node, for refusal messages.
func (l HolderLoss) Holdings() string {
	var parts []string
	if l.Chunks > 0 {
		parts = append(parts, fmt.Sprintf("%d sealed chunk(s)", l.Chunks))
	}
	if l.Segments > 0 {
		parts = append(parts, fmt.Sprintf("%d unreleased segment(s)", l.Segments))
	}
	return strings.Join(parts, " and ")
}

// HolderLossWithout reports, per vault, what would lose a copy it depends on
// if nodeID and its files left the cluster (holderLossIn). For a former home
// or an origin, the node's copy can be the only one. A vault with no placement
// left besides nodeID is not reported here; that is an orphaned vault.
func (o *Orchestrator) HolderLossWithout(ctx context.Context, nodeID string) []HolderLoss {
	sys, err := o.loadSystem(ctx)
	if err != nil || sys == nil {
		return nil
	}
	var out []HolderLoss
	for i := range sys.Config.Vaults {
		v := &sys.Config.Vaults[i]
		fsm, _, _, ok := o.vaultCtlHandle(v.ID)
		if !ok || fsm == nil {
			continue
		}
		remaining := slices.DeleteFunc(o.vaultPlacementNodeIDs(v.ID), func(n string) bool { return n == nodeID })
		chunks, segments := holderLossIn(fsm, nodeID, remaining)
		if chunks > 0 || segments > 0 {
			out = append(out, HolderLoss{VaultID: v.ID, VaultName: v.Name, Chunks: chunks, Segments: segments})
		}
	}
	return out
}

// holderLossIn counts what of a vault depends on nodeID's copies, given the
// placement that remains without it:
//   - sealed chunks whose local copies are their durable copies (not
//     cloud-backed, not being deleted) that nodeID holds;
//   - unreleased segments nodeID holds or originated that the homes still
//     collect (segmentAwaitsPlacement);
//
// each held by fewer than HolderQuorum(remaining) members of remaining.
func holderLossIn(fsm *vaultctlfsm.FSM, nodeID string, remaining []string) (chunks, segments int) {
	quorum := vaultctlfsm.HolderQuorum(remaining)
	if quorum == 0 {
		return 0, 0
	}
	for _, e := range fsm.List() {
		if e.CloudBacked || !e.IsSealed() || e.RetentionPending || !slices.Contains(e.Holders, nodeID) {
			continue
		}
		if vaultctlfsm.PlacedHolderCount(e.Holders, remaining) < quorum {
			chunks++
		}
	}
	for _, seg := range fsm.ListCompletedSegments() {
		if seg.OriginNodeID != nodeID && !slices.Contains(seg.Holders, nodeID) {
			continue
		}
		if segmentAwaitsPlacement(fsm, seg, remaining) {
			segments++
		}
	}
	return chunks, segments
}

// segmentAwaitsPlacement reports whether the homes in placement still collect
// an unreleased segment (it is not superseded, the same test collection skips
// by) and hold fewer than HolderQuorum copies of it, so the copies outside
// placement are still needed.
func segmentAwaitsPlacement(fsm *vaultctlfsm.FSM, seg vaultctlfsm.CompletedSegmentEntry, placement []string) bool {
	if vaultctlfsm.PlacedHolderCount(seg.Holders, placement) >= vaultctlfsm.HolderQuorum(placement) {
		return false
	}
	return !fsm.SegmentSuperseded(seg.SegmentID, placement)
}
