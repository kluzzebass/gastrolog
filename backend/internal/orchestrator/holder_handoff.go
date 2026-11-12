package orchestrator

import (
	"context"
	"slices"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ChunkHolderLoss names a vault whose sealed chunks would fall below their
// holder quorum if a node and its copies left the cluster.
type ChunkHolderLoss struct {
	VaultID   glid.GLID
	VaultName string
	Chunks    int
}

// ChunkHolderLossWithout reports, per vault, the sealed chunks whose local
// copies are their durable copies (not cloud-backed, not being deleted) that
// nodeID holds and whose remaining placement holds fewer than HolderQuorum
// copies of. Removing nodeID would take a copy those chunks still depend on:
// for a former home it can be the only one. A vault with no placement left
// besides nodeID is not reported here; that is an orphaned vault.
func (o *Orchestrator) ChunkHolderLossWithout(ctx context.Context, nodeID string) []ChunkHolderLoss {
	sys, err := o.loadSystem(ctx)
	if err != nil || sys == nil {
		return nil
	}
	var out []ChunkHolderLoss
	for i := range sys.Config.Vaults {
		v := &sys.Config.Vaults[i]
		fsm, _, _, ok := o.vaultCtlHandle(v.ID)
		if !ok || fsm == nil {
			continue
		}
		remaining := slices.DeleteFunc(o.vaultPlacementNodeIDs(v.ID), func(n string) bool { return n == nodeID })
		quorum := vaultctlfsm.HolderQuorum(remaining)
		if quorum == 0 {
			continue
		}
		n := 0
		for _, e := range fsm.List() {
			if e.CloudBacked || !e.IsSealed() || e.RetentionPending || !slices.Contains(e.Holders, nodeID) {
				continue
			}
			if vaultctlfsm.PlacedHolderCount(e.Holders, remaining) < quorum {
				n++
			}
		}
		if n > 0 {
			out = append(out, ChunkHolderLoss{VaultID: v.ID, VaultName: v.Name, Chunks: n})
		}
	}
	return out
}
