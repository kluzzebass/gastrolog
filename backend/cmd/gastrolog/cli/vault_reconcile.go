package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewReconcileCommand returns the top-level "reconcile" command.
func NewReconcileCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile <vault-name-or-id>",
		Short: "Rebuild each node's cloud index from the object store",
		Long: "Rebuild the cloud index on every node that homes a cloud-backed vault.\n\n" +
			"Drops cached entries whose object is gone, resets sizes that drifted from\n" +
			"the object they describe, and indexes objects the cache never recorded.\n\n" +
			"Repairs a cache only: it never deletes an object and never changes cluster\n" +
			"state, so it cannot lose data however wrong the local view was. Run\n" +
			"`gastrolog validate` first to see what it would fix.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			resp, err := client.Vault.ReconcileCloudIndex(context.Background(),
				connect.NewRequest(&v1.ReconcileCloudIndexRequest{Vault: id}))
			if err != nil {
				return err
			}
			printReconcile(args[0], resp.Msg)
			return nil
		},
	}
}

func printReconcile(vaultName string, msg *v1.ReconcileCloudIndexResponse) {
	if len(msg.GetRepairs()) == 0 {
		fmt.Printf("Vault %s: no cloud index to rebuild (local-only vault, or not homed here)\n", vaultName)
	}
	var total int64
	for _, r := range msg.GetRepairs() {
		changed := r.GetRemovedEntries() + r.GetCorrectedSizes() + r.GetIndexedBlobs()
		total += changed
		if changed == 0 {
			fmt.Printf("  %s: already consistent\n", nodeLabelOrLocal(r.GetNodeId()))
			continue
		}
		fmt.Printf("  %s: %d stale entr(ies) dropped, %d size(s) corrected, %d object(s) indexed\n",
			nodeLabelOrLocal(r.GetNodeId()), r.GetRemovedEntries(), r.GetCorrectedSizes(), r.GetIndexedBlobs())
	}
	if len(msg.GetRepairs()) > 0 && total == 0 {
		fmt.Printf("Vault %s: every node's cloud index was already consistent\n", vaultName)
	}

	// An unreached peer still has an unrepaired index, and saying nothing here
	// would read as "all nodes done".
	for _, d := range msg.GetContributionReport().GetDegraded() {
		fmt.Printf("  ! node %s did not answer (%s) — its cloud index is still unrepaired\n",
			d.GetNodeId(), d.GetReason())
	}
}
