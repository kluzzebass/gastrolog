package cli

import (
	"context"
	"encoding/hex"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// NewValidateCommand returns the top-level "validate" command.
func NewValidateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <vault-name-or-id>",
		Short: "Check a vault's chunks, indexes and cloud objects for damage",
		Long: "Validate a vault across every node that homes it.\n\n" +
			"Reads each chunk end to end and compares the record count against its\n" +
			"metadata. For a cloud-backed vault, also compares the objects the blob\n" +
			"store actually holds against what the cluster expects, which is the only\n" +
			"way an object deleted outside GastroLog is ever noticed.\n\n" +
			"Read-only: reports what it finds and changes nothing.",
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
			resp, err := client.Vault.ValidateVault(context.Background(),
				connect.NewRequest(&v1.ValidateVaultRequest{Vault: id}))
			if err != nil {
				return err
			}
			printValidation(args[0], resp.Msg)
			return nil
		},
	}
}

func printValidation(vaultName string, msg *v1.ValidateVaultResponse) {
	if msg.GetValid() {
		fmt.Printf("Vault %s: no damage found (%d chunk(s) checked)\n", vaultName, len(msg.GetChunks()))
	} else {
		fmt.Printf("Vault %s: DAMAGE FOUND\n", vaultName)
	}

	for _, cv := range msg.GetChunks() {
		if cv.GetValid() {
			continue
		}
		fmt.Printf("  chunk %s on %s:\n", shortChunkID(cv.GetChunkId()), nodeLabelOrLocal(cv.GetNodeId()))
		for _, issue := range cv.GetIssues() {
			fmt.Printf("    %s\n", issue)
		}
	}

	for _, a := range msg.GetCloudIndexAudits() {
		printCloudIndexAudit(a)
	}

	// A partial fan-out has to be visible: "no damage found" over a subset of
	// nodes is the one output an operator must never read as an all-clear.
	for _, d := range msg.GetContributionReport().GetDegraded() {
		fmt.Printf("  ! node %s did not answer (%s) — this report is partial\n",
			d.GetNodeId(), d.GetReason())
	}
}

func printCloudIndexAudit(a *v1.CloudIndexAudit) {
	fmt.Printf("  cloud objects on %s: %d expected / %d in store / %d indexed",
		nodeLabelOrLocal(a.GetNodeId()), a.GetExpectedChunks(), a.GetStoreObjects(), a.GetIndexEntries())
	if a.GetArchivedObjects() > 0 {
		fmt.Printf(" (%d archived)", a.GetArchivedObjects())
	}
	fmt.Println()

	reportIDs("bytes MISSING from the store (data loss)", a.GetMissingBlobs())
	for _, m := range a.GetSizeMismatches() {
		fmt.Printf("    size mismatch %s: cluster recorded %d bytes, store holds %d\n",
			shortChunkID(m.GetChunkId()), m.GetExpectedBytes(), m.GetStoreBytes())
	}
	reportIDs("objects no chunk claims (paid for, never read)", a.GetUntrackedBlobs())
	reportIDs("objects for deleted chunks (delete may still be in flight)", a.GetTombstonedBlobs())
	reportIDs("stale cloud-index entries (repair with: gastrolog reconcile)", a.GetStaleIndexEntries())
	reportIDs("objects missing from the cloud index (repair with: gastrolog reconcile)", a.GetUnindexedBlobs())
}

func reportIDs(label string, ids [][]byte) {
	if len(ids) == 0 {
		return
	}
	fmt.Printf("    %d %s:\n", len(ids), label)
	for _, id := range ids {
		fmt.Printf("      %s\n", shortChunkID(id))
	}
}

// nodeLabelOrLocal names the node a finding came from. An empty ID means the
// node serving the request answered for itself.
func nodeLabelOrLocal(nodeID string) string {
	if nodeID == "" {
		return "this node"
	}
	return nodeID
}

func shortChunkID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return glid.FromBytes(b).String()
}
