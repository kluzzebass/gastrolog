package cli

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// NewRepatriateCommand returns the "repatriate" command. Operator-driven
// recovery for unknown orphan chunks: a sealed chunk that exists on a
// node's local disk but is missing from the vault-ctl FSM manifest gets
// re-introduced into the FSM via CmdRepatriateChunk.
//
// The RPC is RouteToResourceOwner: the interceptor forwards it to the node
// that owns the vault, but orphan chunks are inherently node-local.
// Run this command against the node that holds the orphan on disk —
// usually that's also the vault owner, but if the orphan is on a
// non-owner node (e.g. post-shuffle leftover), connect directly to
// that node via --addr or --home. See gastrolog-32bf2.
func NewRepatriateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repatriate <chunk-id>",
		Short: "Re-introduce an orphan chunk into the vault-ctl FSM manifest",
		Long: "Re-introduce a sealed local chunk into the vault-ctl FSM. " +
			"Used to recover \"unknown orphans\" — sealed chunks present on " +
			"local disk but absent from the FSM manifest. The chunk must be " +
			"sealed and have at least one record.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			vaultFlag, _ := cmd.Flags().GetString("vault")
			if vaultFlag == "" {
				return errors.New("--vault is required")
			}

			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			vaultID, err := resolve(vaultFlag, r.vaults, "vault")
			if err != nil {
				return err
			}

			chunkID, parseErr := chunk.ParseChunkID(args[0])
			if parseErr != nil {
				return fmt.Errorf("invalid chunk ID: %w", parseErr)
			}
			_, err = client.Vault.RepatriateOrphan(context.Background(),
				connect.NewRequest(&v1.RepatriateOrphanRequest{
					Vault:   vaultID,
					ChunkId: glid.GLID(chunkID).ToProto(),
				}))
			if err != nil {
				return err
			}

			fmt.Printf("Repatriated orphan chunk %s into vault %s\n", args[0], vaultFlag)
			return nil
		},
	}
	cmd.Flags().String("vault", "", "vault name or ID (required)")
	return cmd
}
