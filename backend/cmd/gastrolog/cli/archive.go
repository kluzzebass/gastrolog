package cli

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewArchiveCommand returns the "archive" command.
func NewArchiveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "archive <chunk-id>",
		Short: "Archive a cloud-backed chunk to offline storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			vaultFlag, _ := cmd.Flags().GetString("vault")
			if vaultFlag == "" {
				return errors.New("--vault is required")
			}
			storageClass, _ := cmd.Flags().GetString("class")

			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			vaultID, err := resolve(vaultFlag, r.vaults, "vault")
			if err != nil {
				return err
			}

			_, err = client.Vault.ArchiveChunk(context.Background(),
				connect.NewRequest(&v1.ArchiveChunkRequest{
					Vault:        vaultID,
					ChunkId:      args[0],
					StorageClass: storageClass,
				}))
			if err != nil {
				return err
			}

			fmt.Printf("Archived chunk %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().String("vault", "", "vault name or ID (required)")
	cmd.Flags().String("class", "", "target storage class (e.g. GLACIER, DEEP_ARCHIVE)")
	return cmd
}

// NewRestoreCommand returns the "restore" command.
func NewRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <chunk-id>",
		Short: "Restore an archived chunk to readable storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			vaultFlag, _ := cmd.Flags().GetString("vault")
			if vaultFlag == "" {
				return errors.New("--vault is required")
			}
			restoreTier, _ := cmd.Flags().GetString("tier")
			restoreDays, _ := cmd.Flags().GetInt32("days")

			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			vaultID, err := resolve(vaultFlag, r.vaults, "vault")
			if err != nil {
				return err
			}

			_, err = client.Vault.RestoreChunk(context.Background(),
				connect.NewRequest(&v1.RestoreChunkRequest{
					Vault:       vaultID,
					ChunkId:     args[0],
					RestoreTier: restoreTier,
					RestoreDays: restoreDays,
				}))
			if err != nil {
				return err
			}

			fmt.Printf("Restore initiated for chunk %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().String("vault", "", "vault name or ID (required)")
	cmd.Flags().String("tier", "", "restore speed tier: Expedited, Standard, Bulk (default: Standard)")
	cmd.Flags().Int32("days", 0, "days to keep restored copy readable (S3 only, default: provider default)")
	return cmd
}
