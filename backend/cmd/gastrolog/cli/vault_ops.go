package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewSealCommand returns the top-level "seal" command.
func NewSealCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "seal <vault-name-or-id>",
		Short: "Seal the vault's open chunks and start new ones",
		Long:  "Seal every open chunk in a vault that holds records, ahead of its rotation policy: the pipeline's open chunk manifest and, when one is present, a chunk-manager active chunk. The sealed chunks are then built, indexed and uploaded like any other. The request runs on the vault's leader from whichever node receives it; while vault-ctl leadership is moving onto that node the seal is refused as unavailable and can be retried.",
		Args:  cobra.ExactArgs(1),
		RunE:  runSeal,
	}
}

func runSeal(cmd *cobra.Command, args []string) error {
	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(args[0], r.vaults, "vault")
	if err != nil {
		return err
	}

	resp, err := client.Vault.SealVault(context.Background(), connect.NewRequest(&v1.SealVaultRequest{Vault: vaultID}))
	if err != nil {
		return err
	}
	fmt.Println(sealSummary(resp.Msg.SealedCount, args[0]))
	return nil
}

func sealSummary(sealed int32, vault string) string {
	if sealed == 0 {
		return fmt.Sprintf("Nothing to seal in vault %s: no open chunk holds records", vault)
	}
	return fmt.Sprintf("Sealed %d open chunk(s) in vault %s", sealed, vault)
}

// NewReindexCommand returns the top-level "reindex" command.
func NewReindexCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "reindex <vault-name-or-id>",
		Short: "Rebuild all indexes for sealed chunks in a vault",
		Args:  cobra.ExactArgs(1),
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
			resp, err := client.Vault.ReindexVault(context.Background(), connect.NewRequest(&v1.ReindexVaultRequest{Vault: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Reindexing vault %s (job %s)\n", args[0], resp.Msg.JobId)
			return nil
		},
	}
}

// NewPauseCommand returns the top-level "pause" command.
func NewPauseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pause <vault-name-or-id>",
		Short: "Pause ingestion for a vault",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			_, err = client.System.PauseVault(context.Background(), connect.NewRequest(&v1.PauseVaultRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Paused vault %s\n", args[0])
			return nil
		},
	}
}

// NewResumeCommand returns the top-level "resume" command.
func NewResumeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <vault-name-or-id>",
		Short: "Resume ingestion for a vault",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			_, err = client.System.ResumeVault(context.Background(), connect.NewRequest(&v1.ResumeVaultRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Resumed vault %s\n", args[0])
			return nil
		},
	}
}
