package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewSealCommand returns the top-level "seal" command.
// Forces rotation: seals the active chunk for the named vault and triggers
// the post-seal pipeline (compress → index → upload).
func NewSealCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "seal <vault-name-or-id>",
		Short: "Seal the active chunk and start a new one",
		Long:  "Seal the active chunk in a vault. Triggers compress + index + (cloud) upload.",
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
	fmt.Printf("Sealed %d active chunk(s) in vault %s\n", resp.Msg.SealedCount, args[0])
	return nil
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

