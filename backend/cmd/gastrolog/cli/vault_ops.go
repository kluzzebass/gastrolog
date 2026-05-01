package cli

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewSealCommand returns the top-level "seal" command.
// Forces rotation: seals the active chunk on the specified tier (or all tiers)
// and triggers the post-seal pipeline (compress → index → upload).
func NewSealCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "seal <vault-name-or-id>",
		Short: "Seal the active chunk and start a new one",
		Long:  "Seal the active chunk on a tier (or all tiers with --all) in a vault.\nExactly one of --tier or --all must be specified.",
		Args:  cobra.ExactArgs(1),
		RunE:  runSeal,
	}
	cmd.Flags().String("tier", "", "seal only this tier (name or ID)")
	cmd.Flags().Bool("all", false, "seal all tiers in the vault")
	return cmd
}

func runSeal(cmd *cobra.Command, args []string) error {
	tierFlag, _ := cmd.Flags().GetString("tier")
	allFlag, _ := cmd.Flags().GetBool("all")
	if tierFlag == "" && !allFlag {
		return errors.New("specify --tier <name-or-id> or --all")
	}
	if tierFlag != "" && allFlag {
		return errors.New("--tier and --all are mutually exclusive")
	}

	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(args[0], r.vaults, "vault")
	if err != nil {
		return err
	}

	req := &v1.SealVaultRequest{Vault: vaultID}
	if tierFlag != "" {
		tierID, err := resolve(tierFlag, r.tiers, "tier")
		if err != nil {
			return err
		}
		req.Tier = tierID
	}

	resp, err := client.Vault.SealVault(context.Background(), connect.NewRequest(req))
	if err != nil {
		return err
	}
	fmt.Printf("Sealed %d tier(s) in vault %s\n", resp.Msg.SealedCount, args[0])
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

