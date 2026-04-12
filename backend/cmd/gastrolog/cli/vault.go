package cli

import (
	"context"
	"fmt"
	"strconv"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newVaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vault",
		Short: "Manage vaults",
	}
	cmd.AddCommand(
		newVaultListCmd(),
		newVaultGetCmd(),
		newVaultCreateCmd(),
		newVaultDeleteCmd(),
	)
	return cmd
}

func newVaultListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all vaults",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Vaults)
			}
			// Count tiers per vault from the tier configs.
			tierCount := make(map[string]int)
			for _, t := range resp.Msg.Tiers {
				if t.VaultId != "" {
					tierCount[t.VaultId]++
				}
			}
			var rows [][]string
			for _, v := range resp.Msg.Vaults {
				rows = append(rows, []string{
					v.Id, v.Name,
					strconv.FormatBool(v.Enabled),
					strconv.Itoa(tierCount[v.Id]),
				})
			}
			p.table([]string{"ID", "NAME", "ENABLED", "TIERS"}, rows)
			return nil
		},
	}
}

func newVaultGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get vault details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			for _, v := range resp.Msg.Vaults {
				if v.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(v)
					}
					p.kv(vaultDetailPairs(v, resp.Msg.Tiers))
					return nil
				}
			}
			return fmt.Errorf("vault %q not found", args[0])
		},
	}
}

// vaultDetailPairs builds the key-value pairs for vault detail rendering.
func vaultDetailPairs(v *v1.VaultConfig, allTiers []*v1.TierConfig) [][2]string {
	pairs := [][2]string{
		{"ID", v.Id},
		{"Name", v.Name},
		{"Enabled", strconv.FormatBool(v.Enabled)},
	}
	var idx int
	for _, t := range allTiers {
		if t.VaultId == v.Id {
			pairs = append(pairs, [2]string{"Tier[" + strconv.Itoa(idx) + "]", t.Id})
			idx++
		}
	}
	return pairs
}

func newVaultCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create or update a vault",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.VaultConfig{
				Id:      uuid.Must(uuid.NewV7()).String(),
				Name:    name,
				Enabled: true,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, v := range resp.Msg.Vaults {
				if v.Name == name {
					cfg = v
					verb = "Updated"
					break
				}
			}

			if cmd.Flags().Changed("enabled") {
				cfg.Enabled, _ = cmd.Flags().GetBool("enabled")
			}

			_, err = client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s vault %q (%s)\n", verb, name, cfg.Id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "vault name (required)")
	cmd.Flags().Bool("enabled", true, "enable the vault")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newVaultDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a vault",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			force, _ := cmd.Flags().GetBool("force")
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteVault(context.Background(), connect.NewRequest(&v1.DeleteVaultRequest{
				Id:    id,
				Force: force,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted vault %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().Bool("force", false, "force delete even if vault has data")
	return cmd
}

