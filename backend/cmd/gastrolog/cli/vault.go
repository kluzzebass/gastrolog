package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
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
		newVaultPauseCmd(),
		newVaultResumeCmd(),
	)
	return cmd
}

func newVaultListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all vaults",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Vaults)
			}
			var rows [][]string
			for _, v := range resp.Msg.Vaults {
				rows = append(rows, []string{
					v.Id, v.Name, v.Type,
					strconv.FormatBool(v.Enabled),
					v.Policy, v.NodeId,
				})
			}
			p.table([]string{"ID", "NAME", "TYPE", "ENABLED", "POLICY", "NODE"}, rows)
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
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
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
					p.kv(vaultDetailPairs(v))
					return nil
				}
			}
			return fmt.Errorf("vault %q not found", args[0])
		},
	}
}

// vaultDetailPairs builds the key-value pairs for vault detail rendering.
func vaultDetailPairs(v *v1.VaultConfig) [][2]string {
	pairs := [][2]string{
		{"ID", v.Id},
		{"Name", v.Name},
		{"Type", v.Type},
		{"Enabled", strconv.FormatBool(v.Enabled)},
		{"Policy", v.Policy},
		{"Node", v.NodeId},
	}
	for k, pv := range v.Params {
		pairs = append(pairs, [2]string{"Param: " + k, pv})
	}
	for i, rr := range v.RetentionRules {
		prefix := "Retention[" + strconv.Itoa(i) + "]"
		pairs = append(pairs, [2]string{prefix + " Policy", rr.RetentionPolicyId})
		pairs = append(pairs, [2]string{prefix + " Action", rr.Action})
		if rr.DestinationId != "" {
			pairs = append(pairs, [2]string{prefix + " Destination", rr.DestinationId})
		}
	}
	return pairs
}

func newVaultCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a vault",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			vaultType, _ := cmd.Flags().GetString("type")
			filterName, _ := cmd.Flags().GetString("filter")
			policyName, _ := cmd.Flags().GetString("policy")
			params, _ := cmd.Flags().GetStringSlice("param")
			enabled, _ := cmd.Flags().GetBool("enabled")
			nodeID, _ := cmd.Flags().GetString("node-id")
			retentionSpecs, _ := cmd.Flags().GetStringSlice("retention")

			client := clientFromCmd(cmd)

			_, policyID, rules, err := resolveVaultRefs(client, filterName, policyName, retentionSpecs)
			if err != nil {
				return err
			}

			id := uuid.Must(uuid.NewV7()).String()
			_, err = client.Config.PutVault(context.Background(), connect.NewRequest(&v1.PutVaultRequest{
				Config: &v1.VaultConfig{
					Id:             id,
					Name:           name,
					Type:           vaultType,
					Policy:         policyID,
					Params:         parseParams(params),
					Enabled:        enabled,
					NodeId:         nodeID,
					RetentionRules: rules,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created vault %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "vault name (required)")
	cmd.Flags().String("type", "", "vault type: file or memory (required)")
	cmd.Flags().String("filter", "", "filter name or ID")
	cmd.Flags().String("policy", "", "rotation policy name or ID")
	cmd.Flags().StringSlice("param", nil, "key=value parameter (repeatable)")
	cmd.Flags().Bool("enabled", true, "enable the vault")
	cmd.Flags().String("node-id", "", "node ID to assign")
	cmd.Flags().StringSlice("retention", nil, "retention rule: policy:action[:destination] (repeatable)")
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("type")
	return cmd
}

// resolveVaultRefs resolves filter, policy, and retention rule names to IDs.
func resolveVaultRefs(client *server.Client, filterName, policyName string, retentionSpecs []string) (filterID, policyID string, rules []*v1.RetentionRule, err error) {
	needsResolver := filterName != "" || policyName != "" || len(retentionSpecs) > 0
	if !needsResolver {
		return "", "", nil, nil
	}

	r, err := newResolver(context.Background(), client)
	if err != nil {
		return "", "", nil, err
	}

	if filterName != "" {
		filterID, err = resolve(filterName, r.filters, "filter")
		if err != nil {
			return "", "", nil, err
		}
	}
	if policyName != "" {
		policyID, err = resolve(policyName, r.rotationPolicies, "rotation policy")
		if err != nil {
			return "", "", nil, err
		}
	}
	for _, spec := range retentionSpecs {
		rule, err := parseRetentionRule(spec, r)
		if err != nil {
			return "", "", nil, err
		}
		rules = append(rules, rule)
	}
	return filterID, policyID, rules, nil
}

// parseRetentionRule parses "policy:action[:destination]" into a RetentionRule.
func parseRetentionRule(spec string, r *resolver) (*v1.RetentionRule, error) {
	parts := strings.SplitN(spec, ":", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid retention rule %q: expected policy:action[:destination]", spec)
	}
	policyID, err := resolve(parts[0], r.retentionPolicies, "retention policy")
	if err != nil {
		return nil, err
	}
	rule := &v1.RetentionRule{
		RetentionPolicyId: policyID,
		Action:            parts[1],
	}
	if len(parts) == 3 {
		destID, err := resolve(parts[2], r.vaults, "vault")
		if err != nil {
			return nil, err
		}
		rule.DestinationId = destID
	}
	return rule, nil
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
			_, err = client.Config.DeleteVault(context.Background(), connect.NewRequest(&v1.DeleteVaultRequest{
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

func newVaultPauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pause <name-or-id>",
		Short: "Pause a vault",
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
			_, err = client.Config.PauseVault(context.Background(), connect.NewRequest(&v1.PauseVaultRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Paused vault %s\n", args[0])
			return nil
		},
	}
}

func newVaultResumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <name-or-id>",
		Short: "Resume a vault",
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
			_, err = client.Config.ResumeVault(context.Background(), connect.NewRequest(&v1.ResumeVaultRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Resumed vault %s\n", args[0])
			return nil
		},
	}
}
