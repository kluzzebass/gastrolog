package cli

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// formatRefuse renders the tri-state refuse flag for table/kv output.
// Unset (nil) reads as true — the default — same as
// system.RetentionPolicyConfig.RefuseEnabled().
func formatRefuse(v *bool) string {
	if v == nil || *v {
		return "true"
	}
	return "false"
}

func newRetentionPolicyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "retention-policy",
		Aliases: []string{"ret"},
		Short:   "Manage retention policies",
	}
	cmd.AddCommand(
		newRetentionPolicyListCmd(),
		newRetentionPolicyGetCmd(),
		newRetentionPolicyCreateCmd(),
		newRetentionPolicyDeleteCmd(),
	)
	return cmd
}

func newRetentionPolicyListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all retention policies",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.RetentionPolicies)
			}
			var rows [][]string
			for _, rp := range resp.Msg.RetentionPolicies {
				rows = append(rows, []string{
					glid.FromBytes(rp.Id).String(), rp.Name,
					rp.MaxAge,
					rp.MaxSize,
					formatInt64(rp.MaxChunks),
					formatRefuse(rp.Refuse),
				})
			}
			p.table([]string{"ID", "NAME", "MAX AGE", "MAX SIZE", "MAX CHUNKS", "REFUSE"}, rows)
			return nil
		},
	}
}

func newRetentionPolicyGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get retention policy details",
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
			id, err := resolve(args[0], r.retentionPolicies, "retention policy")
			if err != nil {
				return err
			}
			for _, rp := range resp.Msg.RetentionPolicies {
				if glid.FromBytes(rp.Id).String() == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(rp)
					}
					p.kv([][2]string{
						{"ID", glid.FromBytes(rp.Id).String()},
						{"Name", rp.Name},
						{"Max Age", rp.MaxAge},
						{"Max Size", rp.MaxSize},
						{"Max Chunks", formatInt64(rp.MaxChunks)},
						{"Refuse", formatRefuse(rp.Refuse)},
					})
					return nil
				}
			}
			return fmt.Errorf("retention policy %q not found", args[0])
		},
	}
}

func newRetentionPolicyCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create or update a retention policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.RetentionPolicyConfig{
				Id:   glid.New().ToProto(),
				Name: name,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, rp := range resp.Msg.RetentionPolicies {
				if rp.Name == name {
					cfg = rp
					verb = "Updated"
					break
				}
			}

			if cmd.Flags().Changed("max-age") {
				cfg.MaxAge, _ = cmd.Flags().GetString("max-age")
			}
			if cmd.Flags().Changed("max-size") {
				cfg.MaxSize, _ = cmd.Flags().GetString("max-size")
			}
			if cmd.Flags().Changed("max-chunks") {
				cfg.MaxChunks, _ = cmd.Flags().GetInt64("max-chunks")
			}
			if cmd.Flags().Changed("refuse") {
				refuse, _ := cmd.Flags().GetBool("refuse")
				cfg.Refuse = &refuse
			}

			_, err = client.System.PutRetentionPolicy(ctx, connect.NewRequest(&v1.PutRetentionPolicyRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s retention policy %q (%s)\n", verb, name, glid.FromBytes(cfg.Id))
			return nil
		},
	}
	cmd.Flags().String("name", "", "policy name (required)")
	cmd.Flags().String("max-age", "", "max age (e.g. 3m, 1h, 30s)")
	cmd.Flags().String("max-size", "", "vault's disk-claim bound (e.g. \"50GB\", \"1GiB\"; empty = no bound from this policy): oldest sealed chunks drain past it, and admission refuses cluster-wide while the vault's local claim is at/over it. Min-wins across a vault's attached policies")
	cmd.Flags().Int64("max-chunks", 0, "max chunks")
	cmd.Flags().Bool("refuse", true, "while any set bound (max-age, max-size, max-chunks) is violated, refuse admission cluster-wide (a \"hard bound\"). "+
		"--no-refuse makes this a drain-only \"soft bound\": the policy still drains past its bounds, but only the node-level disk guard backstops the vault while violated. "+
		"max-size's refuse check is instantaneous; max-age/max-chunks refuse only once a retention sweep has run and failed to clear the violation")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newRetentionPolicyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a retention policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.retentionPolicies, "retention policy")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteRetentionPolicy(context.Background(), connect.NewRequest(&v1.DeleteRetentionPolicyRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted retention policy %s\n", args[0])
			return nil
		},
	}
}
