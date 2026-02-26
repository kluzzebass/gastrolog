package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

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
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
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
					rp.Id, rp.Name,
					formatInt64(rp.MaxAgeSeconds),
					formatInt64(rp.MaxBytes),
					formatInt64(rp.MaxChunks),
				})
			}
			p.table([]string{"ID", "NAME", "MAX AGE (s)", "MAX BYTES", "MAX CHUNKS"}, rows)
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
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
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
				if rp.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(rp)
					}
					p.kv([][2]string{
						{"ID", rp.Id},
						{"Name", rp.Name},
						{"Max Age (s)", formatInt64(rp.MaxAgeSeconds)},
						{"Max Bytes", formatInt64(rp.MaxBytes)},
						{"Max Chunks", formatInt64(rp.MaxChunks)},
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
		Short: "Create a retention policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			maxAge, _ := cmd.Flags().GetInt64("max-age")
			maxBytes, _ := cmd.Flags().GetInt64("max-bytes")
			maxChunks, _ := cmd.Flags().GetInt64("max-chunks")

			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()
			_, err := client.Config.PutRetentionPolicy(context.Background(), connect.NewRequest(&v1.PutRetentionPolicyRequest{
				Config: &v1.RetentionPolicyConfig{
					Id:            id,
					Name:          name,
					MaxAgeSeconds: maxAge,
					MaxBytes:      maxBytes,
					MaxChunks:     maxChunks,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created retention policy %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "policy name (required)")
	cmd.Flags().Int64("max-age", 0, "max age in seconds")
	cmd.Flags().Int64("max-bytes", 0, "max bytes")
	cmd.Flags().Int64("max-chunks", 0, "max chunks")
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
			id, err := resolve(args[0], r.retentionPolicies, "retention policy")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteRetentionPolicy(context.Background(), connect.NewRequest(&v1.DeleteRetentionPolicyRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted retention policy %s\n", args[0])
			return nil
		},
	}
}
