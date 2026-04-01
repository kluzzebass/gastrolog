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
		Short: "Create or update a retention policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.RetentionPolicyConfig{
				Id:   uuid.Must(uuid.NewV7()).String(),
				Name: name,
			}
			verb := "Created"
			resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
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
				maxAgeStr, _ := cmd.Flags().GetString("max-age")
				if maxAgeStr != "" {
					cfg.MaxAgeSeconds = parseDurationSeconds(maxAgeStr)
				} else {
					cfg.MaxAgeSeconds = 0
				}
			}
			if cmd.Flags().Changed("max-bytes") {
				cfg.MaxBytes, _ = cmd.Flags().GetInt64("max-bytes")
			}
			if cmd.Flags().Changed("max-chunks") {
				cfg.MaxChunks, _ = cmd.Flags().GetInt64("max-chunks")
			}

			_, err = client.Config.PutRetentionPolicy(ctx, connect.NewRequest(&v1.PutRetentionPolicyRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s retention policy %q (%s)\n", verb, name, cfg.Id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "policy name (required)")
	cmd.Flags().String("max-age", "", "max age (e.g. 3m, 1h, 30s)")
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
