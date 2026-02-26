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

func newRotationPolicyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rotation-policy",
		Aliases: []string{"rp"},
		Short:   "Manage rotation policies",
	}
	cmd.AddCommand(
		newRotationPolicyListCmd(),
		newRotationPolicyGetCmd(),
		newRotationPolicyCreateCmd(),
		newRotationPolicyDeleteCmd(),
	)
	return cmd
}

func newRotationPolicyListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all rotation policies",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.RotationPolicies)
			}
			var rows [][]string
			for _, rp := range resp.Msg.RotationPolicies {
				rows = append(rows, []string{
					rp.Id, rp.Name,
					formatInt64(rp.MaxBytes),
					formatInt64(rp.MaxAgeSeconds),
					formatInt64(rp.MaxRecords),
					rp.Cron,
				})
			}
			p.table([]string{"ID", "NAME", "MAX BYTES", "MAX AGE (s)", "MAX RECORDS", "CRON"}, rows)
			return nil
		},
	}
}

func newRotationPolicyGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get rotation policy details",
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
			id, err := resolve(args[0], r.rotationPolicies, "rotation policy")
			if err != nil {
				return err
			}
			for _, rp := range resp.Msg.RotationPolicies {
				if rp.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(rp)
					}
					p.kv([][2]string{
						{"ID", rp.Id},
						{"Name", rp.Name},
						{"Max Bytes", formatInt64(rp.MaxBytes)},
						{"Max Age (s)", formatInt64(rp.MaxAgeSeconds)},
						{"Max Records", formatInt64(rp.MaxRecords)},
						{"Cron", rp.Cron},
					})
					return nil
				}
			}
			return fmt.Errorf("rotation policy %q not found", args[0])
		},
	}
}

func newRotationPolicyCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a rotation policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			maxBytes, _ := cmd.Flags().GetInt64("max-bytes")
			maxAge, _ := cmd.Flags().GetInt64("max-age")
			maxRecords, _ := cmd.Flags().GetInt64("max-records")
			cron, _ := cmd.Flags().GetString("cron")

			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()
			_, err := client.Config.PutRotationPolicy(context.Background(), connect.NewRequest(&v1.PutRotationPolicyRequest{
				Config: &v1.RotationPolicyConfig{
					Id:            id,
					Name:          name,
					MaxBytes:      maxBytes,
					MaxAgeSeconds: maxAge,
					MaxRecords:    maxRecords,
					Cron:          cron,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created rotation policy %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "policy name (required)")
	cmd.Flags().Int64("max-bytes", 0, "max bytes before rotation")
	cmd.Flags().Int64("max-age", 0, "max age in seconds before rotation")
	cmd.Flags().Int64("max-records", 0, "max records before rotation")
	cmd.Flags().String("cron", "", "cron schedule for rotation")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newRotationPolicyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a rotation policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.rotationPolicies, "rotation policy")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteRotationPolicy(context.Background(), connect.NewRequest(&v1.DeleteRotationPolicyRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted rotation policy %s\n", args[0])
			return nil
		},
	}
}

func formatInt64(v int64) string {
	if v == 0 {
		return "-"
	}
	return strconv.FormatInt(v, 10)
}
