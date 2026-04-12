package cli

import (
	"context"
	"fmt"
	"strconv"
	"time"

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
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
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
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
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
		Short: "Create or update a rotation policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.RotationPolicyConfig{
				Id:   uuid.Must(uuid.NewV7()).String(),
				Name: name,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, rp := range resp.Msg.RotationPolicies {
				if rp.Name == name {
					cfg = rp
					verb = "Updated"
					break
				}
			}

			if cmd.Flags().Changed("max-bytes") {
				cfg.MaxBytes, _ = cmd.Flags().GetInt64("max-bytes")
			}
			if cmd.Flags().Changed("max-age") {
				maxAgeStr, _ := cmd.Flags().GetString("max-age")
				if maxAgeStr != "" {
					cfg.MaxAgeSeconds = parseDurationSeconds(maxAgeStr)
				} else {
					cfg.MaxAgeSeconds = 0
				}
			}
			if cmd.Flags().Changed("max-records") {
				cfg.MaxRecords, _ = cmd.Flags().GetInt64("max-records")
			}
			if cmd.Flags().Changed("cron") {
				cfg.Cron, _ = cmd.Flags().GetString("cron")
			}

			_, err = client.System.PutRotationPolicy(ctx, connect.NewRequest(&v1.PutRotationPolicyRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s rotation policy %q (%s)\n", verb, name, cfg.Id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "policy name (required)")
	cmd.Flags().Int64("max-bytes", 0, "max bytes before rotation")
	cmd.Flags().String("max-age", "", "max age before rotation (e.g. 1m, 30s, 2h)")
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
			_, err = client.System.DeleteRotationPolicy(context.Background(), connect.NewRequest(&v1.DeleteRotationPolicyRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted rotation policy %s\n", args[0])
			return nil
		},
	}
}

// parseDurationSeconds parses a duration string (e.g. "1m", "30s", "2h") or
// a plain integer (seconds) and returns the value in seconds.
func parseDurationSeconds(s string) int64 {
	if d, err := time.ParseDuration(s); err == nil {
		return int64(d.Seconds())
	}
	// Fall back to plain integer (seconds).
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	return 0
}

func formatInt64(v int64) string {
	if v == 0 {
		return "-"
	}
	return strconv.FormatInt(v, 10)
}
