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

func newCloudServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-service",
		Short: "Manage cloud storage services",
	}
	cmd.AddCommand(
		newCloudServiceListCmd(),
		newCloudServiceGetCmd(),
		newCloudServiceCreateCmd(),
		newCloudServiceDeleteCmd(),
	)
	return cmd
}

func newCloudServiceListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all cloud services",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.CloudServices)
			}
			var rows [][]string
			for _, cs := range resp.Msg.CloudServices {
				rows = append(rows, []string{
					cs.Id, cs.Name, cs.Provider, cs.Bucket, cs.Region, cs.Endpoint,
				})
			}
			p.table([]string{"ID", "NAME", "PROVIDER", "BUCKET", "REGION", "ENDPOINT"}, rows)
			return nil
		},
	}
}

func newCloudServiceGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get cloud service details",
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
			id, err := resolve(args[0], r.cloudServices, "cloud service")
			if err != nil {
				return err
			}
			for _, cs := range resp.Msg.CloudServices {
				if cs.Id == id {
					return printCloudService(cmd, cs)
				}
			}
			return fmt.Errorf("cloud service %q not found", args[0])
		},
	}
}

func newCloudServiceCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create or update a cloud service",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.CloudService{
				Id:   uuid.Must(uuid.NewV7()).String(),
				Name: name,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, cs := range resp.Msg.CloudServices {
				if cs.Name == name {
					cfg = cs
					verb = "Updated"
					break
				}
			}

			applyCloudServiceFlags(cmd, cfg)

			_, err = client.System.PutCloudService(ctx, connect.NewRequest(&v1.PutCloudServiceRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s cloud service %q (%s)\n", verb, name, cfg.Id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "cloud service name (required)")
	cmd.Flags().String("provider", "", "provider: s3, gcs, azure")
	cmd.Flags().String("bucket", "", "bucket name (S3/GCS)")
	cmd.Flags().String("region", "", "region")
	cmd.Flags().String("endpoint", "", "endpoint URL (for S3-compatible services)")
	cmd.Flags().String("access-key", "", "access key (S3)")
	cmd.Flags().String("secret-key", "", "secret key (S3)")
	cmd.Flags().String("container", "", "container name (Azure)")
	cmd.Flags().String("connection-string", "", "connection string (Azure)")
	cmd.Flags().String("credentials-json", "", "credentials JSON (GCS)")
	cmd.Flags().Uint32("storage-class", 0, "storage class for tier placement")
	cmd.Flags().String("archival-mode", "", "storage class transition management: 'none' (external) or 'active' (managed by GastroLog)")
	cmd.Flags().StringSlice("transition", nil, "archival transition: 'AFTER:CLASS' (e.g. '90d:GLACIER', '360d:DEEP_ARCHIVE', '730d:' for delete). Repeatable.")
	cmd.Flags().String("restore-tier", "", "default restore speed (S3: Expedited/Standard/Bulk, Azure: High/Standard)")
	cmd.Flags().Uint32("restore-days", 0, "S3: how long restored copy stays readable (days)")
	cmd.Flags().Uint32("suspect-grace-days", 0, "days before suspect chunk removed from index (default 7)")
	cmd.Flags().String("reconcile-schedule", "", "cron for reconciliation sweep (default '0 3 * * *')")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newCloudServiceDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a cloud service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.cloudServices, "cloud service")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteCloudService(context.Background(), connect.NewRequest(&v1.DeleteCloudServiceRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted cloud service %s\n", args[0])
			return nil
		},
	}
}

func printCloudService(cmd *cobra.Command, cs *v1.CloudService) error {
	p := newPrinter(outputFormat(cmd))
	if outputFormat(cmd) == "json" {
		return p.json(cs)
	}
	pairs := [][2]string{
		{"ID", cs.Id},
		{"Name", cs.Name},
		{"Provider", cs.Provider},
		{"Bucket", cs.Bucket},
		{"Region", cs.Region},
		{"Endpoint", cs.Endpoint},
	}
	if cs.StorageClass > 0 {
		pairs = append(pairs, [2]string{"Storage Class", strconv.FormatUint(uint64(cs.StorageClass), 10)})
	}
	if cs.ArchivalMode != "" {
		pairs = append(pairs, [2]string{"Archival Mode", cs.ArchivalMode})
	}
	for i, tr := range cs.Transitions {
		class := tr.StorageClass
		if class == "" {
			class = "(delete)"
		}
		pairs = append(pairs, [2]string{fmt.Sprintf("Transition %d", i+1), fmt.Sprintf("after %s → %s", tr.After, class)})
	}
	if cs.RestoreTier != "" {
		pairs = append(pairs, [2]string{"Restore Tier", cs.RestoreTier})
	}
	if cs.RestoreDays > 0 {
		pairs = append(pairs, [2]string{"Restore Days", strconv.FormatUint(uint64(cs.RestoreDays), 10)})
	}
	if cs.SuspectGraceDays > 0 {
		pairs = append(pairs, [2]string{"Suspect Grace Days", strconv.FormatUint(uint64(cs.SuspectGraceDays), 10)})
	}
	if cs.ReconcileSchedule != "" {
		pairs = append(pairs, [2]string{"Reconcile Schedule", cs.ReconcileSchedule})
	}
	p.kv(pairs)
	return nil
}

// applyCloudServiceFlags overlays explicitly-set CLI flags onto the cloud service config.
func applyCloudServiceFlags(cmd *cobra.Command, cfg *v1.CloudService) {
	if cmd.Flags().Changed("provider") {
		cfg.Provider, _ = cmd.Flags().GetString("provider")
	}
	if cmd.Flags().Changed("bucket") {
		cfg.Bucket, _ = cmd.Flags().GetString("bucket")
	}
	if cmd.Flags().Changed("region") {
		cfg.Region, _ = cmd.Flags().GetString("region")
	}
	if cmd.Flags().Changed("endpoint") {
		cfg.Endpoint, _ = cmd.Flags().GetString("endpoint")
	}
	if cmd.Flags().Changed("access-key") {
		cfg.AccessKey, _ = cmd.Flags().GetString("access-key")
	}
	if cmd.Flags().Changed("secret-key") {
		cfg.SecretKey, _ = cmd.Flags().GetString("secret-key")
	}
	if cmd.Flags().Changed("container") {
		cfg.Container, _ = cmd.Flags().GetString("container")
	}
	if cmd.Flags().Changed("connection-string") {
		cfg.ConnectionString, _ = cmd.Flags().GetString("connection-string")
	}
	if cmd.Flags().Changed("credentials-json") {
		cfg.CredentialsJson, _ = cmd.Flags().GetString("credentials-json")
	}
	if cmd.Flags().Changed("storage-class") {
		sc, _ := cmd.Flags().GetUint32("storage-class")
		cfg.StorageClass = sc
	}
	if cmd.Flags().Changed("archival-mode") {
		cfg.ArchivalMode, _ = cmd.Flags().GetString("archival-mode")
	}
	if cmd.Flags().Changed("transition") {
		specs, _ := cmd.Flags().GetStringSlice("transition")
		cfg.Transitions = nil
		for _, spec := range specs {
			parts := splitTransitionSpec(spec)
			cfg.Transitions = append(cfg.Transitions, &v1.CloudStorageTransition{
				After:        parts[0],
				StorageClass: parts[1],
			})
		}
	}
	if cmd.Flags().Changed("restore-tier") {
		cfg.RestoreTier, _ = cmd.Flags().GetString("restore-tier")
	}
	if cmd.Flags().Changed("restore-days") {
		d, _ := cmd.Flags().GetUint32("restore-days")
		cfg.RestoreDays = d
	}
	if cmd.Flags().Changed("suspect-grace-days") {
		d, _ := cmd.Flags().GetUint32("suspect-grace-days")
		cfg.SuspectGraceDays = d
	}
	if cmd.Flags().Changed("reconcile-schedule") {
		cfg.ReconcileSchedule, _ = cmd.Flags().GetString("reconcile-schedule")
	}
}

// splitTransitionSpec parses "AFTER:CLASS" into [after, class].
// An empty class (e.g. "730d:") means delete.
func splitTransitionSpec(spec string) [2]string {
	for i, c := range spec {
		if c == ':' {
			return [2]string{spec[:i], spec[i+1:]}
		}
	}
	return [2]string{spec, ""}
}
