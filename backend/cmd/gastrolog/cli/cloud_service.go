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
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
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
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
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
					p.kv(pairs)
					return nil
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
			resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
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

			_, err = client.Config.PutCloudService(ctx, connect.NewRequest(&v1.PutCloudServiceRequest{
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
			_, err = client.Config.DeleteCloudService(context.Background(), connect.NewRequest(&v1.DeleteCloudServiceRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted cloud service %s\n", args[0])
			return nil
		},
	}
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
}
