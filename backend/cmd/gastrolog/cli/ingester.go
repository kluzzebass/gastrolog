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

func newIngesterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingester",
		Short: "Manage ingesters",
	}
	cmd.AddCommand(
		newIngesterListCmd(),
		newIngesterGetCmd(),
		newIngesterCreateCmd(),
		newIngesterDeleteCmd(),
		newIngesterTestCmd(),
	)
	return cmd
}

func newIngesterListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all ingesters",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Ingesters)
			}
			var rows [][]string
			for _, ig := range resp.Msg.Ingesters {
				rows = append(rows, []string{
					ig.Id, ig.Name, ig.Type,
					strconv.FormatBool(ig.Enabled),
					ig.NodeId,
				})
			}
			p.table([]string{"ID", "NAME", "TYPE", "ENABLED", "NODE"}, rows)
			return nil
		},
	}
}

func newIngesterGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get ingester details",
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
			id, err := resolve(args[0], r.ingesters, "ingester")
			if err != nil {
				return err
			}
			for _, ig := range resp.Msg.Ingesters {
				if ig.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(ig)
					}
					pairs := [][2]string{
						{"ID", ig.Id},
						{"Name", ig.Name},
						{"Type", ig.Type},
						{"Enabled", strconv.FormatBool(ig.Enabled)},
						{"Node", ig.NodeId},
					}
					for k, v := range ig.Params {
						pairs = append(pairs, [2]string{"Param: " + k, v})
					}
					p.kv(pairs)
					return nil
				}
			}
			return fmt.Errorf("ingester %q not found", args[0])
		},
	}
}

func newIngesterCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create an ingester",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			ingType, _ := cmd.Flags().GetString("type")
			params, _ := cmd.Flags().GetStringSlice("param")
			enabled, _ := cmd.Flags().GetBool("enabled")
			nodeID, _ := cmd.Flags().GetString("node-id")

			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()
			_, err := client.Config.PutIngester(context.Background(), connect.NewRequest(&v1.PutIngesterRequest{
				Config: &v1.IngesterConfig{
					Id:      id,
					Name:    name,
					Type:    ingType,
					Params:  parseParams(params),
					Enabled: enabled,
					NodeId:  nodeID,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created ingester %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "ingester name (required)")
	cmd.Flags().String("type", "", "ingester type (required)")
	cmd.Flags().StringSlice("param", nil, "key=value parameter (repeatable)")
	cmd.Flags().Bool("enabled", true, "enable the ingester")
	cmd.Flags().String("node-id", "", "node ID to assign")
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("type")
	return cmd
}

func newIngesterDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete an ingester",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.ingesters, "ingester")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteIngester(context.Background(), connect.NewRequest(&v1.DeleteIngesterRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted ingester %s\n", args[0])
			return nil
		},
	}
}

func newIngesterTestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test ingester connectivity",
		RunE: func(cmd *cobra.Command, args []string) error {
			ingType, _ := cmd.Flags().GetString("type")
			params, _ := cmd.Flags().GetStringSlice("param")

			client := clientFromCmd(cmd)
			resp, err := client.Config.TestIngester(context.Background(), connect.NewRequest(&v1.TestIngesterRequest{
				Type:   ingType,
				Params: parseParams(params),
			}))
			if err != nil {
				return err
			}

			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg)
			}
			if resp.Msg.Success {
				fmt.Println("Test passed:", resp.Msg.Message)
			} else {
				fmt.Println("Test failed:", resp.Msg.Message)
			}
			return nil
		},
	}
	cmd.Flags().String("type", "", "ingester type (required)")
	cmd.Flags().StringSlice("param", nil, "key=value parameter (repeatable)")
	_ = cmd.MarkFlagRequired("type")
	return cmd
}
