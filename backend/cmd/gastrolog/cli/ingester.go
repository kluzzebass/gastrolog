package cli

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/units"
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
		newIngesterStatusCmd(),
	)
	return cmd
}

func newIngesterListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all ingesters",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
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
					glid.FromBytes(ig.Id).String(), ig.Name, ig.Type,
					strconv.FormatBool(ig.Enabled),
					string(ig.NodeId),
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
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.ingesters, "ingester")
			if err != nil {
				return err
			}
			for _, ig := range resp.Msg.Ingesters {
				if glid.FromBytes(ig.Id) == glid.FromBytes(idBytes) {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(ig)
					}
					pairs := [][2]string{
						{"ID", glid.FromBytes(ig.Id).String()},
						{"Name", ig.Name},
						{"Type", ig.Type},
						{"Enabled", strconv.FormatBool(ig.Enabled)},
						{"Node", string(ig.NodeId)},
						{"Singleton", strconv.FormatBool(ig.Singleton)},
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
		Short: "Create or update an ingester",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			// Upsert: if an ingester with this name exists, start from its config.
			cfg := &v1.IngesterConfig{
				Id:      glid.New().ToProto(),
				Name:    name,
				Enabled: true, // default for new ingesters
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, ig := range resp.Msg.Ingesters {
				if ig.Name == name {
					cfg = ig
					verb = "Updated"
					break
				}
			}

			// Overlay explicitly-set flags.
			if cmd.Flags().Changed("type") {
				cfg.Type, _ = cmd.Flags().GetString("type")
			}
			if cmd.Flags().Changed("enabled") {
				cfg.Enabled, _ = cmd.Flags().GetBool("enabled")
			}
			if cmd.Flags().Changed("node-id") {
				nid, _ := cmd.Flags().GetString("node-id")
				cfg.NodeId = []byte(nid)
			}
			if cmd.Flags().Changed("param") {
				params, _ := cmd.Flags().GetStringSlice("param")
				cfg.Params = parseParams(params)
			}
			if cmd.Flags().Changed("singleton") {
				cfg.Singleton, _ = cmd.Flags().GetBool("singleton")
			}

			if cfg.Type == "" {
				return errors.New("--type is required for new ingesters")
			}

			_, err = client.System.PutIngester(ctx, connect.NewRequest(&v1.PutIngesterRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s ingester %q (%s)\n", verb, name, glid.FromBytes(cfg.Id))
			return nil
		},
	}
	cmd.Flags().String("name", "", "ingester name (required)")
	cmd.Flags().String("type", "", "ingester type")
	cmd.Flags().StringSlice("param", nil, "key=value parameter (repeatable)")
	cmd.Flags().Bool("enabled", true, "enable the ingester")
	cmd.Flags().String("node-id", "", "node ID to assign")
	cmd.Flags().Bool("singleton", false, "run on one node with Raft-coordinated failover (only meaningful for types that support it)")
	_ = cmd.MarkFlagRequired("name")
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
			idBytes, err := resolveToProto(args[0], r.ingesters, "ingester")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteIngester(context.Background(), connect.NewRequest(&v1.DeleteIngesterRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted ingester %s\n", args[0])
			return nil
		},
	}
}

func newIngesterStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <name-or-id>",
		Short: "Get ingester runtime status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.ingesters, "ingester")
			if err != nil {
				return err
			}
			resp, err := client.System.GetIngesterStatus(context.Background(), connect.NewRequest(&v1.GetIngesterStatusRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg)
			}
			p.kv([][2]string{
				{"ID", glid.FromBytes(resp.Msg.Id).String()},
				{"Type", resp.Msg.Type},
				{"Running", strconv.FormatBool(resp.Msg.Running)},
				{"Messages Ingested", strconv.FormatInt(resp.Msg.MessagesIngested, 10)},
				{"Bytes Ingested", units.FormatBytesDisplay(resp.Msg.BytesIngested)},
				{"Errors", strconv.FormatInt(resp.Msg.Errors, 10)},
			})
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
			resp, err := client.System.TestIngester(context.Background(), connect.NewRequest(&v1.TestIngesterRequest{
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
