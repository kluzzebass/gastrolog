package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage node configurations",
	}
	cmd.AddCommand(
		newNodeListCmd(),
		newNodeGetCmd(),
		newNodeRenameCmd(),
	)
	return cmd
}

func newNodeListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.NodeConfigs)
			}
			var rows [][]string
			for _, n := range resp.Msg.NodeConfigs {
				rows = append(rows, []string{n.Id, n.Name})
			}
			p.table([]string{"ID", "NAME"}, rows)
			return nil
		},
	}
}

func newNodeGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get node details",
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
			id, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}
			for _, n := range resp.Msg.NodeConfigs {
				if n.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(n)
					}
					p.kv([][2]string{
						{"ID", n.Id},
						{"Name", n.Name},
					})
					return nil
				}
			}
			return fmt.Errorf("node %q not found", args[0])
		},
	}
}

func newNodeRenameCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename <name-or-id> <new-name>",
		Short: "Rename a node",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}
			_, err = client.Config.PutNodeConfig(context.Background(), connect.NewRequest(&v1.PutNodeConfigRequest{
				Config: &v1.NodeConfig{
					Id:   id,
					Name: args[1],
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Renamed node %s to %q\n", args[0], args[1])
			return nil
		},
	}
	return cmd
}
