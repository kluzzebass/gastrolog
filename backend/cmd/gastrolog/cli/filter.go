package cli

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newFilterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "filter",
		Short: "Manage filters",
	}
	cmd.AddCommand(
		newFilterListCmd(),
		newFilterGetCmd(),
		newFilterCreateCmd(),
		newFilterDeleteCmd(),
	)
	return cmd
}

func newFilterListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all filters",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Filters)
			}
			var rows [][]string
			for _, f := range resp.Msg.Filters {
				rows = append(rows, []string{f.Id, f.Name, f.Expression})
			}
			p.table([]string{"ID", "NAME", "EXPRESSION"}, rows)
			return nil
		},
	}
}

func newFilterGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get filter details",
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
			id, err := resolve(args[0], r.filters, "filter")
			if err != nil {
				return err
			}
			for _, f := range resp.Msg.Filters {
				if f.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(f)
					}
					p.kv([][2]string{
						{"ID", f.Id},
						{"Name", f.Name},
						{"Expression", f.Expression},
					})
					return nil
				}
			}
			return fmt.Errorf("filter %q not found", args[0])
		},
	}
}

func newFilterCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a filter",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			expr, _ := cmd.Flags().GetString("expression")
			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()
			_, err := client.Config.PutFilter(context.Background(), connect.NewRequest(&v1.PutFilterRequest{
				Config: &v1.FilterConfig{
					Id:         id,
					Name:       name,
					Expression: expr,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created filter %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "filter name (required)")
	cmd.Flags().String("expression", "*", "filter expression")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newFilterDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a filter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.filters, "filter")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteFilter(context.Background(), connect.NewRequest(&v1.DeleteFilterRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted filter %s\n", args[0])
			return nil
		},
	}
}
