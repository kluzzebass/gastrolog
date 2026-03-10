package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "route",
		Aliases: []string{"routes"},
		Short:   "Manage routes",
	}
	cmd.AddCommand(
		newRouteListCmd(),
		newRouteGetCmd(),
		newRouteCreateCmd(),
		newRouteDeleteCmd(),
	)
	return cmd
}

func newRouteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all routes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Routes)
			}
			var rows [][]string
			for _, r := range resp.Msg.Routes {
				dests := make([]string, len(r.Destinations))
				for i, d := range r.Destinations {
					dests[i] = d.VaultId
				}
				rows = append(rows, []string{
					r.Id, r.Name, r.FilterId,
					strings.Join(dests, ","),
					r.Distribution,
					strconv.FormatBool(r.Enabled),
				})
			}
			p.table([]string{"ID", "NAME", "FILTER", "DESTINATIONS", "DISTRIBUTION", "ENABLED"}, rows)
			return nil
		},
	}
}

func newRouteGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get route details",
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
			id, err := resolve(args[0], r.routes, "route")
			if err != nil {
				return err
			}
			for _, rt := range resp.Msg.Routes {
				if rt.Id == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(rt)
					}
					dests := make([]string, len(rt.Destinations))
					for i, d := range rt.Destinations {
						dests[i] = d.VaultId
					}
					p.kv([][2]string{
						{"ID", rt.Id},
						{"Name", rt.Name},
						{"Filter", rt.FilterId},
						{"Destinations", strings.Join(dests, ", ")},
						{"Distribution", rt.Distribution},
						{"Enabled", strconv.FormatBool(rt.Enabled)},
					})
					return nil
				}
			}
			return fmt.Errorf("route %q not found", args[0])
		},
	}
}

func newRouteCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a route",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			filterName, _ := cmd.Flags().GetString("filter")
			destNames, _ := cmd.Flags().GetStringSlice("destination")
			dist, _ := cmd.Flags().GetString("distribution")
			enabled, _ := cmd.Flags().GetBool("enabled")

			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}

			var filterID string
			if filterName != "" {
				filterID, err = resolve(filterName, r.filters, "filter")
				if err != nil {
					return err
				}
			}

			var dests []*v1.RouteDestination
			for _, d := range destNames {
				vaultID, err := resolve(d, r.vaults, "vault")
				if err != nil {
					return err
				}
				dests = append(dests, &v1.RouteDestination{VaultId: vaultID})
			}

			id := uuid.Must(uuid.NewV7()).String()
			_, err = client.Config.PutRoute(context.Background(), connect.NewRequest(&v1.PutRouteRequest{
				Config: &v1.RouteConfig{
					Id:           id,
					Name:         name,
					FilterId:     filterID,
					Destinations: dests,
					Distribution: dist,
					Enabled:      enabled,
				},
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created route %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "route name (required)")
	cmd.Flags().String("filter", "", "filter name or ID")
	cmd.Flags().StringSlice("destination", nil, "destination vault name or ID (repeatable)")
	cmd.Flags().String("distribution", "fanout", "distribution: fanout, round-robin, or failover")
	cmd.Flags().Bool("enabled", true, "enable the route")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newRouteDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a route",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.routes, "route")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteRoute(context.Background(), connect.NewRequest(&v1.DeleteRouteRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted route %s\n", args[0])
			return nil
		},
	}
}
