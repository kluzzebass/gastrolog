package cli

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
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
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
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
					dests[i] = glid.FromBytes(d.VaultId).String()
				}
				rows = append(rows, []string{
					glid.FromBytes(r.Id).String(), r.Name, glid.FromBytes(r.FilterId).String(),
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
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
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
				if glid.FromBytes(rt.Id).String() == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(rt)
					}
					dests := make([]string, len(rt.Destinations))
					for i, d := range rt.Destinations {
						dests[i] = glid.FromBytes(d.VaultId).String()
					}
					p.kv([][2]string{
						{"ID", glid.FromBytes(rt.Id).String()},
						{"Name", rt.Name},
						{"Filter", glid.FromBytes(rt.FilterId).String()},
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
		Short: "Create or update a route",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.RouteConfig{
				Id:           glid.New().ToProto(),
				Name:         name,
				Distribution: "fanout",
				Enabled:      true,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, rt := range resp.Msg.Routes {
				if rt.Name == name {
					cfg = rt
					verb = "Updated"
					break
				}
			}

			if err := resolveRouteFilterAndDestinations(ctx, cmd, client, cfg); err != nil {
				return err
			}

			if cmd.Flags().Changed("distribution") {
				cfg.Distribution, _ = cmd.Flags().GetString("distribution")
			}
			if cmd.Flags().Changed("enabled") {
				cfg.Enabled, _ = cmd.Flags().GetBool("enabled")
			}

			_, err = client.System.PutRoute(ctx, connect.NewRequest(&v1.PutRouteRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s route %q (%s)\n", verb, name, glid.FromBytes(cfg.Id))
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

// resolveRouteFilterAndDestinations resolves the --filter and --destination
// flags, updating cfg in place. It only creates a resolver if needed.
func resolveRouteFilterAndDestinations(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.RouteConfig) error {
	needsResolver := cmd.Flags().Changed("filter") || cmd.Flags().Changed("destination")
	if !needsResolver {
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	if cmd.Flags().Changed("filter") {
		filterName, _ := cmd.Flags().GetString("filter")
		if filterName != "" {
			cfg.FilterId, err = resolveToProto(filterName, r.filters, "filter")
			if err != nil {
				return err
			}
		} else {
			cfg.FilterId = nil
		}
	}
	if cmd.Flags().Changed("destination") {
		destNames, _ := cmd.Flags().GetStringSlice("destination")
		var dests []*v1.RouteDestination
		for _, d := range destNames {
			vaultIDBytes, err := resolveToProto(d, r.vaults, "vault")
			if err != nil {
				return err
			}
			dests = append(dests, &v1.RouteDestination{VaultId: vaultIDBytes})
		}
		cfg.Destinations = dests
	}
	return nil
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
			idBytes, err := resolveToProto(args[0], r.routes, "route")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteRoute(context.Background(), connect.NewRequest(&v1.DeleteRouteRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted route %s\n", args[0])
			return nil
		},
	}
}
