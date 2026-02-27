// Package cli implements the "gastrolog config" subcommand tree for managing
// a running gastrolog server via Connect RPC.
package cli

import (
	"context"
	"net"
	"net/http"

	"connectrpc.com/connect"
	"gastrolog/internal/home"
	"gastrolog/internal/server"

	"github.com/spf13/cobra"
)

// NewConfigCommand returns the "config" command with all subcommands wired in.
func NewConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage gastrolog server configuration",
		Long:  "Connect to a running gastrolog server and manage vaults, ingesters, filters, policies, certificates, users, and server settings.",
	}

	cmd.PersistentFlags().String("addr", "http://localhost:4564", "server address")
	cmd.PersistentFlags().String("token", "", "authentication token (or GASTROLOG_TOKEN env)")
	cmd.PersistentFlags().StringP("output", "o", "table", "output format: table or json")

	cmd.AddCommand(
		newFilterCmd(),
		newRotationPolicyCmd(),
		newRetentionPolicyCmd(),
		newVaultCmd(),
		newIngesterCmd(),
		newNodeCmd(),
		newCertCmd(),
		newUserCmd(),
		newAuthCmd(),
		newQueryCmd(),
		newSchedulerCmd(),
		newTLSCmd(),
		newLookupCmd(),
		newExportCmd(),
		newImportCmd(),
	)

	return cmd
}

// clientFromCmd builds a Connect RPC client from the persistent flags on cmd.
// It prefers the unix socket when available (no auth needed), falling back to
// TCP with an optional bearer token.
func clientFromCmd(cmd *cobra.Command) *server.Client {
	addr, _ := cmd.Flags().GetString("addr")
	token, _ := cmd.Flags().GetString("token")
	if token == "" {
		token = envToken()
	}

	// If no explicit token and addr wasn't overridden, try the unix socket.
	addrChanged := cmd.Flags().Changed("addr")
	if token == "" && !addrChanged {
		homeFlag, _ := cmd.Flags().GetString("home")
		if client, ok := tryUnixSocket(homeFlag); ok {
			return client
		}
	}

	var opts []connect.ClientOption
	if token != "" {
		opts = append(opts, connect.WithInterceptors(newAuthInterceptor(token)))
	}
	return server.NewClient(addr, opts...)
}

// tryUnixSocket attempts to connect via the unix socket in the home directory.
// Uses the --home flag value if set, otherwise the platform default.
func tryUnixSocket(homeFlag string) (*server.Client, bool) {
	var hd home.Dir
	if homeFlag != "" {
		hd = home.New(homeFlag)
	} else {
		var err error
		hd, err = home.Default()
		if err != nil {
			return nil, false
		}
	}
	sockPath := hd.SocketPath()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		return nil, false
	}
	_ = conn.Close()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}
	return server.NewClientWithHTTP(httpClient, "http://localhost"), true
}

// outputFormat returns "json" or "table" from the --output flag.
func outputFormat(cmd *cobra.Command) string {
	f, _ := cmd.Flags().GetString("output")
	return f
}
