// Package cli implements the "gastrolog config" subcommand tree for managing
// a running gastrolog server via Connect RPC.
package cli

import (
	"context"
	"net"
	"net/http"
	"os"

	"connectrpc.com/connect"
	"gastrolog/internal/home"
	"gastrolog/internal/server"

	"github.com/spf13/cobra"
)

// AddClientFlags registers the shared connection and output flags as persistent
// flags on cmd. These are available to all subcommands in the tree.
// Can also be registered on rootCmd — server's local --addr flag shadows it.
func AddClientFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("addr", "http://localhost:4564", "server address")
	cmd.PersistentFlags().String("token", "", "authentication token (or GASTROLOG_TOKEN env)")
	cmd.PersistentFlags().StringP("output", "o", "table", "output format: table or json")
}

// NewConfigCommand returns the "config" command for entity and settings management.
func NewConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage gastrolog server configuration",
		Long:  "Connect to a running gastrolog server and manage vaults, ingesters, filters, routes, policies, certificates, and server settings.",
	}

	cmd.AddCommand(
		newCloudServiceCmd(),
		newFilterCmd(),
		newRotationPolicyCmd(),
		newRetentionPolicyCmd(),
		newTierCmd(),
		newVaultCmd(),
		newIngesterCmd(),
		newRouteCmd(),
		newFileCmd(),
		newNodeCmd(),
		newCertCmd(),
		newAuthCmd(),
		newQueryCmd(),
		newSchedulerCmd(),
		newTLSCmd(),
		newMaxMindCmd(),
		newExportCmd(),
		newImportCmd(),
	)

	return cmd
}

// NewClusterCommand returns the "cluster" command for cluster lifecycle management.
func NewClusterCommand() *cobra.Command {
	return newClusterCmd()
}

// NewJobCommand returns the "job" command for async job monitoring.
func NewJobCommand() *cobra.Command {
	return newJobCmd()
}

// NewUserCommand returns the "user" command for user CRUD (without login/register).
func NewUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage users",
	}
	cmd.AddCommand(
		newUserListCmd(),
		newUserGetCmd(),
		newUserCreateCmd(),
		newUserDeleteCmd(),
		newUserResetPasswordCmd(),
	)
	return cmd
}

// NewLoginCommand returns the top-level "login" command.
func NewLoginCommand() *cobra.Command {
	return newUserLoginCmd()
}

// NewRegisterCommand returns the top-level "register" command.
func NewRegisterCommand() *cobra.Command {
	return newUserRegisterCmd()
}

// clientFromCmd builds a Connect RPC client from the persistent flags on cmd.
// It prefers the unix socket when available (no auth needed), falling back to
// TCP with an optional bearer token.
//
// Socket resolution order:
//  1. --addr pointing at a .sock file → direct unix socket
//  2. --home → <home>/gastrolog.sock
//  3. Platform default home → <default>/gastrolog.sock
//  4. --addr as HTTP endpoint (with optional --token)
func clientFromCmd(cmd *cobra.Command) *server.Client {
	addr, _ := cmd.Flags().GetString("addr")
	token, _ := cmd.Flags().GetString("token")
	if token == "" {
		token = envToken()
	}

	addrChanged := cmd.Flags().Changed("addr")

	// If --addr points at a unix socket, use it directly.
	if addrChanged && isUnixSocket(addr) {
		return newUnixClient(addr)
	}

	// If no explicit token and addr wasn't overridden, try the unix socket.
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

// isUnixSocket returns true if path looks like a unix domain socket file.
func isUnixSocket(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().Type()&os.ModeSocket != 0
}

// newUnixClient creates a Connect client that dials the given unix socket.
func newUnixClient(sockPath string) *server.Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}
	return server.NewClientWithHTTP(httpClient, "http://localhost")
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
