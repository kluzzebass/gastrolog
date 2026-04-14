package cli

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

func newUserRegisterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "register",
		Short: "Register the first admin user (only works when no users exist)",
		RunE: func(cmd *cobra.Command, args []string) error {
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")

			client := clientFromCmd(cmd)
			resp, err := client.Auth.Register(context.Background(), connect.NewRequest(&v1.RegisterRequest{
				Username: username,
				Password: password,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Registered admin user %q (token: %s)\n", username, resp.Msg.Token)
			return nil
		},
	}
	cmd.Flags().String("username", "", "username (required)")
	cmd.Flags().String("password", "", "password (required)")
	_ = cmd.MarkFlagRequired("username")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func newUserLoginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Log in and print a JWT token",
		RunE: func(cmd *cobra.Command, args []string) error {
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")

			client := clientFromCmd(cmd)
			resp, err := client.Auth.Login(context.Background(), connect.NewRequest(&v1.LoginRequest{
				Username: username,
				Password: password,
			}))
			if err != nil {
				return err
			}
			fmt.Println(resp.Msg.Token.Token)
			return nil
		},
	}
	cmd.Flags().String("username", "", "username (required)")
	cmd.Flags().String("password", "", "password (required)")
	_ = cmd.MarkFlagRequired("username")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func newUserListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all users",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Auth.ListUsers(context.Background(), connect.NewRequest(&v1.ListUsersRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Users)
			}
			var rows [][]string
			for _, u := range resp.Msg.Users {
				rows = append(rows, []string{
					glid.FromBytes(u.Id).String(), u.Username, u.Role,
					formatTimestamp(u.CreatedAt),
				})
			}
			p.table([]string{"ID", "USERNAME", "ROLE", "CREATED"}, rows)
			return nil
		},
	}
}

func newUserGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <username-or-id>",
		Short: "Get user details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Auth.ListUsers(context.Background(), connect.NewRequest(&v1.ListUsersRequest{}))
			if err != nil {
				return err
			}
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.users, "user")
			if err != nil {
				return err
			}
			for _, u := range resp.Msg.Users {
				if glid.FromBytes(u.Id).String() == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(u)
					}
					p.kv([][2]string{
						{"ID", glid.FromBytes(u.Id).String()},
						{"Username", u.Username},
						{"Role", u.Role},
						{"Created", formatTimestamp(u.CreatedAt)},
						{"Updated", formatTimestamp(u.UpdatedAt)},
					})
					return nil
				}
			}
			return fmt.Errorf("user %q not found", args[0])
		},
	}
}

func newUserCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a user",
		RunE: func(cmd *cobra.Command, args []string) error {
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")
			role, _ := cmd.Flags().GetString("role")

			client := clientFromCmd(cmd)
			resp, err := client.Auth.CreateUser(context.Background(), connect.NewRequest(&v1.CreateUserRequest{
				Username: username,
				Password: password,
				Role:     role,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created user %q (%s) with role %s\n", resp.Msg.User.Username, glid.FromBytes(resp.Msg.User.Id), resp.Msg.User.Role)
			return nil
		},
	}
	cmd.Flags().String("username", "", "username (required)")
	cmd.Flags().String("password", "", "password (required)")
	cmd.Flags().String("role", "user", "role: admin or user")
	_ = cmd.MarkFlagRequired("username")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func newUserDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <username-or-id>",
		Short: "Delete a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.users, "user")
			if err != nil {
				return err
			}
			_, err = client.Auth.DeleteUser(context.Background(), connect.NewRequest(&v1.DeleteUserRequest{Id: idBytes}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted user %s\n", args[0])
			return nil
		},
	}
}

func newUserResetPasswordCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset-password <username-or-id>",
		Short: "Reset a user's password",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			password, _ := cmd.Flags().GetString("password")

			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.users, "user")
			if err != nil {
				return err
			}
			_, err = client.Auth.ResetPassword(context.Background(), connect.NewRequest(&v1.ResetPasswordRequest{
				Id:          idBytes,
				NewPassword: password,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Password reset for user %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().String("password", "", "new password (required)")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func formatTimestamp(unix int64) string {
	if unix == 0 {
		return "-"
	}
	return time.Unix(unix, 0).Format(time.RFC3339)
}
