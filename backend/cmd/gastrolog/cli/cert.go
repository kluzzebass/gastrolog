package cli

import (
	"context"
	"fmt"
	"os"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newCertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cert",
		Short: "Manage TLS certificates",
	}
	cmd.AddCommand(
		newCertListCmd(),
		newCertGetCmd(),
		newCertCreateCmd(),
		newCertDeleteCmd(),
	)
	return cmd
}

func newCertListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all certificates",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.ListCertificates(context.Background(), connect.NewRequest(&v1.ListCertificatesRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Certificates)
			}
			var rows [][]string
			for _, c := range resp.Msg.Certificates {
				rows = append(rows, []string{c.Id, c.Name})
			}
			p.table([]string{"ID", "NAME"}, rows)
			return nil
		},
	}
}

func newCertGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get certificate details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.certs, "certificate")
			if err != nil {
				return err
			}
			resp, err := client.Config.GetCertificate(context.Background(), connect.NewRequest(&v1.GetCertificateRequest{Id: id}))
			if err != nil {
				return err
			}
			cert := resp.Msg
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(cert)
			}
			pairs := [][2]string{
				{"ID", cert.Id},
				{"Name", cert.Name},
				{"Cert File", cert.CertFile},
				{"Key File", cert.KeyFile},
			}
			if cert.CertPem != "" {
				pairs = append(pairs, [2]string{"Cert PEM", "(inline)"})
			}
			p.kv(pairs)
			return nil
		},
	}
}

func newCertCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a TLS certificate",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			certPemFile, _ := cmd.Flags().GetString("cert-pem")
			keyPemFile, _ := cmd.Flags().GetString("key-pem")
			certFile, _ := cmd.Flags().GetString("cert-file")
			keyFile, _ := cmd.Flags().GetString("key-file")
			setDefault, _ := cmd.Flags().GetBool("set-default")

			var certPem, keyPem string
			if certPemFile != "" {
				data, err := os.ReadFile(certPemFile) //nolint:gosec // path is from CLI flag, user explicitly provides it
				if err != nil {
					return fmt.Errorf("read cert PEM: %w", err)
				}
				certPem = string(data)
			}
			if keyPemFile != "" {
				data, err := os.ReadFile(keyPemFile) //nolint:gosec // path is from CLI flag, user explicitly provides it
				if err != nil {
					return fmt.Errorf("read key PEM: %w", err)
				}
				keyPem = string(data)
			}

			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()
			_, err := client.Config.PutCertificate(context.Background(), connect.NewRequest(&v1.PutCertificateRequest{
				Id:           id,
				Name:         name,
				CertPem:      certPem,
				KeyPem:       keyPem,
				CertFile:     certFile,
				KeyFile:      keyFile,
				SetAsDefault: setDefault,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created certificate %q (%s)\n", name, id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "certificate name (required)")
	cmd.Flags().String("cert-pem", "", "path to certificate PEM file (reads content)")
	cmd.Flags().String("key-pem", "", "path to private key PEM file (reads content)")
	cmd.Flags().String("cert-file", "", "path for server to watch for certificate")
	cmd.Flags().String("key-file", "", "path for server to watch for key")
	cmd.Flags().Bool("set-default", false, "set as default TLS certificate")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newCertDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a certificate",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.certs, "certificate")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteCertificate(context.Background(), connect.NewRequest(&v1.DeleteCertificateRequest{Id: id}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted certificate %s\n", args[0])
			return nil
		},
	}
}
