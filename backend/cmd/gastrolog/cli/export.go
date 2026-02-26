package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// exportDoc is the JSON structure for config export/import.
type exportDoc struct {
	Filters           []*v1.FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  []*v1.RotationPolicyConfig  `json:"rotation_policies,omitempty"`
	RetentionPolicies []*v1.RetentionPolicyConfig `json:"retention_policies,omitempty"`
	Vaults            []*v1.VaultConfig           `json:"vaults,omitempty"`
	Ingesters         []*v1.IngesterConfig        `json:"ingesters,omitempty"`
	Nodes             []*v1.NodeConfig            `json:"nodes,omitempty"`
	Certificates      []*certExport               `json:"certificates,omitempty"`
	Users             []*userExport               `json:"users,omitempty"`
	ServerConfig      *v1.GetServerConfigResponse `json:"server_config,omitempty"`
}

type certExport struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	CertFile string `json:"cert_file,omitempty"`
	KeyFile  string `json:"key_file,omitempty"`
}

type userExport struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

func newExportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "export",
		Short: "Export full configuration as JSON",
		Long:  "Exports all configuration entities to stdout as JSON. Passwords and secrets are excluded.",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfgResp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			scResp, err := client.Config.GetServerConfig(ctx, connect.NewRequest(&v1.GetServerConfigRequest{}))
			if err != nil {
				return fmt.Errorf("get server config: %w", err)
			}

			certResp, err := client.Config.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
			if err != nil {
				return fmt.Errorf("list certificates: %w", err)
			}
			var certs []*certExport
			for _, c := range certResp.Msg.Certificates {
				// Fetch details for each cert (to get cert_file/key_file).
				detail, err := client.Config.GetCertificate(ctx, connect.NewRequest(&v1.GetCertificateRequest{Id: c.Id}))
				if err != nil {
					return fmt.Errorf("get certificate %s: %w", c.Id, err)
				}
				certs = append(certs, &certExport{
					ID:       detail.Msg.Id,
					Name:     detail.Msg.Name,
					CertFile: detail.Msg.CertFile,
					KeyFile:  detail.Msg.KeyFile,
				})
			}

			userResp, err := client.Auth.ListUsers(ctx, connect.NewRequest(&v1.ListUsersRequest{}))
			if err != nil {
				return fmt.Errorf("list users: %w", err)
			}
			var users []*userExport
			for _, u := range userResp.Msg.Users {
				users = append(users, &userExport{
					ID:       u.Id,
					Username: u.Username,
					Role:     u.Role,
				})
			}

			doc := &exportDoc{
				Filters:           cfgResp.Msg.Filters,
				RotationPolicies:  cfgResp.Msg.RotationPolicies,
				RetentionPolicies: cfgResp.Msg.RetentionPolicies,
				Vaults:            cfgResp.Msg.Vaults,
				Ingesters:         cfgResp.Msg.Ingesters,
				Nodes:             cfgResp.Msg.NodeConfigs,
				Certificates:      certs,
				Users:             users,
				ServerConfig:      scResp.Msg,
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(doc)
		},
	}
}
