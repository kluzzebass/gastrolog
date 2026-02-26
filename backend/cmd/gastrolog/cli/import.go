package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import [file]",
		Short: "Import configuration from JSON",
		Long:  "Imports configuration from a JSON file (or stdin if no file given). Use --merge (default) to upsert or --replace to delete all existing entities first.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			replace, _ := cmd.Flags().GetBool("replace")
			yes, _ := cmd.Flags().GetBool("yes")

			doc, err := readImportDoc(args)
			if err != nil {
				return err
			}

			if replace && !yes {
				fmt.Print("This will DELETE all existing entities before importing. Continue? [y/N] ")
				reader := bufio.NewReader(os.Stdin)
				line, _ := reader.ReadString('\n')
				if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "y") {
					fmt.Println("Aborted.")
					return nil
				}
			}

			client := clientFromCmd(cmd)
			ctx := context.Background()

			if replace {
				if err := deleteAll(ctx, client); err != nil {
					return fmt.Errorf("delete existing entities: %w", err)
				}
			}

			imported, err := importEntities(ctx, client, &doc)
			if err != nil {
				return err
			}

			fmt.Printf("Imported %d entities\n", imported)
			return nil
		},
	}
	cmd.Flags().Bool("merge", true, "merge with existing config (default)")
	cmd.Flags().Bool("replace", false, "delete all existing entities before importing")
	cmd.Flags().Bool("yes", false, "skip confirmation for --replace")
	cmd.MarkFlagsMutuallyExclusive("merge", "replace")
	return cmd
}

// readImportDoc reads and decodes an exportDoc from a file argument or stdin.
func readImportDoc(args []string) (exportDoc, error) {
	var doc exportDoc
	if len(args) == 1 {
		f, err := os.Open(args[0])
		if err != nil {
			return doc, err
		}
		defer func() { _ = f.Close() }()
		if err := json.NewDecoder(f).Decode(&doc); err != nil {
			return doc, fmt.Errorf("decode JSON: %w", err)
		}
	} else {
		if err := json.NewDecoder(os.Stdin).Decode(&doc); err != nil {
			return doc, fmt.Errorf("decode JSON from stdin: %w", err)
		}
	}
	return doc, nil
}

// importEntities imports all entities from the document in dependency order.
func importEntities(ctx context.Context, client *server.Client, doc *exportDoc) (int, error) {
	r, err := newResolver(ctx, client)
	if err != nil {
		return 0, err
	}

	var imported int

	for _, f := range doc.Filters {
		ensureID(f.Name, r.filters, &f.Id)
		_, err := client.Config.PutFilter(ctx, connect.NewRequest(&v1.PutFilterRequest{
			Config: f,
		}))
		if err != nil {
			return imported, fmt.Errorf("import filter %q: %w", f.Name, err)
		}
		imported++
	}

	for _, p := range doc.RotationPolicies {
		ensureID(p.Name, r.rotationPolicies, &p.Id)
		_, err := client.Config.PutRotationPolicy(ctx, connect.NewRequest(&v1.PutRotationPolicyRequest{
			Config: p,
		}))
		if err != nil {
			return imported, fmt.Errorf("import rotation policy %q: %w", p.Name, err)
		}
		imported++
	}

	for _, p := range doc.RetentionPolicies {
		ensureID(p.Name, r.retentionPolicies, &p.Id)
		_, err := client.Config.PutRetentionPolicy(ctx, connect.NewRequest(&v1.PutRetentionPolicyRequest{
			Config: p,
		}))
		if err != nil {
			return imported, fmt.Errorf("import retention policy %q: %w", p.Name, err)
		}
		imported++
	}

	for _, v := range doc.Vaults {
		ensureID(v.Name, r.vaults, &v.Id)
		_, err := client.Config.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{
			Config: v,
		}))
		if err != nil {
			return imported, fmt.Errorf("import vault %q: %w", v.Name, err)
		}
		imported++
	}

	for _, ig := range doc.Ingesters {
		ensureID(ig.Name, r.ingesters, &ig.Id)
		_, err := client.Config.PutIngester(ctx, connect.NewRequest(&v1.PutIngesterRequest{
			Config: ig,
		}))
		if err != nil {
			return imported, fmt.Errorf("import ingester %q: %w", ig.Name, err)
		}
		imported++
	}

	for _, n := range doc.Nodes {
		ensureID(n.Name, r.nodes, &n.Id)
		_, err := client.Config.PutNodeConfig(ctx, connect.NewRequest(&v1.PutNodeConfigRequest{
			Config: n,
		}))
		if err != nil {
			return imported, fmt.Errorf("import node %q: %w", n.Name, err)
		}
		imported++
	}

	for _, c := range doc.Certificates {
		ensureID(c.Name, r.certs, &c.ID)
		_, err := client.Config.PutCertificate(ctx, connect.NewRequest(&v1.PutCertificateRequest{
			Id:       c.ID,
			Name:     c.Name,
			CertFile: c.CertFile,
			KeyFile:  c.KeyFile,
		}))
		if err != nil {
			return imported, fmt.Errorf("import certificate %q: %w", c.Name, err)
		}
		imported++
	}

	for _, u := range doc.Users {
		// Users can only be created with passwords, which we don't export.
		// On import, we skip users that already exist and warn about new ones.
		if _, ok := r.users[strings.ToLower(u.Username)]; ok {
			continue // already exists, skip (no way to update role without password)
		}
		_, _ = fmt.Fprintf(os.Stderr, "Warning: skipping user %q — cannot create without password\n", u.Username)
	}

	if doc.ServerConfig != nil {
		req := serverConfigToRequest(doc.ServerConfig)
		_, err := client.Config.PutServerConfig(ctx, connect.NewRequest(req))
		if err != nil {
			return imported, fmt.Errorf("import server config: %w", err)
		}
		imported++
	}

	return imported, nil
}

// ensureID reuses an existing ID if the name matches, or generates a new UUIDv7.
func ensureID(name string, existing map[string]string, id *string) {
	if existingID, ok := existing[strings.ToLower(name)]; ok {
		*id = existingID
	} else if *id == "" {
		*id = uuid.Must(uuid.NewV7()).String()
	}
}

// deleteAll removes all config entities (not server config).
func deleteAll(ctx context.Context, client *server.Client) error {
	resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
	if err != nil {
		return err
	}

	// Delete in reverse dependency order: vaults, ingesters first, then policies/filters.
	for _, v := range resp.Msg.Vaults {
		if _, err := client.Config.DeleteVault(ctx, connect.NewRequest(&v1.DeleteVaultRequest{Id: v.Id, Force: true})); err != nil {
			return fmt.Errorf("delete vault %s: %w", v.Name, err)
		}
	}
	for _, ig := range resp.Msg.Ingesters {
		if _, err := client.Config.DeleteIngester(ctx, connect.NewRequest(&v1.DeleteIngesterRequest{Id: ig.Id})); err != nil {
			return fmt.Errorf("delete ingester %s: %w", ig.Name, err)
		}
	}
	for _, f := range resp.Msg.Filters {
		if _, err := client.Config.DeleteFilter(ctx, connect.NewRequest(&v1.DeleteFilterRequest{Id: f.Id})); err != nil {
			return fmt.Errorf("delete filter %s: %w", f.Name, err)
		}
	}
	for _, p := range resp.Msg.RotationPolicies {
		if _, err := client.Config.DeleteRotationPolicy(ctx, connect.NewRequest(&v1.DeleteRotationPolicyRequest{Id: p.Id})); err != nil {
			return fmt.Errorf("delete rotation policy %s: %w", p.Name, err)
		}
	}
	for _, p := range resp.Msg.RetentionPolicies {
		if _, err := client.Config.DeleteRetentionPolicy(ctx, connect.NewRequest(&v1.DeleteRetentionPolicyRequest{Id: p.Id})); err != nil {
			return fmt.Errorf("delete retention policy %s: %w", p.Name, err)
		}
	}

	// Delete certs.
	certResp, err := client.Config.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
	if err == nil {
		for _, c := range certResp.Msg.Certificates {
			if _, err := client.Config.DeleteCertificate(ctx, connect.NewRequest(&v1.DeleteCertificateRequest{Id: c.Id})); err != nil {
				return fmt.Errorf("delete certificate %s: %w", c.Name, err)
			}
		}
	}

	return nil
}

// serverConfigToRequest converts GetServerConfigResponse fields into a PutServerConfigRequest.
func serverConfigToRequest(sc *v1.GetServerConfigResponse) *v1.PutServerConfigRequest {
	req := &v1.PutServerConfigRequest{}
	if sc.TokenDuration != "" {
		req.TokenDuration = &sc.TokenDuration
	}
	if sc.MinPasswordLength != 0 {
		req.MinPasswordLength = &sc.MinPasswordLength
	}
	if sc.MaxConcurrentJobs != 0 {
		req.MaxConcurrentJobs = &sc.MaxConcurrentJobs
	}
	if sc.TlsDefaultCert != "" {
		req.TlsDefaultCert = &sc.TlsDefaultCert
	}
	if sc.TlsEnabled {
		req.TlsEnabled = &sc.TlsEnabled
	}
	if sc.HttpToHttpsRedirect {
		req.HttpToHttpsRedirect = &sc.HttpToHttpsRedirect
	}
	if sc.RequireMixedCase {
		req.RequireMixedCase = &sc.RequireMixedCase
	}
	if sc.RequireDigit {
		req.RequireDigit = &sc.RequireDigit
	}
	if sc.RequireSpecial {
		req.RequireSpecial = &sc.RequireSpecial
	}
	if sc.MaxConsecutiveRepeats != 0 {
		req.MaxConsecutiveRepeats = &sc.MaxConsecutiveRepeats
	}
	if sc.ForbidAnimalNoise {
		req.ForbidAnimalNoise = &sc.ForbidAnimalNoise
	}
	if sc.HttpsPort != "" {
		req.HttpsPort = &sc.HttpsPort
	}
	if sc.MaxFollowDuration != "" {
		req.MaxFollowDuration = &sc.MaxFollowDuration
	}
	if sc.QueryTimeout != "" {
		req.QueryTimeout = &sc.QueryTimeout
	}
	if sc.RefreshTokenDuration != "" {
		req.RefreshTokenDuration = &sc.RefreshTokenDuration
	}
	if sc.MaxResultCount != 0 {
		req.MaxResultCount = &sc.MaxResultCount
	}
	if sc.SetupWizardDismissed {
		req.SetupWizardDismissed = &sc.SetupWizardDismissed
	}
	if sc.GeoipDbPath != "" {
		req.GeoipDbPath = &sc.GeoipDbPath
	}
	if sc.AsnDbPath != "" {
		req.AsnDbPath = &sc.AsnDbPath
	}
	if sc.MaxmindAutoDownload {
		req.MaxmindAutoDownload = &sc.MaxmindAutoDownload
	}
	return req
}
