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
// Server settings are promoted to the top level in a hierarchical layout
// that mirrors the internal config.Config structure.
type exportDoc struct {
	// Entity collections.
	Filters           []*v1.FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  []*v1.RotationPolicyConfig  `json:"rotation_policies,omitempty"`
	RetentionPolicies []*v1.RetentionPolicyConfig `json:"retention_policies,omitempty"`
	Vaults            []*v1.VaultConfig           `json:"vaults,omitempty"`
	Ingesters         []*v1.IngesterConfig        `json:"ingesters,omitempty"`
	Nodes             []*v1.NodeConfig            `json:"nodes,omitempty"`
	Certificates      []*certExport               `json:"certificates,omitempty"`
	Users             []*userExport               `json:"users,omitempty"`

	// Server settings — hierarchical.
	Auth                 *authExport      `json:"auth,omitempty"`
	Query                *queryExport     `json:"query,omitempty"`
	Scheduler            *schedulerExport `json:"scheduler,omitempty"`
	TLS                  *tlsExport       `json:"tls,omitempty"`
	Lookup               *lookupExport    `json:"lookup,omitempty"`
	SetupWizardDismissed bool             `json:"setup_wizard_dismissed,omitempty"`

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

// ---------------------------------------------------------------------------
// Hierarchical server-settings export types
// ---------------------------------------------------------------------------

type authExport struct {
	JWTSecret            string                `json:"jwt_secret,omitempty"` //nolint:gosec // export field name, not a credential
	TokenDuration        string                `json:"token_duration,omitempty"`
	RefreshTokenDuration string                `json:"refresh_token_duration,omitempty"`
	PasswordPolicy       *passwordPolicyExport `json:"password_policy,omitempty"`
}

type passwordPolicyExport struct {
	MinLength             int32 `json:"min_password_length,omitempty"`
	RequireMixedCase      bool  `json:"require_mixed_case,omitempty"`
	RequireDigit          bool  `json:"require_digit,omitempty"`
	RequireSpecial        bool  `json:"require_special,omitempty"`
	MaxConsecutiveRepeats int32 `json:"max_consecutive_repeats,omitempty"`
	ForbidAnimalNoise     bool  `json:"forbid_animal_noise,omitempty"`
}

type queryExport struct {
	Timeout           string `json:"timeout,omitempty"`
	MaxFollowDuration string `json:"max_follow_duration,omitempty"`
	MaxResultCount    int32  `json:"max_result_count,omitempty"`
}

type schedulerExport struct {
	MaxConcurrentJobs int32 `json:"max_concurrent_jobs,omitempty"`
}

type tlsExport struct {
	TLSEnabled          bool   `json:"tls_enabled,omitempty"`
	DefaultCert         string `json:"default_cert,omitempty"`
	HTTPToHTTPSRedirect bool   `json:"http_to_https_redirect,omitempty"`
	HTTPSPort           string `json:"https_port,omitempty"`
}

type lookupExport struct {
	GeoIPDBPath string         `json:"geoip_db_path,omitempty"`
	ASNDBPath   string         `json:"asn_db_path,omitempty"`
	MaxMind     *maxmindExport `json:"maxmind,omitempty"`
}

type maxmindExport struct {
	AutoDownload bool   `json:"auto_download,omitempty"`
	AccountID    string `json:"account_id,omitempty"`
	LicenseKey   string `json:"license_key,omitempty"`
	LastUpdate   string `json:"last_update,omitempty"`
}

// settingsToExport converts the nested proto GetSettingsResponse into
// the hierarchical export types. Zero-value sub-objects are returned as nil.
func settingsToExport(sc *v1.GetSettingsResponse) (auth *authExport, query *queryExport, sched *schedulerExport, tls *tlsExport, lookup *lookupExport, setupDismissed bool) {
	// Auth + PasswordPolicy
	if a := sc.GetAuth(); a != nil {
		auth = &authExport{
			JWTSecret:            a.GetJwtSecret(),
			TokenDuration:        a.GetTokenDuration(),
			RefreshTokenDuration: a.GetRefreshTokenDuration(),
		}
		if pp := a.GetPasswordPolicy(); pp != nil {
			pe := passwordPolicyExport{
				MinLength:             pp.GetMinLength(),
				RequireMixedCase:      pp.GetRequireMixedCase(),
				RequireDigit:          pp.GetRequireDigit(),
				RequireSpecial:        pp.GetRequireSpecial(),
				MaxConsecutiveRepeats: pp.GetMaxConsecutiveRepeats(),
				ForbidAnimalNoise:     pp.GetForbidAnimalNoise(),
			}
			if pe != (passwordPolicyExport{}) {
				auth.PasswordPolicy = &pe
			}
		}
		if *auth == (authExport{}) {
			auth = nil
		}
	}

	// Query
	if q := sc.GetQuery(); q != nil {
		query = &queryExport{
			Timeout:           q.GetTimeout(),
			MaxFollowDuration: q.GetMaxFollowDuration(),
			MaxResultCount:    q.GetMaxResultCount(),
		}
		if *query == (queryExport{}) {
			query = nil
		}
	}

	// Scheduler
	if s := sc.GetScheduler(); s != nil {
		sched = &schedulerExport{
			MaxConcurrentJobs: s.GetMaxConcurrentJobs(),
		}
		if *sched == (schedulerExport{}) {
			sched = nil
		}
	}

	// TLS
	if t := sc.GetTls(); t != nil {
		tls = &tlsExport{
			TLSEnabled:          t.GetEnabled(),
			DefaultCert:         t.GetDefaultCert(),
			HTTPToHTTPSRedirect: t.GetHttpToHttpsRedirect(),
			HTTPSPort:           t.GetHttpsPort(),
		}
		if *tls == (tlsExport{}) {
			tls = nil
		}
	}

	// Lookup + MaxMind
	if l := sc.GetLookup(); l != nil {
		lookup = &lookupExport{
			GeoIPDBPath: l.GetGeoipDbPath(),
			ASNDBPath:   l.GetAsnDbPath(),
		}
		if mm := l.GetMaxmind(); mm != nil {
			me := maxmindExport{
				AutoDownload: mm.GetAutoDownload(),
				AccountID:    mm.GetAccountId(),
				LicenseKey:   mm.GetLicenseKey(),
				LastUpdate:   mm.GetLastUpdate(),
			}
			if me != (maxmindExport{}) {
				lookup.MaxMind = &me
			}
		}
		if *lookup == (lookupExport{}) {
			lookup = nil
		}
	}

	setupDismissed = sc.GetSetupWizardDismissed()
	return
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

			scResp, err := client.Config.GetSettings(ctx, connect.NewRequest(&v1.GetSettingsRequest{IncludeSecrets: true}))
			if err != nil {
				return fmt.Errorf("get server config: %w", err)
			}

			certResp, err := client.Config.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
			if err != nil {
				return fmt.Errorf("list certificates: %w", err)
			}
			var certs []*certExport
			for _, c := range certResp.Msg.Certificates {
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

			auth, query, sched, tls, lookup, setupDismissed := settingsToExport(scResp.Msg)

			doc := &exportDoc{
				Filters:              cfgResp.Msg.Filters,
				RotationPolicies:     cfgResp.Msg.RotationPolicies,
				RetentionPolicies:    cfgResp.Msg.RetentionPolicies,
				Vaults:               cfgResp.Msg.Vaults,
				Ingesters:            cfgResp.Msg.Ingesters,
				Nodes:                cfgResp.Msg.NodeConfigs,
				Certificates:         certs,
				Users:                users,
				Auth:                 auth,
				Query:                query,
				Scheduler:            sched,
				TLS:                  tls,
				Lookup:               lookup,
				SetupWizardDismissed: setupDismissed,
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(doc)
		},
	}
}
