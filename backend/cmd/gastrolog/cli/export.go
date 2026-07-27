package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// exportDoc is the JSON structure for config export/import.
// Server settings are promoted to the top level in a hierarchical layout
// that mirrors the internal system.Config structure.
//
// Every operator-owned config entity in the store belongs here (see
// system.Config for the owning list). What cannot be restored is named in
// Excluded rather than dropped in silence — a document titled "full
// configuration" that quietly omits a config type asserts a completeness it
// does not have (gastrolog-2nr3aa).
type exportDoc struct {
	// Excluded and ContainsSecrets are emitted first, unconditionally, so
	// the limits of the document are the first thing a reader sees.
	Excluded        []exclusionNote `json:"excluded"`
	ContainsSecrets []string        `json:"contains_secrets"`

	// Entity collections.
	// gastrolog-4kkoo (Phase 5): no Filters; expressions live inline on routes.
	RotationPolicies   protoList[*v1.RotationPolicyConfig]  `json:"rotation_policies,omitempty"`
	RetentionPolicies  protoList[*v1.RetentionPolicyConfig] `json:"retention_policies,omitempty"`
	CloudServices      protoList[*v1.CloudService]          `json:"cloud_services,omitempty"`
	Vaults             protoList[*v1.VaultConfig]           `json:"vaults,omitempty"`
	Ingesters          protoList[*v1.IngesterConfig]        `json:"ingesters,omitempty"`
	Routes             protoList[*v1.RouteConfig]           `json:"routes,omitempty"`
	Nodes              []*nodeExport                        `json:"nodes,omitempty"`
	NodeStorageConfigs protoList[*v1.NodeStorageConfig]     `json:"node_storage_configs,omitempty"`
	ManagedFiles       []*managedFileExport                 `json:"managed_files,omitempty"`
	Certificates       []*certExport                        `json:"certificates,omitempty"`
	Users              []*userExport                        `json:"users,omitempty"`

	// Server settings — hierarchical.
	Auth                 *authExport                   `json:"auth,omitempty"`
	Query                *queryExport                  `json:"query,omitempty"`
	Scheduler            *schedulerExport              `json:"scheduler,omitempty"`
	TLS                  *tlsExport                    `json:"tls,omitempty"`
	MaxMind              *maxmindExport                `json:"maxmind,omitempty"`
	Cluster              *clusterExport                `json:"cluster,omitempty"`
	LogLevels            *protoMsg[*v1.LogLevelConfig] `json:"log_levels,omitempty"`
	SetupWizardDismissed bool                          `json:"setup_wizard_dismissed,omitempty"`
}

// protoList is a slice of proto messages that marshals as canonical proto
// JSON. Plain encoding/json cannot be used for generated proto types: a oneof
// field (a route's stages) marshals as a Go interface wrapper that it then
// refuses to decode, so routes could not be imported at all. protojson also
// renders enums by name and skips the generated internal fields.
type protoList[T proto.Message] []T

func (l protoList[T]) MarshalJSON() ([]byte, error) {
	parts := make([]json.RawMessage, 0, len(l))
	for _, m := range l {
		raw, err := marshalProtoJSON(m)
		if err != nil {
			return nil, err
		}
		parts = append(parts, raw)
	}
	return json.Marshal(parts)
}

func (l *protoList[T]) UnmarshalJSON(data []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	out := make(protoList[T], 0, len(parts))
	for _, part := range parts {
		msg, err := unmarshalProtoJSON[T](part)
		if err != nil {
			return err
		}
		out = append(out, msg)
	}
	*l = out
	return nil
}

// protoMsg is protoList's single-message counterpart.
type protoMsg[T proto.Message] struct{ Msg T }

func (m protoMsg[T]) MarshalJSON() ([]byte, error) { return marshalProtoJSON(m.Msg) }

func (m *protoMsg[T]) UnmarshalJSON(data []byte) error {
	msg, err := unmarshalProtoJSON[T](data)
	if err != nil {
		return err
	}
	m.Msg = msg
	return nil
}

// marshalProtoJSON renders one message with proto field names, compacted:
// protojson deliberately varies its whitespace, and an export document that
// differs run to run is useless for diffing two clusters' configs.
func marshalProtoJSON(m proto.Message) (json.RawMessage, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalProtoJSON[T proto.Message](data []byte) (T, error) {
	var zero T
	msg, ok := zero.ProtoReflect().New().Interface().(T)
	if !ok {
		return zero, fmt.Errorf("cannot allocate %T", zero)
	}
	if err := protojson.Unmarshal(data, msg); err != nil {
		return zero, err
	}
	return msg, nil
}

// exclusionNote names one thing this export deliberately leaves out and why,
// so a restore is never mistaken for complete.
type exclusionNote struct {
	Item   string `json:"item"`
	Reason string `json:"reason"`
}

// exportExclusions is the standing list of what a config export cannot carry.
// Every entry is either a secret the API never returns, content that is not
// configuration (uploaded blobs), or cluster-managed runtime state that the
// cluster re-derives. Config never belongs on this list.
func exportExclusions() []exclusionNote {
	return []exclusionNote{
		{
			Item:   "user passwords",
			Reason: "never returned by the API. Users are listed for reference only — 'config import' cannot recreate them; use 'config user create'.",
		},
		{
			Item:   "auth JWT signing secret",
			Reason: "never returned by the API. The target server keeps or generates its own; existing tokens do not survive a restore onto a different server.",
		},
		{
			Item:   "certificate private keys and inline PEM material",
			Reason: "never returned by the API. Certificates configured with cert_file/key_file paths restore fully; a certificate stored as inline PEM cannot be restored from an export.",
		},
		{
			Item:   "user sessions (refresh tokens)",
			Reason: "session state, not configuration.",
		},
		{
			Item:   "managed file contents",
			Reason: "uploaded blobs (GeoIP databases, lookup tables). Listed by name, size and sha256 so references can be checked; re-upload with 'config file upload'.",
		},
		{
			Item:   "runtime cluster state",
			Reason: "vault placements, node lifecycle state, ingester alive/checkpoint/assignment state and cluster TLS identity are cluster-managed and re-derived after a restore.",
		},
	}
}

// exportSecrets names the secrets this document actually carries, so the file
// is handled as the credential store it is. Computed from the content, not
// asserted from the format: claiming credentials that are not there is the
// same class of lie as omitting the ones that are.
func exportSecrets(services []*v1.CloudService, maxmind *maxmindExport) []string {
	out := []string{} // an explicit empty list, not a null: "none" is a fact too
	for _, cs := range services {
		if cs.GetAccessKey() != "" || cs.GetSecretKey() != "" ||
			cs.GetConnectionString() != "" || cs.GetCredentialsJson() != "" {
			out = append(out, "cloud service credentials (access_key, secret_key, connection_string, credentials_json)")
			break
		}
	}
	if maxmind != nil && (maxmind.LicenseKey != "" || maxmind.AccountID != "") {
		out = append(out, "maxmind account id and license key")
	}
	return out
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

// nodeExport carries the operator-owned part of a node record. Node lifecycle
// state (state, state_since) is cluster-managed: PutNodeConfig ignores what it
// is handed and stamps the node live, so exporting it would advertise a
// setting the import cannot honour.
type nodeExport struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// managedFileExport is metadata for an uploaded file. The content lives on
// disk, not in the config store, so this section is informational: it lets an
// operator see which file a FileID reference points at. Import cannot restore
// it (see exportExclusions).
type managedFileExport struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	SHA256     string `json:"sha256,omitempty"`
	Size       int64  `json:"size,omitempty"`
	UploadedAt string `json:"uploaded_at,omitempty"`
}

// ---------------------------------------------------------------------------
// Hierarchical server-settings export types
// ---------------------------------------------------------------------------

type authExport struct {
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

type maxmindExport struct {
	AutoDownload bool   `json:"auto_download,omitempty"`
	AccountID    string `json:"account_id,omitempty"`
	LicenseKey   string `json:"license_key,omitempty"`
	LastUpdate   string `json:"last_update,omitempty"`
}

type clusterExport struct {
	BroadcastInterval  string `json:"broadcast_interval,omitempty"`
	HeartbeatInterval  string `json:"heartbeat_interval,omitempty"`
	PipelineBacklogMax string `json:"pipeline_backlog_max,omitempty"`
}

// settingsToExport converts the nested proto GetSettingsResponse into
// the hierarchical export types. Zero-value sub-objects are returned as nil.
func settingsToExport(sc *v1.GetSettingsResponse) (auth *authExport, query *queryExport, sched *schedulerExport, tls *tlsExport, maxmind *maxmindExport, cluster *clusterExport, setupDismissed bool) {
	// Auth + PasswordPolicy
	if a := sc.GetAuth(); a != nil {
		auth = &authExport{
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

	// MaxMind
	if mm := sc.GetMaxmind(); mm != nil {
		me := maxmindExport{
			AutoDownload: mm.GetAutoDownload(),
			AccountID:    string(mm.GetAccountId()),
			LicenseKey:   mm.GetLicenseKey(),
			LastUpdate:   mm.GetLastUpdate(),
		}
		if me != (maxmindExport{}) {
			maxmind = &me
		}
	}

	// Cluster
	if cl := sc.GetCluster(); cl != nil {
		ce := clusterExport{
			BroadcastInterval:  cl.GetBroadcastInterval(),
			HeartbeatInterval:  cl.GetHeartbeatInterval(),
			PipelineBacklogMax: cl.GetPipelineBacklogMax(),
		}
		if ce != (clusterExport{}) {
			cluster = &ce
		}
	}

	setupDismissed = sc.GetSetupWizardDismissed()
	return
}

// logLevelsExport boxes the log level config for the document, or returns nil
// when the cluster has never set one.
func logLevelsExport(cfg *v1.LogLevelConfig) *protoMsg[*v1.LogLevelConfig] {
	if cfg == nil {
		return nil
	}
	return &protoMsg[*v1.LogLevelConfig]{Msg: cfg}
}

// strippedVaults returns copies of the vault configs with the system-managed
// placements cleared. Placements are assigned by the placement manager against
// the storages of the nodes that exist right now; carrying them through an
// export would let an import overwrite live placement decisions with a
// snapshot of old ones.
func strippedVaults(vaults []*v1.VaultConfig) []*v1.VaultConfig {
	out := make([]*v1.VaultConfig, 0, len(vaults))
	for _, v := range vaults {
		c, ok := proto.Clone(v).(*v1.VaultConfig)
		if !ok {
			continue
		}
		c.Placements = nil
		out = append(out, c)
	}
	return out
}

func newExportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "export",
		Short: "Export full configuration as JSON",
		Long: "Exports every configuration entity to stdout as JSON: rotation and retention " +
			"policies, cloud services, vaults, ingesters, routes, nodes, node storage, " +
			"certificates, users, log levels and server settings.\n\n" +
			"What the document cannot carry is listed in its \"excluded\" section — user " +
			"passwords, the JWT signing secret, certificate private keys, managed file " +
			"contents and cluster-managed runtime state. Users and managed files are " +
			"listed for reference but 'config import' cannot recreate them.\n\n" +
			"The output DOES contain cloud service credentials and the MaxMind license " +
			"key (see \"contains_secrets\"). Treat the file as a secret.",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfgResp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			scResp, err := client.System.GetSettings(ctx, connect.NewRequest(&v1.GetSettingsRequest{IncludeSecrets: true}))
			if err != nil {
				return fmt.Errorf("get server config: %w", err)
			}

			certResp, err := client.System.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
			if err != nil {
				return fmt.Errorf("list certificates: %w", err)
			}
			var certs []*certExport
			for _, c := range certResp.Msg.Certificates {
				detail, err := client.System.GetCertificate(ctx, connect.NewRequest(&v1.GetCertificateRequest{Id: c.Id}))
				if err != nil {
					return fmt.Errorf("get certificate %s: %w", c.Id, err)
				}
				certs = append(certs, &certExport{
					ID:       glid.FromBytes(detail.Msg.Id).String(),
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
					ID:       glid.FromBytes(u.Id).String(),
					Username: u.Username,
					Role:     u.Role,
				})
			}

			auth, query, sched, tls, maxmind, cluster, setupDismissed := settingsToExport(scResp.Msg)

			nodes := make([]*nodeExport, 0, len(cfgResp.Msg.NodeConfigs))
			for _, n := range cfgResp.Msg.NodeConfigs {
				nodes = append(nodes, &nodeExport{
					ID:   glid.FromBytes(n.Id).String(),
					Name: n.Name,
				})
			}

			files := make([]*managedFileExport, 0, len(cfgResp.Msg.ManagedFiles))
			for _, f := range cfgResp.Msg.ManagedFiles {
				files = append(files, &managedFileExport{
					ID:         glid.FromBytes(f.Id).String(),
					Name:       f.Name,
					SHA256:     f.Sha256,
					Size:       f.Size,
					UploadedAt: f.UploadedAt,
				})
			}

			doc := &exportDoc{
				Excluded:             exportExclusions(),
				ContainsSecrets:      exportSecrets(cfgResp.Msg.CloudServices, maxmind),
				RotationPolicies:     cfgResp.Msg.RotationPolicies,
				RetentionPolicies:    cfgResp.Msg.RetentionPolicies,
				CloudServices:        cfgResp.Msg.CloudServices,
				Vaults:               strippedVaults(cfgResp.Msg.Vaults),
				Ingesters:            cfgResp.Msg.Ingesters,
				Routes:               cfgResp.Msg.Routes,
				Nodes:                nodes,
				NodeStorageConfigs:   cfgResp.Msg.NodeStorageConfigs,
				ManagedFiles:         files,
				Certificates:         certs,
				Users:                users,
				Auth:                 auth,
				Query:                query,
				Scheduler:            sched,
				TLS:                  tls,
				MaxMind:              maxmind,
				Cluster:              cluster,
				LogLevels:            logLevelsExport(cfgResp.Msg.LogLevels),
				SetupWizardDismissed: setupDismissed,
			}

			return newPrinter("json").json(doc)
		},
	}
}
