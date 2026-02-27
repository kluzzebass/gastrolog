// Package command provides serialization for config mutations applied via Raft.
//
// Each config.Store write method maps to a ConfigCommand variant. The FSM
// deserializes commands and dispatches to the in-memory store. ConfigSnapshot
// captures the full state for FSM.Snapshot()/Restore().
package command

import (
	"encoding/json"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

// Marshal serializes a ConfigCommand to bytes for raft.Apply().
func Marshal(cmd *gastrologv1.ConfigCommand) ([]byte, error) {
	return proto.Marshal(cmd)
}

// Unmarshal deserializes bytes back to a ConfigCommand.
func Unmarshal(b []byte) (*gastrologv1.ConfigCommand, error) {
	cmd := &gastrologv1.ConfigCommand{}
	if err := proto.Unmarshal(b, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

// MarshalSnapshot serializes a ConfigSnapshot to bytes.
func MarshalSnapshot(snap *gastrologv1.ConfigSnapshot) ([]byte, error) {
	return proto.Marshal(snap)
}

// UnmarshalSnapshot deserializes bytes back to a ConfigSnapshot.
func UnmarshalSnapshot(b []byte) (*gastrologv1.ConfigSnapshot, error) {
	snap := &gastrologv1.ConfigSnapshot{}
	if err := proto.Unmarshal(b, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

func putFilterCmd(cfg config.FilterConfig) *gastrologv1.PutFilterCommand {
	return &gastrologv1.PutFilterCommand{
		Id:         cfg.ID.String(),
		Name:       cfg.Name,
		Expression: cfg.Expression,
	}
}

// NewPutFilter creates a ConfigCommand for PutFilter.
func NewPutFilter(cfg config.FilterConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutFilter{PutFilter: putFilterCmd(cfg)},
	}
}

// NewDeleteFilter creates a ConfigCommand for DeleteFilter.
func NewDeleteFilter(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteFilter{
			DeleteFilter: &gastrologv1.DeleteFilterCommand{Id: id.String()},
		},
	}
}

// ExtractPutFilter converts a PutFilterCommand back to a FilterConfig.
func ExtractPutFilter(cmd *gastrologv1.PutFilterCommand) (config.FilterConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.FilterConfig{}, fmt.Errorf("parse filter id: %w", err)
	}
	return config.FilterConfig{
		ID:         id,
		Name:       cmd.GetName(),
		Expression: cmd.GetExpression(),
	}, nil
}

// ExtractDeleteFilter extracts the UUID from a DeleteFilterCommand.
func ExtractDeleteFilter(cmd *gastrologv1.DeleteFilterCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Rotation Policies
// ---------------------------------------------------------------------------

func putRotationPolicyCmd(cfg config.RotationPolicyConfig) *gastrologv1.PutRotationPolicyCommand {
	return &gastrologv1.PutRotationPolicyCommand{
		Id:         cfg.ID.String(),
		Name:       cfg.Name,
		MaxBytes:   cfg.MaxBytes,
		MaxAge:     cfg.MaxAge,
		MaxRecords: cfg.MaxRecords,
		Cron:       cfg.Cron,
	}
}

// NewPutRotationPolicy creates a ConfigCommand for PutRotationPolicy.
func NewPutRotationPolicy(cfg config.RotationPolicyConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutRotationPolicy{
			PutRotationPolicy: putRotationPolicyCmd(cfg),
		},
	}
}

// NewDeleteRotationPolicy creates a ConfigCommand for DeleteRotationPolicy.
func NewDeleteRotationPolicy(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteRotationPolicy{
			DeleteRotationPolicy: &gastrologv1.DeleteRotationPolicyCommand{Id: id.String()},
		},
	}
}

// ExtractPutRotationPolicy converts a PutRotationPolicyCommand back to a RotationPolicyConfig.
func ExtractPutRotationPolicy(cmd *gastrologv1.PutRotationPolicyCommand) (config.RotationPolicyConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.RotationPolicyConfig{}, fmt.Errorf("parse rotation policy id: %w", err)
	}
	return config.RotationPolicyConfig{
		ID:         id,
		Name:       cmd.GetName(),
		MaxBytes:   cmd.MaxBytes,
		MaxAge:     cmd.MaxAge,
		MaxRecords: cmd.MaxRecords,
		Cron:       cmd.Cron,
	}, nil
}

// ExtractDeleteRotationPolicy extracts the UUID from a DeleteRotationPolicyCommand.
func ExtractDeleteRotationPolicy(cmd *gastrologv1.DeleteRotationPolicyCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Retention Policies
// ---------------------------------------------------------------------------

func putRetentionPolicyCmd(cfg config.RetentionPolicyConfig) *gastrologv1.PutRetentionPolicyCommand {
	return &gastrologv1.PutRetentionPolicyCommand{
		Id:        cfg.ID.String(),
		Name:      cfg.Name,
		MaxAge:    cfg.MaxAge,
		MaxBytes:  cfg.MaxBytes,
		MaxChunks: cfg.MaxChunks,
	}
}

// NewPutRetentionPolicy creates a ConfigCommand for PutRetentionPolicy.
func NewPutRetentionPolicy(cfg config.RetentionPolicyConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutRetentionPolicy{
			PutRetentionPolicy: putRetentionPolicyCmd(cfg),
		},
	}
}

// NewDeleteRetentionPolicy creates a ConfigCommand for DeleteRetentionPolicy.
func NewDeleteRetentionPolicy(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteRetentionPolicy{
			DeleteRetentionPolicy: &gastrologv1.DeleteRetentionPolicyCommand{Id: id.String()},
		},
	}
}

// ExtractPutRetentionPolicy converts a PutRetentionPolicyCommand back to a RetentionPolicyConfig.
func ExtractPutRetentionPolicy(cmd *gastrologv1.PutRetentionPolicyCommand) (config.RetentionPolicyConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.RetentionPolicyConfig{}, fmt.Errorf("parse retention policy id: %w", err)
	}
	return config.RetentionPolicyConfig{
		ID:        id,
		Name:      cmd.GetName(),
		MaxAge:    cmd.MaxAge,
		MaxBytes:  cmd.MaxBytes,
		MaxChunks: cmd.MaxChunks,
	}, nil
}

// ExtractDeleteRetentionPolicy extracts the UUID from a DeleteRetentionPolicyCommand.
func ExtractDeleteRetentionPolicy(cmd *gastrologv1.DeleteRetentionPolicyCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Vaults
// ---------------------------------------------------------------------------

func putVaultCmd(cfg config.VaultConfig) *gastrologv1.PutVaultCommand {
	rules := make([]*gastrologv1.VaultRetentionRule, len(cfg.RetentionRules))
	for i, r := range cfg.RetentionRules {
		rules[i] = &gastrologv1.VaultRetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.String(),
			Action:            string(r.Action),
			Destination:       uuidPtrToString(r.Destination),
		}
	}
	return &gastrologv1.PutVaultCommand{
		Id:             cfg.ID.String(),
		Name:           cfg.Name,
		Type:           cfg.Type,
		Filter:         uuidPtrToString(cfg.Filter),
		Policy:         uuidPtrToString(cfg.Policy),
		RetentionRules: rules,
		Enabled:        cfg.Enabled,
		Params:         cfg.Params,
		NodeId:         cfg.NodeID,
	}
}

// NewPutVault creates a ConfigCommand for PutVault.
func NewPutVault(cfg config.VaultConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutVault{PutVault: putVaultCmd(cfg)},
	}
}

// NewDeleteVault creates a ConfigCommand for DeleteVault.
func NewDeleteVault(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteVault{
			DeleteVault: &gastrologv1.DeleteVaultCommand{Id: id.String()},
		},
	}
}

// ExtractPutVault converts a PutVaultCommand back to a VaultConfig.
func ExtractPutVault(cmd *gastrologv1.PutVaultCommand) (config.VaultConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.VaultConfig{}, fmt.Errorf("parse vault id: %w", err)
	}
	filter, err := parseOptionalUUID(cmd.GetFilter())
	if err != nil {
		return config.VaultConfig{}, fmt.Errorf("parse vault filter: %w", err)
	}
	policy, err := parseOptionalUUID(cmd.GetPolicy())
	if err != nil {
		return config.VaultConfig{}, fmt.Errorf("parse vault policy: %w", err)
	}

	var rules []config.RetentionRule
	for _, r := range cmd.GetRetentionRules() {
		rpID, err := uuid.Parse(r.GetRetentionPolicyId())
		if err != nil {
			return config.VaultConfig{}, fmt.Errorf("parse retention rule policy id: %w", err)
		}
		dest, err := parseOptionalUUID(r.GetDestination())
		if err != nil {
			return config.VaultConfig{}, fmt.Errorf("parse retention rule destination: %w", err)
		}
		rules = append(rules, config.RetentionRule{
			RetentionPolicyID: rpID,
			Action:            config.RetentionAction(r.GetAction()),
			Destination:       dest,
		})
	}

	return config.VaultConfig{
		ID:             id,
		Name:           cmd.GetName(),
		Type:           cmd.GetType(),
		Filter:         filter,
		Policy:         policy,
		RetentionRules: rules,
		Enabled:        cmd.GetEnabled(),
		Params:         nilIfEmpty(cmd.GetParams()),
		NodeID:         cmd.GetNodeId(),
	}, nil
}

// ExtractDeleteVault extracts the UUID from a DeleteVaultCommand.
func ExtractDeleteVault(cmd *gastrologv1.DeleteVaultCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Ingesters
// ---------------------------------------------------------------------------

func putIngesterCmd(cfg config.IngesterConfig) *gastrologv1.PutIngesterCommand {
	return &gastrologv1.PutIngesterCommand{
		Id:      cfg.ID.String(),
		Name:    cfg.Name,
		Type:    cfg.Type,
		Enabled: cfg.Enabled,
		Params:  cfg.Params,
		NodeId:  cfg.NodeID,
	}
}

// NewPutIngester creates a ConfigCommand for PutIngester.
func NewPutIngester(cfg config.IngesterConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutIngester{PutIngester: putIngesterCmd(cfg)},
	}
}

// NewDeleteIngester creates a ConfigCommand for DeleteIngester.
func NewDeleteIngester(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteIngester{
			DeleteIngester: &gastrologv1.DeleteIngesterCommand{Id: id.String()},
		},
	}
}

// ExtractPutIngester converts a PutIngesterCommand back to an IngesterConfig.
func ExtractPutIngester(cmd *gastrologv1.PutIngesterCommand) (config.IngesterConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.IngesterConfig{}, fmt.Errorf("parse ingester id: %w", err)
	}
	return config.IngesterConfig{
		ID:      id,
		Name:    cmd.GetName(),
		Type:    cmd.GetType(),
		Enabled: cmd.GetEnabled(),
		Params:  nilIfEmpty(cmd.GetParams()),
		NodeID:  cmd.GetNodeId(),
	}, nil
}

// ExtractDeleteIngester extracts the UUID from a DeleteIngesterCommand.
func ExtractDeleteIngester(cmd *gastrologv1.DeleteIngesterCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Server Settings
// ---------------------------------------------------------------------------

// NewPutServerSettings creates a ConfigCommand for persisting server-level settings.
// The settings are serialized as JSON inside a PutSettingCommand with key="server"
// for wire/snapshot compatibility.
func NewPutServerSettings(auth config.AuthConfig, query config.QueryConfig, sched config.SchedulerConfig, tls config.TLSConfig, lookup config.LookupConfig, setupDismissed bool) (*gastrologv1.ConfigCommand, error) {
	blob, err := json.Marshal(serverSettingsJSON{
		Auth:                 auth,
		Query:                query,
		Scheduler:            sched,
		TLS:                  tls,
		Lookup:               lookup,
		SetupWizardDismissed: setupDismissed,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal server settings: %w", err)
	}
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutSetting{
			PutSetting: &gastrologv1.PutSettingCommand{Key: "server", Value: string(blob)},
		},
	}, nil
}

// ExtractPutServerSettings parses server settings from a PutSettingCommand with key="server".
func ExtractPutServerSettings(value string) (config.AuthConfig, config.QueryConfig, config.SchedulerConfig, config.TLSConfig, config.LookupConfig, bool, error) {
	var ss serverSettingsJSON
	if err := json.Unmarshal([]byte(value), &ss); err != nil {
		return config.AuthConfig{}, config.QueryConfig{}, config.SchedulerConfig{}, config.TLSConfig{}, config.LookupConfig{}, false, fmt.Errorf("parse server settings: %w", err)
	}
	// Migrate flat MaxMind fields to nested MaxMindConfig if present.
	ss.Lookup = migrateLookupConfig(ss.Lookup, value)
	// Migrate flat password policy fields to nested PasswordPolicy if present.
	ss.Auth = migratePasswordPolicy(ss.Auth, value)
	return ss.Auth, ss.Query, ss.Scheduler, ss.TLS, ss.Lookup, ss.SetupWizardDismissed, nil
}

// serverSettingsJSON is the JSON shape for server settings.
// This matches the old ServerConfig layout for backward compatibility.
type serverSettingsJSON struct {
	Auth                 config.AuthConfig      `json:"auth,omitzero"`
	Query                config.QueryConfig     `json:"query,omitzero"`
	Scheduler            config.SchedulerConfig `json:"scheduler,omitzero"`
	TLS                  config.TLSConfig       `json:"tls,omitzero"`
	Lookup               config.LookupConfig    `json:"lookup,omitzero"`
	SetupWizardDismissed bool                   `json:"setup_wizard_dismissed,omitempty"`
}

// legacyLookupJSON captures the old flat MaxMind fields for migration.
type legacyLookupJSON struct {
	Lookup struct {
		GeoIPDBPath         string    `json:"geoip_db_path"`
		ASNDBPath           string    `json:"asn_db_path"`
		MaxMindAutoDownload bool      `json:"maxmind_auto_download"`
		MaxMindAccountID    string    `json:"maxmind_account_id"`
		MaxMindLicenseKey   string    `json:"maxmind_license_key"`
		MaxMindLastUpdate   time.Time `json:"maxmind_last_update"`
	} `json:"lookup"`
}

// migrateLookupConfig checks if the JSON has old flat MaxMind fields and migrates them
// into the nested MaxMindConfig.
func migrateLookupConfig(lc config.LookupConfig, raw string) config.LookupConfig {
	// If MaxMind is already populated, no migration needed.
	if lc.MaxMind.AutoDownload || lc.MaxMind.AccountID != "" || lc.MaxMind.LicenseKey != "" || !lc.MaxMind.LastUpdate.IsZero() {
		return lc
	}
	// Try to extract legacy flat fields.
	var legacy legacyLookupJSON
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		return lc
	}
	ll := legacy.Lookup
	if ll.MaxMindAutoDownload || ll.MaxMindAccountID != "" || ll.MaxMindLicenseKey != "" || !ll.MaxMindLastUpdate.IsZero() {
		lc.MaxMind = config.MaxMindConfig{
			AutoDownload: ll.MaxMindAutoDownload,
			AccountID:    ll.MaxMindAccountID,
			LicenseKey:   ll.MaxMindLicenseKey,
			LastUpdate:   ll.MaxMindLastUpdate,
		}
	}
	return lc
}

// legacyAuthJSON captures old flat password policy fields for migration.
type legacyAuthJSON struct {
	Auth struct {
		MinPasswordLength     int  `json:"min_password_length"`
		RequireMixedCase      bool `json:"require_mixed_case"`
		RequireDigit          bool `json:"require_digit"`
		RequireSpecial        bool `json:"require_special"`
		MaxConsecutiveRepeats int  `json:"max_consecutive_repeats"`
		ForbidAnimalNoise     bool `json:"forbid_animal_noise"`
	} `json:"auth"`
}

// migratePasswordPolicy checks if the JSON has old flat password policy fields
// on the auth object and migrates them into the nested PasswordPolicy.
func migratePasswordPolicy(ac config.AuthConfig, raw string) config.AuthConfig {
	// If PasswordPolicy is already populated, no migration needed.
	pp := ac.PasswordPolicy
	if pp.MinLength != 0 || pp.RequireMixedCase || pp.RequireDigit || pp.RequireSpecial || pp.MaxConsecutiveRepeats != 0 || pp.ForbidAnimalNoise {
		return ac
	}
	// Try to extract legacy flat fields.
	var legacy legacyAuthJSON
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		return ac
	}
	la := legacy.Auth
	if la.MinPasswordLength != 0 || la.RequireMixedCase || la.RequireDigit || la.RequireSpecial || la.MaxConsecutiveRepeats != 0 || la.ForbidAnimalNoise {
		ac.PasswordPolicy = config.PasswordPolicy{
			MinLength:             la.MinPasswordLength,
			RequireMixedCase:      la.RequireMixedCase,
			RequireDigit:          la.RequireDigit,
			RequireSpecial:        la.RequireSpecial,
			MaxConsecutiveRepeats: la.MaxConsecutiveRepeats,
			ForbidAnimalNoise:     la.ForbidAnimalNoise,
		}
	}
	return ac
}

// ExtractPutSetting returns the key and value from a PutSettingCommand.
// Kept for backward-compatible command dispatch.
func ExtractPutSetting(cmd *gastrologv1.PutSettingCommand) (key, value string) {
	return cmd.GetKey(), cmd.GetValue()
}

// ExtractDeleteSetting returns the key from a DeleteSettingCommand.
func ExtractDeleteSetting(cmd *gastrologv1.DeleteSettingCommand) string {
	return cmd.GetKey()
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

func putCertificateCmd(cert config.CertPEM) *gastrologv1.PutCertificateCommand {
	return &gastrologv1.PutCertificateCommand{
		Id:       cert.ID.String(),
		Name:     cert.Name,
		CertPem:  cert.CertPEM,
		KeyPem:   cert.KeyPEM,
		CertFile: cert.CertFile,
		KeyFile:  cert.KeyFile,
	}
}

// NewPutCertificate creates a ConfigCommand for PutCertificate.
func NewPutCertificate(cert config.CertPEM) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutCertificate{
			PutCertificate: putCertificateCmd(cert),
		},
	}
}

// NewDeleteCertificate creates a ConfigCommand for DeleteCertificate.
func NewDeleteCertificate(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteCertificate{
			DeleteCertificate: &gastrologv1.DeleteCertificateCommand{Id: id.String()},
		},
	}
}

// ExtractPutCertificate converts a PutCertificateCommand back to a CertPEM.
func ExtractPutCertificate(cmd *gastrologv1.PutCertificateCommand) (config.CertPEM, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.CertPEM{}, fmt.Errorf("parse certificate id: %w", err)
	}
	return config.CertPEM{
		ID:       id,
		Name:     cmd.GetName(),
		CertPEM:  cmd.GetCertPem(),
		KeyPEM:   cmd.GetKeyPem(),
		CertFile: cmd.GetCertFile(),
		KeyFile:  cmd.GetKeyFile(),
	}, nil
}

// ExtractDeleteCertificate extracts the UUID from a DeleteCertificateCommand.
func ExtractDeleteCertificate(cmd *gastrologv1.DeleteCertificateCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

func createUserCmd(u config.User) *gastrologv1.CreateUserCommand {
	return &gastrologv1.CreateUserCommand{
		Id:                 u.ID.String(),
		Username:           u.Username,
		PasswordHash:       u.PasswordHash,
		Role:               u.Role,
		Preferences:        u.Preferences,
		TokenInvalidatedAt: timestamppb.New(u.TokenInvalidatedAt),
		CreatedAt:          timestamppb.New(u.CreatedAt),
		UpdatedAt:          timestamppb.New(u.UpdatedAt),
	}
}

// NewCreateUser creates a ConfigCommand for CreateUser.
func NewCreateUser(u config.User) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_CreateUser{CreateUser: createUserCmd(u)},
	}
}

// NewUpdatePassword creates a ConfigCommand for UpdatePassword.
func NewUpdatePassword(id uuid.UUID, passwordHash string) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_UpdatePassword{
			UpdatePassword: &gastrologv1.UpdatePasswordCommand{
				Id:           id.String(),
				PasswordHash: passwordHash,
			},
		},
	}
}

// NewUpdateUserRole creates a ConfigCommand for UpdateUserRole.
func NewUpdateUserRole(id uuid.UUID, role string) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_UpdateUserRole{
			UpdateUserRole: &gastrologv1.UpdateUserRoleCommand{
				Id:   id.String(),
				Role: role,
			},
		},
	}
}

// NewUpdateUsername creates a ConfigCommand for UpdateUsername.
func NewUpdateUsername(id uuid.UUID, username string) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_UpdateUsername{
			UpdateUsername: &gastrologv1.UpdateUsernameCommand{
				Id:       id.String(),
				Username: username,
			},
		},
	}
}

// NewDeleteUser creates a ConfigCommand for DeleteUser.
func NewDeleteUser(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteUser{
			DeleteUser: &gastrologv1.DeleteUserCommand{Id: id.String()},
		},
	}
}

// NewInvalidateTokens creates a ConfigCommand for InvalidateTokens.
func NewInvalidateTokens(id uuid.UUID, at time.Time) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_InvalidateTokens{
			InvalidateTokens: &gastrologv1.InvalidateTokensCommand{
				Id: id.String(),
				At: timestamppb.New(at),
			},
		},
	}
}

// NewPutUserPreferences creates a ConfigCommand for PutUserPreferences.
func NewPutUserPreferences(id uuid.UUID, prefs string) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutUserPreferences{
			PutUserPreferences: &gastrologv1.PutUserPreferencesCommand{
				Id:          id.String(),
				Preferences: prefs,
			},
		},
	}
}

// ExtractCreateUser converts a CreateUserCommand back to a User.
func ExtractCreateUser(cmd *gastrologv1.CreateUserCommand) (config.User, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.User{}, fmt.Errorf("parse user id: %w", err)
	}
	return config.User{
		ID:                 id,
		Username:           cmd.GetUsername(),
		PasswordHash:       cmd.GetPasswordHash(),
		Role:               cmd.GetRole(),
		Preferences:        cmd.GetPreferences(),
		TokenInvalidatedAt: cmd.GetTokenInvalidatedAt().AsTime(),
		CreatedAt:          cmd.GetCreatedAt().AsTime(),
		UpdatedAt:          cmd.GetUpdatedAt().AsTime(),
	}, nil
}

// ExtractUpdatePassword returns the user ID and new password hash.
func ExtractUpdatePassword(cmd *gastrologv1.UpdatePasswordCommand) (uuid.UUID, string, error) {
	id, err := uuid.Parse(cmd.GetId())
	return id, cmd.GetPasswordHash(), err
}

// ExtractUpdateUserRole returns the user ID and new role.
func ExtractUpdateUserRole(cmd *gastrologv1.UpdateUserRoleCommand) (uuid.UUID, string, error) {
	id, err := uuid.Parse(cmd.GetId())
	return id, cmd.GetRole(), err
}

// ExtractUpdateUsername returns the user ID and new username.
func ExtractUpdateUsername(cmd *gastrologv1.UpdateUsernameCommand) (uuid.UUID, string, error) {
	id, err := uuid.Parse(cmd.GetId())
	return id, cmd.GetUsername(), err
}

// ExtractDeleteUser extracts the UUID from a DeleteUserCommand.
func ExtractDeleteUser(cmd *gastrologv1.DeleteUserCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ExtractInvalidateTokens returns the user ID and invalidation time.
func ExtractInvalidateTokens(cmd *gastrologv1.InvalidateTokensCommand) (uuid.UUID, time.Time, error) {
	id, err := uuid.Parse(cmd.GetId())
	return id, cmd.GetAt().AsTime(), err
}

// ExtractPutUserPreferences returns the user ID and preferences JSON.
func ExtractPutUserPreferences(cmd *gastrologv1.PutUserPreferencesCommand) (uuid.UUID, string, error) {
	id, err := uuid.Parse(cmd.GetId())
	return id, cmd.GetPreferences(), err
}

// ---------------------------------------------------------------------------
// Refresh Tokens
// ---------------------------------------------------------------------------

func createRefreshTokenCmd(t config.RefreshToken) *gastrologv1.CreateRefreshTokenCommand {
	return &gastrologv1.CreateRefreshTokenCommand{
		Id:        t.ID.String(),
		UserId:    t.UserID.String(),
		TokenHash: t.TokenHash,
		ExpiresAt: timestamppb.New(t.ExpiresAt),
		CreatedAt: timestamppb.New(t.CreatedAt),
	}
}

// NewCreateRefreshToken creates a ConfigCommand for CreateRefreshToken.
func NewCreateRefreshToken(t config.RefreshToken) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_CreateRefreshToken{
			CreateRefreshToken: createRefreshTokenCmd(t),
		},
	}
}

// NewDeleteRefreshToken creates a ConfigCommand for DeleteRefreshToken.
func NewDeleteRefreshToken(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteRefreshToken{
			DeleteRefreshToken: &gastrologv1.DeleteRefreshTokenCommand{Id: id.String()},
		},
	}
}

// NewDeleteUserRefreshTokens creates a ConfigCommand for DeleteUserRefreshTokens.
func NewDeleteUserRefreshTokens(userID uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteUserRefreshTokens{
			DeleteUserRefreshTokens: &gastrologv1.DeleteUserRefreshTokensCommand{
				UserId: userID.String(),
			},
		},
	}
}

// ExtractCreateRefreshToken converts a CreateRefreshTokenCommand back to a RefreshToken.
func ExtractCreateRefreshToken(cmd *gastrologv1.CreateRefreshTokenCommand) (config.RefreshToken, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.RefreshToken{}, fmt.Errorf("parse refresh token id: %w", err)
	}
	userID, err := uuid.Parse(cmd.GetUserId())
	if err != nil {
		return config.RefreshToken{}, fmt.Errorf("parse refresh token user id: %w", err)
	}
	return config.RefreshToken{
		ID:        id,
		UserID:    userID,
		TokenHash: cmd.GetTokenHash(),
		ExpiresAt: cmd.GetExpiresAt().AsTime(),
		CreatedAt: cmd.GetCreatedAt().AsTime(),
	}, nil
}

// ExtractDeleteRefreshToken extracts the UUID from a DeleteRefreshTokenCommand.
func ExtractDeleteRefreshToken(cmd *gastrologv1.DeleteRefreshTokenCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ExtractDeleteUserRefreshTokens extracts the user UUID from a DeleteUserRefreshTokensCommand.
func ExtractDeleteUserRefreshTokens(cmd *gastrologv1.DeleteUserRefreshTokensCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetUserId())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func putNodeConfigCmd(node config.NodeConfig) *gastrologv1.PutNodeConfigCommand {
	return &gastrologv1.PutNodeConfigCommand{
		Id:   node.ID.String(),
		Name: node.Name,
	}
}

// NewPutNodeConfig creates a ConfigCommand for PutNodeConfig.
func NewPutNodeConfig(node config.NodeConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutNodeConfig{PutNodeConfig: putNodeConfigCmd(node)},
	}
}

// NewDeleteNodeConfig creates a ConfigCommand for DeleteNodeConfig.
func NewDeleteNodeConfig(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteNodeConfig{
			DeleteNodeConfig: &gastrologv1.DeleteNodeConfigCommand{Id: id.String()},
		},
	}
}

// ExtractPutNodeConfig converts a PutNodeConfigCommand back to a NodeConfig.
func ExtractPutNodeConfig(cmd *gastrologv1.PutNodeConfigCommand) (config.NodeConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.NodeConfig{}, fmt.Errorf("parse node id: %w", err)
	}
	return config.NodeConfig{
		ID:   id,
		Name: cmd.GetName(),
	}, nil
}

// ExtractDeleteNodeConfig extracts the UUID from a DeleteNodeConfigCommand.
func ExtractDeleteNodeConfig(cmd *gastrologv1.DeleteNodeConfigCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Cluster TLS
// ---------------------------------------------------------------------------

// NewPutClusterTLS creates a ConfigCommand for PutClusterTLS.
func NewPutClusterTLS(tls config.ClusterTLS) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutClusterTls{
			PutClusterTls: &gastrologv1.PutClusterTLSCommand{
				CaCertPem:      []byte(tls.CACertPEM),
				CaKeyPem:       []byte(tls.CAKeyPEM),
				ClusterCertPem: []byte(tls.ClusterCertPEM),
				ClusterKeyPem:  []byte(tls.ClusterKeyPEM),
				JoinToken:      tls.JoinToken,
			},
		},
	}
}

// ExtractPutClusterTLS converts a PutClusterTLSCommand back to a ClusterTLS.
func ExtractPutClusterTLS(cmd *gastrologv1.PutClusterTLSCommand) config.ClusterTLS {
	return config.ClusterTLS{
		CACertPEM:      string(cmd.GetCaCertPem()),
		CAKeyPEM:       string(cmd.GetCaKeyPem()),
		ClusterCertPEM: string(cmd.GetClusterCertPem()),
		ClusterKeyPEM:  string(cmd.GetClusterKeyPem()),
		JoinToken:      cmd.GetJoinToken(),
	}
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

// BuildSnapshot creates a ConfigSnapshot from the full config state.
// Server settings (Auth, Query, etc.) are serialized as JSON in the Settings map
// under the "server" key for wire compatibility.
func BuildSnapshot(cfg *config.Config, users []config.User, tokens []config.RefreshToken, nodes []config.NodeConfig) *gastrologv1.ConfigSnapshot {
	snap := &gastrologv1.ConfigSnapshot{}

	// Serialize server settings into the Settings map for backward compatibility.
	blob, err := json.Marshal(serverSettingsJSON{
		Auth:                 cfg.Auth,
		Query:                cfg.Query,
		Scheduler:            cfg.Scheduler,
		TLS:                  cfg.TLS,
		Lookup:               cfg.Lookup,
		SetupWizardDismissed: cfg.SetupWizardDismissed,
	})
	if err == nil && string(blob) != "{}" {
		snap.Settings = map[string]string{"server": string(blob)}
	}

	for _, f := range cfg.Filters {
		snap.Filters = append(snap.Filters, putFilterCmd(f))
	}
	for _, rp := range cfg.RotationPolicies {
		snap.RotationPolicies = append(snap.RotationPolicies, putRotationPolicyCmd(rp))
	}
	for _, rp := range cfg.RetentionPolicies {
		snap.RetentionPolicies = append(snap.RetentionPolicies, putRetentionPolicyCmd(rp))
	}
	for _, v := range cfg.Vaults {
		snap.Vaults = append(snap.Vaults, putVaultCmd(v))
	}
	for _, ing := range cfg.Ingesters {
		snap.Ingesters = append(snap.Ingesters, putIngesterCmd(ing))
	}
	for _, c := range cfg.Certs {
		snap.Certificates = append(snap.Certificates, putCertificateCmd(c))
	}
	for _, u := range users {
		snap.Users = append(snap.Users, createUserCmd(u))
	}
	for _, t := range tokens {
		snap.RefreshTokens = append(snap.RefreshTokens, createRefreshTokenCmd(t))
	}
	for _, n := range nodes {
		snap.NodeConfigs = append(snap.NodeConfigs, putNodeConfigCmd(n))
	}

	// Include ClusterTLS if present on Config.
	if cfg.ClusterTLS != nil {
		snap.ClusterTls = NewPutClusterTLS(*cfg.ClusterTLS).GetPutClusterTls()
	}

	return snap
}

// RestoreSnapshot converts a ConfigSnapshot back to Go config types.
// If the snapshot's Settings map contains a "server" key, it is parsed
// into the Config's server-level fields (Auth, Query, etc.).
func RestoreSnapshot(snap *gastrologv1.ConfigSnapshot) (*config.Config, []config.User, []config.RefreshToken, []config.NodeConfig, error) { //nolint:gocognit // flat field mapping from snapshot proto
	cfg := &config.Config{}

	// Migrate server settings from the Settings map.
	if settings := snap.GetSettings(); len(settings) > 0 {
		if raw, ok := settings["server"]; ok {
			auth, query, sched, tls, lookup, dismissed, err := ExtractPutServerSettings(raw)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("restore server settings: %w", err)
			}
			cfg.Auth = auth
			cfg.Query = query
			cfg.Scheduler = sched
			cfg.TLS = tls
			cfg.Lookup = lookup
			cfg.SetupWizardDismissed = dismissed
		}
	}

	for _, f := range snap.GetFilters() {
		fc, err := ExtractPutFilter(f)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore filter: %w", err)
		}
		cfg.Filters = append(cfg.Filters, fc)
	}
	for _, rp := range snap.GetRotationPolicies() {
		rc, err := ExtractPutRotationPolicy(rp)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore rotation policy: %w", err)
		}
		cfg.RotationPolicies = append(cfg.RotationPolicies, rc)
	}
	for _, rp := range snap.GetRetentionPolicies() {
		rc, err := ExtractPutRetentionPolicy(rp)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore retention policy: %w", err)
		}
		cfg.RetentionPolicies = append(cfg.RetentionPolicies, rc)
	}
	for _, v := range snap.GetVaults() {
		vc, err := ExtractPutVault(v)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore vault: %w", err)
		}
		cfg.Vaults = append(cfg.Vaults, vc)
	}
	for _, ing := range snap.GetIngesters() {
		ic, err := ExtractPutIngester(ing)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore ingester: %w", err)
		}
		cfg.Ingesters = append(cfg.Ingesters, ic)
	}
	for _, c := range snap.GetCertificates() {
		cc, err := ExtractPutCertificate(c)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore certificate: %w", err)
		}
		cfg.Certs = append(cfg.Certs, cc)
	}

	users := make([]config.User, 0, len(snap.GetUsers()))
	for _, u := range snap.GetUsers() {
		user, err := ExtractCreateUser(u)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore user: %w", err)
		}
		users = append(users, user)
	}

	tokens := make([]config.RefreshToken, 0, len(snap.GetRefreshTokens()))
	for _, t := range snap.GetRefreshTokens() {
		token, err := ExtractCreateRefreshToken(t)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore refresh token: %w", err)
		}
		tokens = append(tokens, token)
	}

	nodes := make([]config.NodeConfig, 0, len(snap.GetNodeConfigs()))
	for _, n := range snap.GetNodeConfigs() {
		node, err := ExtractPutNodeConfig(n)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("restore node: %w", err)
		}
		nodes = append(nodes, node)
	}

	// Restore ClusterTLS if present in snapshot.
	if snap.ClusterTls != nil {
		tls := ExtractPutClusterTLS(snap.ClusterTls)
		cfg.ClusterTLS = &tls
	}

	return cfg, users, tokens, nodes, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func uuidPtrToString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func parseOptionalUUID(s string) (*uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func nilIfEmpty(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return m
}
