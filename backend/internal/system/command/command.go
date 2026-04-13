// Package command provides serialization for config mutations applied via Raft.
//
// Each system.Store write method maps to a ConfigCommand variant. The FSM
// deserializes commands and dispatches to the in-memory store. ConfigSnapshot
// captures the full state for FSM.Snapshot()/Restore().
package command

import (
	"encoding/json"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

// Marshal serializes a ConfigCommand to bytes for raft.Apply().
func Marshal(cmd *gastrologv1.SystemCommand) ([]byte, error) {
	return proto.Marshal(cmd)
}

// Unmarshal deserializes bytes back to a ConfigCommand.
func Unmarshal(b []byte) (*gastrologv1.SystemCommand, error) {
	cmd := &gastrologv1.SystemCommand{}
	if err := proto.Unmarshal(b, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

// MarshalSnapshot serializes a ConfigSnapshot to bytes.
func MarshalSnapshot(snap *gastrologv1.SystemSnapshot) ([]byte, error) {
	return proto.Marshal(snap)
}

// UnmarshalSnapshot deserializes bytes back to a ConfigSnapshot.
func UnmarshalSnapshot(b []byte) (*gastrologv1.SystemSnapshot, error) {
	snap := &gastrologv1.SystemSnapshot{}
	if err := proto.Unmarshal(b, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

func putFilterCmd(cfg system.FilterConfig) *gastrologv1.PutFilterCommand {
	return &gastrologv1.PutFilterCommand{
		Id:         cfg.ID.String(),
		Name:       cfg.Name,
		Expression: cfg.Expression,
	}
}

// NewPutFilter creates a ConfigCommand for PutFilter.
func NewPutFilter(cfg system.FilterConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutFilter{PutFilter: putFilterCmd(cfg)},
	}
}

// NewDeleteFilter creates a ConfigCommand for DeleteFilter.
func NewDeleteFilter(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteFilter{
			DeleteFilter: &gastrologv1.DeleteFilterCommand{Id: id.String()},
		},
	}
}

// ExtractPutFilter converts a PutFilterCommand back to a FilterConfig.
func ExtractPutFilter(cmd *gastrologv1.PutFilterCommand) (system.FilterConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.FilterConfig{}, fmt.Errorf("parse filter id: %w", err)
	}
	return system.FilterConfig{
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

func putRotationPolicyCmd(cfg system.RotationPolicyConfig) *gastrologv1.PutRotationPolicyCommand {
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
func NewPutRotationPolicy(cfg system.RotationPolicyConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutRotationPolicy{
			PutRotationPolicy: putRotationPolicyCmd(cfg),
		},
	}
}

// NewDeleteRotationPolicy creates a ConfigCommand for DeleteRotationPolicy.
func NewDeleteRotationPolicy(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteRotationPolicy{
			DeleteRotationPolicy: &gastrologv1.DeleteRotationPolicyCommand{Id: id.String()},
		},
	}
}

// ExtractPutRotationPolicy converts a PutRotationPolicyCommand back to a RotationPolicyConfig.
func ExtractPutRotationPolicy(cmd *gastrologv1.PutRotationPolicyCommand) (system.RotationPolicyConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.RotationPolicyConfig{}, fmt.Errorf("parse rotation policy id: %w", err)
	}
	return system.RotationPolicyConfig{
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

func putRetentionPolicyCmd(cfg system.RetentionPolicyConfig) *gastrologv1.PutRetentionPolicyCommand {
	return &gastrologv1.PutRetentionPolicyCommand{
		Id:        cfg.ID.String(),
		Name:      cfg.Name,
		MaxAge:    cfg.MaxAge,
		MaxBytes:  cfg.MaxBytes,
		MaxChunks: cfg.MaxChunks,
	}
}

// NewPutRetentionPolicy creates a ConfigCommand for PutRetentionPolicy.
func NewPutRetentionPolicy(cfg system.RetentionPolicyConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutRetentionPolicy{
			PutRetentionPolicy: putRetentionPolicyCmd(cfg),
		},
	}
}

// NewDeleteRetentionPolicy creates a ConfigCommand for DeleteRetentionPolicy.
func NewDeleteRetentionPolicy(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteRetentionPolicy{
			DeleteRetentionPolicy: &gastrologv1.DeleteRetentionPolicyCommand{Id: id.String()},
		},
	}
}

// ExtractPutRetentionPolicy converts a PutRetentionPolicyCommand back to a RetentionPolicyConfig.
func ExtractPutRetentionPolicy(cmd *gastrologv1.PutRetentionPolicyCommand) (system.RetentionPolicyConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.RetentionPolicyConfig{}, fmt.Errorf("parse retention policy id: %w", err)
	}
	return system.RetentionPolicyConfig{
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

func putVaultCmd(cfg system.VaultConfig) *gastrologv1.PutVaultCommand {
	return &gastrologv1.PutVaultCommand{
		Id:      cfg.ID.String(),
		Name:    cfg.Name,
		Enabled: cfg.Enabled,
	}
}

// NewPutVault creates a ConfigCommand for PutVault.
func NewPutVault(cfg system.VaultConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutVault{PutVault: putVaultCmd(cfg)},
	}
}

// NewDeleteVault creates a ConfigCommand for DeleteVault.
func NewDeleteVault(id uuid.UUID, deleteData bool) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteVault{
			DeleteVault: &gastrologv1.DeleteVaultCommand{Id: id.String(), DeleteData: deleteData},
		},
	}
}

// ExtractPutVault converts a PutVaultCommand back to a VaultConfig.
func ExtractPutVault(cmd *gastrologv1.PutVaultCommand) (system.VaultConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.VaultConfig{}, fmt.Errorf("parse vault id: %w", err)
	}

	return system.VaultConfig{
		ID:      id,
		Name:    cmd.GetName(),
		Enabled: cmd.GetEnabled(),
	}, nil
}

// ExtractDeleteVault extracts the UUID from a DeleteVaultCommand.
func ExtractDeleteVault(cmd *gastrologv1.DeleteVaultCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Ingesters
// ---------------------------------------------------------------------------

func putIngesterCmd(cfg system.IngesterConfig) *gastrologv1.PutIngesterCommand {
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
func NewPutIngester(cfg system.IngesterConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutIngester{PutIngester: putIngesterCmd(cfg)},
	}
}

// NewDeleteIngester creates a ConfigCommand for DeleteIngester.
func NewDeleteIngester(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteIngester{
			DeleteIngester: &gastrologv1.DeleteIngesterCommand{Id: id.String()},
		},
	}
}

// ExtractPutIngester converts a PutIngesterCommand back to an IngesterConfig.
func ExtractPutIngester(cmd *gastrologv1.PutIngesterCommand) (system.IngesterConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.IngesterConfig{}, fmt.Errorf("parse ingester id: %w", err)
	}
	return system.IngesterConfig{
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
// Routes
// ---------------------------------------------------------------------------

func putRouteCmd(cfg system.RouteConfig) *gastrologv1.PutRouteCommand {
	dests := make([]string, len(cfg.Destinations))
	for i, d := range cfg.Destinations {
		dests[i] = d.String()
	}
	return &gastrologv1.PutRouteCommand{
		Id:             cfg.ID.String(),
		Name:           cfg.Name,
		FilterId:       uuidPtrToString(cfg.FilterID),
		DestinationIds: dests,
		Distribution:   string(cfg.Distribution),
		Enabled:        cfg.Enabled,
		EjectOnly:      cfg.EjectOnly,
	}
}

// NewPutRoute creates a ConfigCommand for PutRoute.
func NewPutRoute(cfg system.RouteConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutRoute{PutRoute: putRouteCmd(cfg)},
	}
}

// NewDeleteRoute creates a ConfigCommand for DeleteRoute.
func NewDeleteRoute(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteRoute{
			DeleteRoute: &gastrologv1.DeleteRouteCommand{Id: id.String()},
		},
	}
}

// ExtractPutRoute converts a PutRouteCommand back to a RouteConfig.
func ExtractPutRoute(cmd *gastrologv1.PutRouteCommand) (system.RouteConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.RouteConfig{}, fmt.Errorf("parse route id: %w", err)
	}
	filterID, err := parseOptionalUUID(cmd.GetFilterId())
	if err != nil {
		return system.RouteConfig{}, fmt.Errorf("parse route filter_id: %w", err)
	}
	var dests []uuid.UUID
	for _, d := range cmd.GetDestinationIds() {
		did, err := uuid.Parse(d)
		if err != nil {
			return system.RouteConfig{}, fmt.Errorf("parse route destination: %w", err)
		}
		dests = append(dests, did)
	}
	return system.RouteConfig{
		ID:           id,
		Name:         cmd.GetName(),
		FilterID:     filterID,
		Destinations: dests,
		Distribution: system.DistributionMode(cmd.GetDistribution()),
		Enabled:      cmd.GetEnabled(),
		EjectOnly:    cmd.GetEjectOnly(),
	}, nil
}

// ExtractDeleteRoute extracts the UUID from a DeleteRouteCommand.
func ExtractDeleteRoute(cmd *gastrologv1.DeleteRouteCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Lookup Files
// ---------------------------------------------------------------------------

func putManagedFileCmd(cfg system.ManagedFileConfig) *gastrologv1.PutManagedFileCommand {
	return &gastrologv1.PutManagedFileCommand{
		Id:         cfg.ID.String(),
		Name:       cfg.Name,
		Sha256:     cfg.SHA256,
		Size:       cfg.Size,
		UploadedAt: cfg.UploadedAt.Format(time.RFC3339Nano),
	}
}

// NewPutManagedFile creates a ConfigCommand for PutManagedFile.
func NewPutManagedFile(cfg system.ManagedFileConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutManagedFile{PutManagedFile: putManagedFileCmd(cfg)},
	}
}

// NewDeleteManagedFile creates a ConfigCommand for DeleteManagedFile.
func NewDeleteManagedFile(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteManagedFile{
			DeleteManagedFile: &gastrologv1.DeleteManagedFileCommand{Id: id.String()},
		},
	}
}

// ExtractPutManagedFile converts a PutManagedFileCommand back to a ManagedFileConfig.
func ExtractPutManagedFile(cmd *gastrologv1.PutManagedFileCommand) (system.ManagedFileConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.ManagedFileConfig{}, fmt.Errorf("parse managed file id: %w", err)
	}
	var uploadedAt time.Time
	if cmd.GetUploadedAt() != "" {
		uploadedAt, _ = time.Parse(time.RFC3339Nano, cmd.GetUploadedAt())
	}
	return system.ManagedFileConfig{
		ID:         id,
		Name:       cmd.GetName(),
		SHA256:     cmd.GetSha256(),
		Size:       cmd.GetSize(),
		UploadedAt: uploadedAt,
	}, nil
}

// ExtractDeleteManagedFile extracts the UUID from a DeleteManagedFileCommand.
func ExtractDeleteManagedFile(cmd *gastrologv1.DeleteManagedFileCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}

// ---------------------------------------------------------------------------
// Server Settings
// ---------------------------------------------------------------------------

// NewPutServerSettings creates a ConfigCommand for persisting server-level settings.
// The settings are serialized as JSON inside a PutSettingCommand with key="server"
// for wire/snapshot compatibility.
func NewPutServerSettings(ss system.ServerSettings) (*gastrologv1.SystemCommand, error) {
	blob, err := json.Marshal(ss)
	if err != nil {
		return nil, fmt.Errorf("marshal server settings: %w", err)
	}
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutSetting{
			PutSetting: &gastrologv1.PutSettingCommand{Key: "server", Value: string(blob)},
		},
	}, nil
}

// ExtractPutServerSettings parses server settings from a PutSettingCommand with key="server".
func ExtractPutServerSettings(value string) (system.ServerSettings, error) {
	var ss system.ServerSettings
	if err := json.Unmarshal([]byte(value), &ss); err != nil {
		return system.ServerSettings{}, fmt.Errorf("parse server settings: %w", err)
	}
	// Migrate flat password policy fields to nested PasswordPolicy if present.
	ss.Auth = migratePasswordPolicy(ss.Auth, value)
	return ss, nil
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
func migratePasswordPolicy(ac system.AuthConfig, raw string) system.AuthConfig {
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
		ac.PasswordPolicy = system.PasswordPolicy{
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

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

func putCertificateCmd(cert system.CertPEM) *gastrologv1.PutCertificateCommand {
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
func NewPutCertificate(cert system.CertPEM) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutCertificate{
			PutCertificate: putCertificateCmd(cert),
		},
	}
}

// NewDeleteCertificate creates a ConfigCommand for DeleteCertificate.
func NewDeleteCertificate(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteCertificate{
			DeleteCertificate: &gastrologv1.DeleteCertificateCommand{Id: id.String()},
		},
	}
}

// ExtractPutCertificate converts a PutCertificateCommand back to a CertPEM.
func ExtractPutCertificate(cmd *gastrologv1.PutCertificateCommand) (system.CertPEM, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.CertPEM{}, fmt.Errorf("parse certificate id: %w", err)
	}
	return system.CertPEM{
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

func createUserCmd(u system.User) *gastrologv1.CreateUserCommand {
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
func NewCreateUser(u system.User) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_CreateUser{CreateUser: createUserCmd(u)},
	}
}

// NewUpdatePassword creates a ConfigCommand for UpdatePassword.
func NewUpdatePassword(id uuid.UUID, passwordHash string) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_UpdatePassword{
			UpdatePassword: &gastrologv1.UpdatePasswordCommand{
				Id:           id.String(),
				PasswordHash: passwordHash,
			},
		},
	}
}

// NewUpdateUserRole creates a ConfigCommand for UpdateUserRole.
func NewUpdateUserRole(id uuid.UUID, role string) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_UpdateUserRole{
			UpdateUserRole: &gastrologv1.UpdateUserRoleCommand{
				Id:   id.String(),
				Role: role,
			},
		},
	}
}

// NewUpdateUsername creates a ConfigCommand for UpdateUsername.
func NewUpdateUsername(id uuid.UUID, username string) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_UpdateUsername{
			UpdateUsername: &gastrologv1.UpdateUsernameCommand{
				Id:       id.String(),
				Username: username,
			},
		},
	}
}

// NewDeleteUser creates a ConfigCommand for DeleteUser.
func NewDeleteUser(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteUser{
			DeleteUser: &gastrologv1.DeleteUserCommand{Id: id.String()},
		},
	}
}

// NewInvalidateTokens creates a ConfigCommand for InvalidateTokens.
func NewInvalidateTokens(id uuid.UUID, at time.Time) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_InvalidateTokens{
			InvalidateTokens: &gastrologv1.InvalidateTokensCommand{
				Id: id.String(),
				At: timestamppb.New(at),
			},
		},
	}
}

// NewPutUserPreferences creates a ConfigCommand for PutUserPreferences.
func NewPutUserPreferences(id uuid.UUID, prefs string) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutUserPreferences{
			PutUserPreferences: &gastrologv1.PutUserPreferencesCommand{
				Id:          id.String(),
				Preferences: prefs,
			},
		},
	}
}

// ExtractCreateUser converts a CreateUserCommand back to a User.
func ExtractCreateUser(cmd *gastrologv1.CreateUserCommand) (system.User, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.User{}, fmt.Errorf("parse user id: %w", err)
	}
	return system.User{
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

func createRefreshTokenCmd(t system.RefreshToken) *gastrologv1.CreateRefreshTokenCommand {
	return &gastrologv1.CreateRefreshTokenCommand{
		Id:        t.ID.String(),
		UserId:    t.UserID.String(),
		TokenHash: t.TokenHash,
		ExpiresAt: timestamppb.New(t.ExpiresAt),
		CreatedAt: timestamppb.New(t.CreatedAt),
	}
}

// NewCreateRefreshToken creates a ConfigCommand for CreateRefreshToken.
func NewCreateRefreshToken(t system.RefreshToken) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_CreateRefreshToken{
			CreateRefreshToken: createRefreshTokenCmd(t),
		},
	}
}

// NewDeleteRefreshToken creates a ConfigCommand for DeleteRefreshToken.
func NewDeleteRefreshToken(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteRefreshToken{
			DeleteRefreshToken: &gastrologv1.DeleteRefreshTokenCommand{Id: id.String()},
		},
	}
}

// NewDeleteUserRefreshTokens creates a ConfigCommand for DeleteUserRefreshTokens.
func NewDeleteUserRefreshTokens(userID uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteUserRefreshTokens{
			DeleteUserRefreshTokens: &gastrologv1.DeleteUserRefreshTokensCommand{
				UserId: userID.String(),
			},
		},
	}
}

// ExtractCreateRefreshToken converts a CreateRefreshTokenCommand back to a RefreshToken.
func ExtractCreateRefreshToken(cmd *gastrologv1.CreateRefreshTokenCommand) (system.RefreshToken, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.RefreshToken{}, fmt.Errorf("parse refresh token id: %w", err)
	}
	userID, err := uuid.Parse(cmd.GetUserId())
	if err != nil {
		return system.RefreshToken{}, fmt.Errorf("parse refresh token user id: %w", err)
	}
	return system.RefreshToken{
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

func putNodeConfigCmd(node system.NodeConfig) *gastrologv1.PutNodeConfigCommand {
	return &gastrologv1.PutNodeConfigCommand{
		Id:   node.ID.String(),
		Name: node.Name,
	}
}

// NewPutNodeConfig creates a ConfigCommand for PutNodeConfig.
func NewPutNodeConfig(node system.NodeConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutNodeConfig{PutNodeConfig: putNodeConfigCmd(node)},
	}
}

// NewDeleteNodeConfig creates a ConfigCommand for DeleteNodeConfig.
func NewDeleteNodeConfig(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteNodeConfig{
			DeleteNodeConfig: &gastrologv1.DeleteNodeConfigCommand{Id: id.String()},
		},
	}
}

// ExtractPutNodeConfig converts a PutNodeConfigCommand back to a NodeConfig.
func ExtractPutNodeConfig(cmd *gastrologv1.PutNodeConfigCommand) (system.NodeConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return system.NodeConfig{}, fmt.Errorf("parse node id: %w", err)
	}
	return system.NodeConfig{
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
func NewPutClusterTLS(tls system.ClusterTLS) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutClusterTls{
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
func ExtractPutClusterTLS(cmd *gastrologv1.PutClusterTLSCommand) system.ClusterTLS {
	return system.ClusterTLS{
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
func BuildSnapshot(sys *system.System, users []system.User, tokens []system.RefreshToken) *gastrologv1.SystemSnapshot {
	snap := &gastrologv1.SystemSnapshot{}
	cfg := &sys.Config
	rt := &sys.Runtime

	// Serialize server settings.
	blob, err := json.Marshal(system.ServerSettings{
		Auth:      cfg.Auth,
		Query:     cfg.Query,
		Scheduler: cfg.Scheduler,
		TLS:       cfg.TLS,
		Lookup:    cfg.Lookup,
		MaxMind:   cfg.MaxMind,
		Cluster:   cfg.Cluster,
	})
	if err == nil && string(blob) != "{}" {
		snap.Settings = map[string]string{"server": string(blob)}
	}

	// Config entities.
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
	for _, rt := range cfg.Routes {
		snap.Routes = append(snap.Routes, putRouteCmd(rt))
	}
	for _, lf := range cfg.ManagedFiles {
		snap.ManagedFiles = append(snap.ManagedFiles, putManagedFileCmd(lf))
	}
	for _, c := range cfg.Certs {
		snap.Certificates = append(snap.Certificates, putCertificateCmd(c))
	}
	for _, cs := range cfg.CloudServices {
		snap.CloudServices = append(snap.CloudServices, putCloudServiceCmd(cs))
	}
	for _, tier := range cfg.Tiers {
		snap.Tiers = append(snap.Tiers, putTierCmd(tier))
	}

	// Users and tokens.
	for _, u := range users {
		snap.Users = append(snap.Users, createUserCmd(u))
	}
	for _, t := range tokens {
		snap.RefreshTokens = append(snap.RefreshTokens, createRefreshTokenCmd(t))
	}

	// Runtime: nodes, storage, TLS.
	for _, n := range rt.Nodes {
		snap.NodeConfigs = append(snap.NodeConfigs, putNodeConfigCmd(n))
	}
	for _, nsc := range rt.NodeStorageConfigs {
		snap.NodeStorageConfigs = append(snap.NodeStorageConfigs, setNodeStorageConfigCmd(nsc))
	}
	if rt.ClusterTLS != nil {
		snap.ClusterTls = NewPutClusterTLS(*rt.ClusterTLS).GetPutClusterTls()
	}

	return snap
}

// RestoreSnapshot converts a ConfigSnapshot back to Go config types.
// If the snapshot's Settings map contains a "server" key, it is parsed
// into the Config's server-level fields (Auth, Query, etc.).
func RestoreSnapshot(snap *gastrologv1.SystemSnapshot) (*system.System, []system.User, []system.RefreshToken, error) { //nolint:gocognit,gocyclo // flat field mapping from snapshot proto
	sys := &system.System{}
	cfg := &sys.Config
	rt := &sys.Runtime

	// Migrate server settings from the Settings map.
	if settings := snap.GetSettings(); len(settings) > 0 {
		if raw, ok := settings["server"]; ok {
			ss, err := ExtractPutServerSettings(raw)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("restore server settings: %w", err)
			}
			cfg.Auth = ss.Auth
			cfg.Query = ss.Query
			cfg.Scheduler = ss.Scheduler
			cfg.TLS = ss.TLS
			cfg.Lookup = ss.Lookup
			cfg.MaxMind = ss.MaxMind
			cfg.Cluster = ss.Cluster
			// TODO(gastrolog-2kx4r): SetupWizardDismissed needs its own snapshot field
		}
	}

	for _, f := range snap.GetFilters() {
		fc, err := ExtractPutFilter(f)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore filter: %w", err)
		}
		cfg.Filters = append(cfg.Filters, fc)
	}
	for _, rp := range snap.GetRotationPolicies() {
		rc, err := ExtractPutRotationPolicy(rp)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore rotation policy: %w", err)
		}
		cfg.RotationPolicies = append(cfg.RotationPolicies, rc)
	}
	for _, rp := range snap.GetRetentionPolicies() {
		rc, err := ExtractPutRetentionPolicy(rp)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore retention policy: %w", err)
		}
		cfg.RetentionPolicies = append(cfg.RetentionPolicies, rc)
	}
	for _, v := range snap.GetVaults() {
		vc, err := ExtractPutVault(v)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore vault: %w", err)
		}
		cfg.Vaults = append(cfg.Vaults, vc)
	}
	for _, ing := range snap.GetIngesters() {
		ic, err := ExtractPutIngester(ing)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore ingester: %w", err)
		}
		cfg.Ingesters = append(cfg.Ingesters, ic)
	}
	for _, rt := range snap.GetRoutes() {
		rc, err := ExtractPutRoute(rt)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore route: %w", err)
		}
		cfg.Routes = append(cfg.Routes, rc)
	}
	for _, lf := range snap.GetManagedFiles() {
		lfc, err := ExtractPutManagedFile(lf)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore managed file: %w", err)
		}
		cfg.ManagedFiles = append(cfg.ManagedFiles, lfc)
	}
	for _, c := range snap.GetCertificates() {
		cc, err := ExtractPutCertificate(c)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore certificate: %w", err)
		}
		cfg.Certs = append(cfg.Certs, cc)
	}
	for _, cs := range snap.GetCloudServices() {
		svc, err := ExtractPutCloudService(cs)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore cloud service: %w", err)
		}
		cfg.CloudServices = append(cfg.CloudServices, svc)
	}
	for _, nsc := range snap.GetNodeStorageConfigs() {
		nc, err := ExtractSetNodeStorageConfig(nsc)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore node storage config: %w", err)
		}
		rt.NodeStorageConfigs = append(rt.NodeStorageConfigs, nc)
	}
	for _, tier := range snap.GetTiers() {
		tc, err := ExtractPutTier(tier)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore tier: %w", err)
		}
		cfg.Tiers = append(cfg.Tiers, tc)
	}

	users := make([]system.User, 0, len(snap.GetUsers()))
	for _, u := range snap.GetUsers() {
		user, err := ExtractCreateUser(u)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore user: %w", err)
		}
		users = append(users, user)
	}

	tokens := make([]system.RefreshToken, 0, len(snap.GetRefreshTokens()))
	for _, t := range snap.GetRefreshTokens() {
		token, err := ExtractCreateRefreshToken(t)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore refresh token: %w", err)
		}
		tokens = append(tokens, token)
	}

	for _, n := range snap.GetNodeConfigs() {
		node, err := ExtractPutNodeConfig(n)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("restore node: %w", err)
		}
		rt.Nodes = append(rt.Nodes, node)
	}

	// Restore ClusterTLS if present in snapshot.
	if snap.ClusterTls != nil {
		tls := ExtractPutClusterTLS(snap.ClusterTls)
		rt.ClusterTLS = &tls
	}

	return sys, users, tokens, nil
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
