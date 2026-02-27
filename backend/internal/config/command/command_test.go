package command

import (
	"reflect"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// ptr returns a pointer to v.
func ptr[T any](v T) *T { return &v }

// ---------------------------------------------------------------------------
// Command round-trip tests
// ---------------------------------------------------------------------------

func TestPutFilter(t *testing.T) {
	want := config.FilterConfig{
		ID:         uuid.Must(uuid.NewV7()),
		Name:       "prod-errors",
		Expression: "env=prod AND level=error",
	}
	cmd := NewPutFilter(want)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	inner := cmd2.GetPutFilter()
	if inner == nil {
		t.Fatal("expected PutFilter variant")
	}
	got, err := ExtractPutFilter(inner)
	if err != nil {
		t.Fatalf("ExtractPutFilter: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDeleteFilter(t *testing.T) {
	want := uuid.Must(uuid.NewV7())
	cmd := NewDeleteFilter(want)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, err := ExtractDeleteFilter(cmd2.GetDeleteFilter())
	if err != nil {
		t.Fatalf("ExtractDeleteFilter: %v", err)
	}
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestPutRotationPolicy(t *testing.T) {
	want := config.RotationPolicyConfig{
		ID:         uuid.Must(uuid.NewV7()),
		Name:       "default-rotation",
		MaxBytes:   ptr("64MB"),
		MaxAge:     ptr("1h"),
		MaxRecords: ptr(int64(100000)),
		Cron:       ptr("0 * * * *"),
	}
	got := roundTripCommand(t, NewPutRotationPolicy(want), func(cmd *gastrologv1.ConfigCommand) (config.RotationPolicyConfig, error) {
		return ExtractPutRotationPolicy(cmd.GetPutRotationPolicy())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutRotationPolicyNilOptionals(t *testing.T) {
	want := config.RotationPolicyConfig{
		ID:   uuid.Must(uuid.NewV7()),
		Name: "minimal",
	}
	got := roundTripCommand(t, NewPutRotationPolicy(want), func(cmd *gastrologv1.ConfigCommand) (config.RotationPolicyConfig, error) {
		return ExtractPutRotationPolicy(cmd.GetPutRotationPolicy())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutRetentionPolicy(t *testing.T) {
	want := config.RetentionPolicyConfig{
		ID:        uuid.Must(uuid.NewV7()),
		Name:      "long-term",
		MaxAge:    ptr("720h"),
		MaxBytes:  ptr("10GB"),
		MaxChunks: ptr(int64(500)),
	}
	got := roundTripCommand(t, NewPutRetentionPolicy(want), func(cmd *gastrologv1.ConfigCommand) (config.RetentionPolicyConfig, error) {
		return ExtractPutRetentionPolicy(cmd.GetPutRetentionPolicy())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutRetentionPolicyNilOptionals(t *testing.T) {
	want := config.RetentionPolicyConfig{
		ID:   uuid.Must(uuid.NewV7()),
		Name: "empty",
	}
	got := roundTripCommand(t, NewPutRetentionPolicy(want), func(cmd *gastrologv1.ConfigCommand) (config.RetentionPolicyConfig, error) {
		return ExtractPutRetentionPolicy(cmd.GetPutRetentionPolicy())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutVault(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())
	retPolicyID := uuid.Must(uuid.NewV7())
	destID := uuid.Must(uuid.NewV7())

	want := config.VaultConfig{
		ID:     uuid.Must(uuid.NewV7()),
		Name:   "production",
		Type:   "file",
		Filter: &filterID,
		Policy: &policyID,
		RetentionRules: []config.RetentionRule{
			{RetentionPolicyID: retPolicyID, Action: config.RetentionActionExpire},
			{RetentionPolicyID: retPolicyID, Action: config.RetentionActionMigrate, Destination: &destID},
		},
		Enabled: true,
		Params:  map[string]string{"path": "/data/vault"},
	}
	got := roundTripCommand(t, NewPutVault(want), func(cmd *gastrologv1.ConfigCommand) (config.VaultConfig, error) {
		return ExtractPutVault(cmd.GetPutVault())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutVaultNilOptionals(t *testing.T) {
	want := config.VaultConfig{
		ID:   uuid.Must(uuid.NewV7()),
		Name: "bare",
		Type: "memory",
	}
	got := roundTripCommand(t, NewPutVault(want), func(cmd *gastrologv1.ConfigCommand) (config.VaultConfig, error) {
		return ExtractPutVault(cmd.GetPutVault())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutIngester(t *testing.T) {
	want := config.IngesterConfig{
		ID:      uuid.Must(uuid.NewV7()),
		Name:    "syslog-prod",
		Type:    "syslog-udp",
		Enabled: true,
		Params:  map[string]string{"addr": ":514"},
	}
	got := roundTripCommand(t, NewPutIngester(want), func(cmd *gastrologv1.ConfigCommand) (config.IngesterConfig, error) {
		return ExtractPutIngester(cmd.GetPutIngester())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPutServerSettings(t *testing.T) {
	wantAuth := config.AuthConfig{JWTSecret: "s3cret"}
	wantSched := config.SchedulerConfig{MaxConcurrentJobs: 4}
	cmd, err := NewPutServerSettings(wantAuth, config.QueryConfig{}, wantSched, config.TLSConfig{}, config.LookupConfig{}, false)
	if err != nil {
		t.Fatalf("NewPutServerSettings: %v", err)
	}
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// The command is serialized as a PutSetting with key="server".
	_, value := ExtractPutSetting(cmd2.GetPutSetting())
	gotAuth, _, gotSched, _, _, _, err := ExtractPutServerSettings(value)
	if err != nil {
		t.Fatalf("ExtractPutServerSettings: %v", err)
	}
	if gotAuth.JWTSecret != wantAuth.JWTSecret {
		t.Fatalf("JWTSecret: got %q, want %q", gotAuth.JWTSecret, wantAuth.JWTSecret)
	}
	if gotSched.MaxConcurrentJobs != wantSched.MaxConcurrentJobs {
		t.Fatalf("MaxConcurrentJobs: got %d, want %d", gotSched.MaxConcurrentJobs, wantSched.MaxConcurrentJobs)
	}
}

func TestPutCertificate(t *testing.T) {
	want := config.CertPEM{
		ID:       uuid.Must(uuid.NewV7()),
		Name:     "main-cert",
		CertPEM:  "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
		KeyPEM:   "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----",
		CertFile: "/etc/ssl/cert.pem",
		KeyFile:  "/etc/ssl/key.pem",
	}
	got := roundTripCommand(t, NewPutCertificate(want), func(cmd *gastrologv1.ConfigCommand) (config.CertPEM, error) {
		return ExtractPutCertificate(cmd.GetPutCertificate())
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestCreateUser(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := config.User{
		ID:                 uuid.Must(uuid.NewV7()),
		Username:           "admin",
		PasswordHash:       "$2a$10$fakehash",
		Role:               "admin",
		Preferences:        `{"theme":"dark"}`,
		TokenInvalidatedAt: now.Add(-time.Hour),
		CreatedAt:          now.Add(-24 * time.Hour),
		UpdatedAt:          now,
	}
	got := roundTripCommand(t, NewCreateUser(want), func(cmd *gastrologv1.ConfigCommand) (config.User, error) {
		return ExtractCreateUser(cmd.GetCreateUser())
	})
	assertTimeEqual(t, "TokenInvalidatedAt", want.TokenInvalidatedAt, got.TokenInvalidatedAt)
	assertTimeEqual(t, "CreatedAt", want.CreatedAt, got.CreatedAt)
	assertTimeEqual(t, "UpdatedAt", want.UpdatedAt, got.UpdatedAt)

	// Compare non-time fields.
	want.TokenInvalidatedAt = got.TokenInvalidatedAt
	want.CreatedAt = got.CreatedAt
	want.UpdatedAt = got.UpdatedAt
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestCreateUserZeroTime(t *testing.T) {
	want := config.User{
		ID:           uuid.Must(uuid.NewV7()),
		Username:     "nobody",
		PasswordHash: "$2a$10$hash",
		Role:         "user",
	}
	got := roundTripCommand(t, NewCreateUser(want), func(cmd *gastrologv1.ConfigCommand) (config.User, error) {
		return ExtractCreateUser(cmd.GetCreateUser())
	})
	if got.Username != want.Username || got.Role != want.Role {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestUpdatePassword(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	hash := "$2a$10$newhash"
	cmd := NewUpdatePassword(id, hash)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	gotID, gotHash, err := ExtractUpdatePassword(cmd2.GetUpdatePassword())
	if err != nil {
		t.Fatalf("ExtractUpdatePassword: %v", err)
	}
	if gotID != id || gotHash != hash {
		t.Fatalf("got (%v, %q), want (%v, %q)", gotID, gotHash, id, hash)
	}
}

func TestUpdateUserRole(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	cmd := NewUpdateUserRole(id, "admin")
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	gotID, gotRole, err := ExtractUpdateUserRole(cmd2.GetUpdateUserRole())
	if err != nil {
		t.Fatalf("ExtractUpdateUserRole: %v", err)
	}
	if gotID != id || gotRole != "admin" {
		t.Fatalf("got (%v, %q), want (%v, %q)", gotID, gotRole, id, "admin")
	}
}

func TestUpdateUsername(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	cmd := NewUpdateUsername(id, "newname")
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	gotID, gotName, err := ExtractUpdateUsername(cmd2.GetUpdateUsername())
	if err != nil {
		t.Fatalf("ExtractUpdateUsername: %v", err)
	}
	if gotID != id || gotName != "newname" {
		t.Fatalf("got (%v, %q), want (%v, %q)", gotID, gotName, id, "newname")
	}
}

func TestInvalidateTokens(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	at := time.Now().UTC().Truncate(time.Microsecond)
	cmd := NewInvalidateTokens(id, at)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	gotID, gotAt, err := ExtractInvalidateTokens(cmd2.GetInvalidateTokens())
	if err != nil {
		t.Fatalf("ExtractInvalidateTokens: %v", err)
	}
	if gotID != id {
		t.Fatalf("id: got %v, want %v", gotID, id)
	}
	assertTimeEqual(t, "at", at, gotAt)
}

func TestPutUserPreferences(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	prefs := `{"theme":"light","syntax":"monokai"}`
	cmd := NewPutUserPreferences(id, prefs)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	gotID, gotPrefs, err := ExtractPutUserPreferences(cmd2.GetPutUserPreferences())
	if err != nil {
		t.Fatalf("ExtractPutUserPreferences: %v", err)
	}
	if gotID != id || gotPrefs != prefs {
		t.Fatalf("got (%v, %q), want (%v, %q)", gotID, gotPrefs, id, prefs)
	}
}

func TestCreateRefreshToken(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := config.RefreshToken{
		ID:        uuid.Must(uuid.NewV7()),
		UserID:    uuid.Must(uuid.NewV7()),
		TokenHash: "sha256:abc123",
		ExpiresAt: now.Add(7 * 24 * time.Hour),
		CreatedAt: now,
	}
	got := roundTripCommand(t, NewCreateRefreshToken(want), func(cmd *gastrologv1.ConfigCommand) (config.RefreshToken, error) {
		return ExtractCreateRefreshToken(cmd.GetCreateRefreshToken())
	})
	assertTimeEqual(t, "ExpiresAt", want.ExpiresAt, got.ExpiresAt)
	assertTimeEqual(t, "CreatedAt", want.CreatedAt, got.CreatedAt)
	want.ExpiresAt = got.ExpiresAt
	want.CreatedAt = got.CreatedAt
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDeleteRefreshToken(t *testing.T) {
	want := uuid.Must(uuid.NewV7())
	cmd := NewDeleteRefreshToken(want)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, err := ExtractDeleteRefreshToken(cmd2.GetDeleteRefreshToken())
	if err != nil {
		t.Fatalf("ExtractDeleteRefreshToken: %v", err)
	}
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDeleteUserRefreshTokens(t *testing.T) {
	want := uuid.Must(uuid.NewV7())
	cmd := NewDeleteUserRefreshTokens(want)
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, err := ExtractDeleteUserRefreshTokens(cmd2.GetDeleteUserRefreshTokens())
	if err != nil {
		t.Fatalf("ExtractDeleteUserRefreshTokens: %v", err)
	}
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Snapshot round-trip
// ---------------------------------------------------------------------------

func TestSnapshotRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)

	filterID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())
	retPolicyID := uuid.Must(uuid.NewV7())
	destVaultID := uuid.Must(uuid.NewV7())

	cfg := &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Name: "all", Expression: "*"},
		},
		RotationPolicies: []config.RotationPolicyConfig{
			{ID: policyID, Name: "hourly", MaxAge: ptr("1h"), MaxBytes: ptr("64MB")},
		},
		RetentionPolicies: []config.RetentionPolicyConfig{
			{ID: retPolicyID, Name: "30d", MaxAge: ptr("720h")},
		},
		Vaults: []config.VaultConfig{
			{
				ID:     uuid.Must(uuid.NewV7()),
				Name:   "main",
				Type:   "file",
				Filter: &filterID,
				Policy: &policyID,
				RetentionRules: []config.RetentionRule{
					{RetentionPolicyID: retPolicyID, Action: config.RetentionActionMigrate, Destination: &destVaultID},
				},
				Enabled: true,
				Params:  map[string]string{"path": "/data"},
			},
		},
		Ingesters: []config.IngesterConfig{
			{ID: uuid.Must(uuid.NewV7()), Name: "syslog", Type: "syslog-udp", Enabled: true, Params: map[string]string{"addr": ":514"}},
		},
		Auth: config.AuthConfig{JWTSecret: "test"},
		Certs: []config.CertPEM{
			{ID: uuid.Must(uuid.NewV7()), Name: "default", CertPEM: "cert", KeyPEM: "key"},
		},
	}

	users := []config.User{
		{
			ID:                 uuid.Must(uuid.NewV7()),
			Username:           "admin",
			PasswordHash:       "$2a$10$hash",
			Role:               "admin",
			Preferences:        `{"theme":"dark"}`,
			TokenInvalidatedAt: now.Add(-time.Hour),
			CreatedAt:          now.Add(-24 * time.Hour),
			UpdatedAt:          now,
		},
	}

	tokens := []config.RefreshToken{
		{
			ID:        uuid.Must(uuid.NewV7()),
			UserID:    users[0].ID,
			TokenHash: "sha256:tok",
			ExpiresAt: now.Add(7 * 24 * time.Hour),
			CreatedAt: now,
		},
	}

	snap := BuildSnapshot(cfg, users, tokens, nil)
	b, err := MarshalSnapshot(snap)
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	snap2, err := UnmarshalSnapshot(b)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	gotCfg, gotUsers, gotTokens, _, err := RestoreSnapshot(snap2)
	if err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	// Compare config (excluding time-sensitive comparisons handled below).
	if len(gotCfg.Filters) != len(cfg.Filters) {
		t.Fatalf("filters: got %d, want %d", len(gotCfg.Filters), len(cfg.Filters))
	}
	if !reflect.DeepEqual(gotCfg.Filters, cfg.Filters) {
		t.Fatalf("filters differ: got %+v, want %+v", gotCfg.Filters, cfg.Filters)
	}
	if !reflect.DeepEqual(gotCfg.RotationPolicies, cfg.RotationPolicies) {
		t.Fatalf("rotation policies differ")
	}
	if !reflect.DeepEqual(gotCfg.RetentionPolicies, cfg.RetentionPolicies) {
		t.Fatalf("retention policies differ")
	}
	if !reflect.DeepEqual(gotCfg.Vaults, cfg.Vaults) {
		t.Fatalf("vaults differ: got %+v, want %+v", gotCfg.Vaults, cfg.Vaults)
	}
	if !reflect.DeepEqual(gotCfg.Ingesters, cfg.Ingesters) {
		t.Fatalf("ingesters differ")
	}
	if gotCfg.Auth.JWTSecret != cfg.Auth.JWTSecret {
		t.Fatalf("auth differ: got %+v, want %+v", gotCfg.Auth, cfg.Auth)
	}
	if !reflect.DeepEqual(gotCfg.Certs, cfg.Certs) {
		t.Fatalf("certs differ")
	}

	// Users.
	if len(gotUsers) != len(users) {
		t.Fatalf("users: got %d, want %d", len(gotUsers), len(users))
	}
	for i := range users {
		if gotUsers[i].ID != users[i].ID {
			t.Fatalf("user[%d].ID: got %v, want %v", i, gotUsers[i].ID, users[i].ID)
		}
		if gotUsers[i].Username != users[i].Username {
			t.Fatalf("user[%d].Username: got %q, want %q", i, gotUsers[i].Username, users[i].Username)
		}
		assertTimeEqual(t, "user.TokenInvalidatedAt", users[i].TokenInvalidatedAt, gotUsers[i].TokenInvalidatedAt)
		assertTimeEqual(t, "user.CreatedAt", users[i].CreatedAt, gotUsers[i].CreatedAt)
		assertTimeEqual(t, "user.UpdatedAt", users[i].UpdatedAt, gotUsers[i].UpdatedAt)
	}

	// Tokens.
	if len(gotTokens) != len(tokens) {
		t.Fatalf("tokens: got %d, want %d", len(gotTokens), len(tokens))
	}
	for i := range tokens {
		if gotTokens[i].ID != tokens[i].ID {
			t.Fatalf("token[%d].ID: got %v, want %v", i, gotTokens[i].ID, tokens[i].ID)
		}
		assertTimeEqual(t, "token.ExpiresAt", tokens[i].ExpiresAt, gotTokens[i].ExpiresAt)
		assertTimeEqual(t, "token.CreatedAt", tokens[i].CreatedAt, gotTokens[i].CreatedAt)
	}
}

func TestSnapshotEmpty(t *testing.T) {
	cfg := &config.Config{}
	snap := BuildSnapshot(cfg, nil, nil, nil)
	b, err := MarshalSnapshot(snap)
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	snap2, err := UnmarshalSnapshot(b)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	gotCfg, gotUsers, gotTokens, _, err := RestoreSnapshot(snap2)
	if err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if len(gotCfg.Filters) != 0 || len(gotCfg.Vaults) != 0 || len(gotCfg.Ingesters) != 0 {
		t.Fatalf("expected empty config, got %+v", gotCfg)
	}
	if len(gotUsers) != 0 {
		t.Fatalf("expected no users, got %d", len(gotUsers))
	}
	if len(gotTokens) != 0 {
		t.Fatalf("expected no tokens, got %d", len(gotTokens))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// roundTripCommand marshals a ConfigCommand, unmarshals it, and extracts the
// inner value using the provided extractor function.
func roundTripCommand[T any](t *testing.T, cmd *gastrologv1.ConfigCommand, extract func(*gastrologv1.ConfigCommand) (T, error)) T {
	t.Helper()
	b, err := Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cmd2, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, err := extract(cmd2)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	return got
}

// assertTimeEqual checks that two times are equal within protobuf's nanosecond
// precision (proto timestamps have second + nanosecond, so sub-nanosecond
// differences from Go's time.Time are acceptable).
func assertTimeEqual(t *testing.T, field string, want, got time.Time) {
	t.Helper()
	// Protobuf timestamps have nanosecond precision, same as Go's time.Time.
	// Truncate to microsecond to handle any edge cases.
	if !want.Truncate(time.Microsecond).Equal(got.Truncate(time.Microsecond)) {
		t.Fatalf("%s: got %v, want %v", field, got, want)
	}
}
