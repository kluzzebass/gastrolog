package raftfsm

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/command"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

// applyCmd marshals a ConfigCommand and applies it to the FSM.
// Fails the test on marshal error or non-nil Apply result.
func applyCmd(t *testing.T, fsm *FSM, cmd *gastrologv1.ConfigCommand) {
	t.Helper()
	data, err := command.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	result := fsm.Apply(&raft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
}

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func TestApplyPutFilter(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutFilter(config.FilterConfig{
		ID: id, Name: "test-filter", Expression: "env=prod",
	}))

	got, err := fsm.Store().GetFilter(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "test-filter" || got.Expression != "env=prod" {
		t.Fatalf("unexpected filter: %+v", got)
	}
}

func TestApplyDeleteFilter(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutFilter(config.FilterConfig{ID: id, Name: "f"}))
	applyCmd(t, fsm, command.NewDeleteFilter(id))

	got, err := fsm.Store().GetFilter(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutRotationPolicy(t *testing.T) {
	fsm := New()
	id := newID()
	maxBytes := "64MB"
	applyCmd(t, fsm, command.NewPutRotationPolicy(config.RotationPolicyConfig{
		ID: id, Name: "rp", MaxBytes: &maxBytes,
	}))

	got, err := fsm.Store().GetRotationPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "rp" || got.MaxBytes == nil || *got.MaxBytes != "64MB" {
		t.Fatalf("unexpected rotation policy: %+v", got)
	}
}

func TestApplyDeleteRotationPolicy(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutRotationPolicy(config.RotationPolicyConfig{ID: id, Name: "rp"}))
	applyCmd(t, fsm, command.NewDeleteRotationPolicy(id))

	got, err := fsm.Store().GetRotationPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutRetentionPolicy(t *testing.T) {
	fsm := New()
	id := newID()
	maxAge := "720h"
	applyCmd(t, fsm, command.NewPutRetentionPolicy(config.RetentionPolicyConfig{
		ID: id, Name: "ret", MaxAge: &maxAge,
	}))

	got, err := fsm.Store().GetRetentionPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "ret" || got.MaxAge == nil || *got.MaxAge != "720h" {
		t.Fatalf("unexpected retention policy: %+v", got)
	}
}

func TestApplyDeleteRetentionPolicy(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutRetentionPolicy(config.RetentionPolicyConfig{ID: id, Name: "ret"}))
	applyCmd(t, fsm, command.NewDeleteRetentionPolicy(id))

	got, err := fsm.Store().GetRetentionPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutVault(t *testing.T) {
	fsm := New()
	id := newID()
	filterID := newID()
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{
		ID: id, Name: "vault", Type: "file", Filter: &filterID, Enabled: true,
		Params: map[string]string{"path": "/data"},
	}))

	got, err := fsm.Store().GetVault(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "vault" || !got.Enabled || got.Params["path"] != "/data" {
		t.Fatalf("unexpected vault: %+v", got)
	}
}

func TestApplyDeleteVault(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{ID: id, Name: "v", Type: "file"}))
	applyCmd(t, fsm, command.NewDeleteVault(id))

	got, err := fsm.Store().GetVault(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutIngester(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutIngester(config.IngesterConfig{
		ID: id, Name: "ing", Type: "syslog-udp", Enabled: true,
		Params: map[string]string{"port": "514"},
	}))

	got, err := fsm.Store().GetIngester(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "ing" || got.Type != "syslog-udp" || got.Params["port"] != "514" {
		t.Fatalf("unexpected ingester: %+v", got)
	}
}

func TestApplyDeleteIngester(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutIngester(config.IngesterConfig{ID: id, Name: "ing", Type: "syslog-udp"}))
	applyCmd(t, fsm, command.NewDeleteIngester(id))

	got, err := fsm.Store().GetIngester(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutServerSettings(t *testing.T) {
	fsm := New()
	cmd, err := command.NewPutServerSettings(config.ServerSettings{
		Auth:      config.AuthConfig{JWTSecret: "test-secret"},
		Scheduler: config.SchedulerConfig{MaxConcurrentJobs: 4},
	})
	if err != nil {
		t.Fatalf("NewPutServerSettings: %v", err)
	}
	applyCmd(t, fsm, cmd)

	ss, err := fsm.Store().LoadServerSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ss.Auth.JWTSecret != "test-secret" {
		t.Fatalf("JWTSecret: got %q, want %q", ss.Auth.JWTSecret, "test-secret")
	}
	if ss.Scheduler.MaxConcurrentJobs != 4 {
		t.Fatalf("MaxConcurrentJobs: got %d, want 4", ss.Scheduler.MaxConcurrentJobs)
	}
}

func TestApplyPutCertificate(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCertificate(config.CertPEM{
		ID: id, Name: "cert", CertPEM: "CERT", KeyPEM: "KEY",
	}))

	got, err := fsm.Store().GetCertificate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "cert" || got.CertPEM != "CERT" {
		t.Fatalf("unexpected cert: %+v", got)
	}
}

func TestApplyDeleteCertificate(t *testing.T) {
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCertificate(config.CertPEM{ID: id, Name: "cert"}))
	applyCmd(t, fsm, command.NewDeleteCertificate(id))

	got, err := fsm.Store().GetCertificate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyCreateUser(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "admin",
		CreatedAt: now, UpdatedAt: now,
	}))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Username != "alice" || got.Role != "admin" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestApplyUpdatePassword(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "old", Role: "admin",
		CreatedAt: now, UpdatedAt: now,
	}))
	applyCmd(t, fsm, command.NewUpdatePassword(id, "new"))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.PasswordHash != "new" {
		t.Fatalf("expected password 'new', got %q", got.PasswordHash)
	}
}

func TestApplyUpdateUserRole(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))
	applyCmd(t, fsm, command.NewUpdateUserRole(id, "admin"))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != "admin" {
		t.Fatalf("expected role 'admin', got %q", got.Role)
	}
}

func TestApplyUpdateUsername(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))
	applyCmd(t, fsm, command.NewUpdateUsername(id, "bob"))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != "bob" {
		t.Fatalf("expected username 'bob', got %q", got.Username)
	}
}

func TestApplyDeleteUser(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))
	applyCmd(t, fsm, command.NewDeleteUser(id))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyInvalidateTokens(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))

	invalidateAt := now.Add(time.Hour)
	applyCmd(t, fsm, command.NewInvalidateTokens(id, invalidateAt))

	got, err := fsm.Store().GetUser(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.TokenInvalidatedAt.Equal(invalidateAt) {
		t.Fatalf("expected TokenInvalidatedAt %v, got %v", invalidateAt, got.TokenInvalidatedAt)
	}
}

func TestApplyPutUserPreferences(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(config.User{
		ID: id, Username: "alice", PasswordHash: "hash", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))
	applyCmd(t, fsm, command.NewPutUserPreferences(id, `{"theme":"dark"}`))

	got, err := fsm.Store().GetUserPreferences(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != `{"theme":"dark"}` {
		t.Fatalf("unexpected preferences: %v", got)
	}
}

func TestApplyCreateRefreshToken(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateRefreshToken(config.RefreshToken{
		ID: id, UserID: newID(), TokenHash: "hash123",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))

	got, err := fsm.Store().GetRefreshTokenByHash(context.Background(), "hash123")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != id {
		t.Fatalf("unexpected refresh token: %+v", got)
	}
}

func TestApplyDeleteRefreshToken(t *testing.T) {
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateRefreshToken(config.RefreshToken{
		ID: id, UserID: newID(), TokenHash: "hash456",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))
	applyCmd(t, fsm, command.NewDeleteRefreshToken(id))

	got, err := fsm.Store().GetRefreshTokenByHash(context.Background(), "hash456")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyDeleteUserRefreshTokens(t *testing.T) {
	fsm := New()
	userID := newID()
	now := time.Now().UTC().Truncate(time.Second)

	applyCmd(t, fsm, command.NewCreateRefreshToken(config.RefreshToken{
		ID: newID(), UserID: userID, TokenHash: "tok1",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))
	applyCmd(t, fsm, command.NewCreateRefreshToken(config.RefreshToken{
		ID: newID(), UserID: userID, TokenHash: "tok2",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))

	applyCmd(t, fsm, command.NewDeleteUserRefreshTokens(userID))

	for _, hash := range []string{"tok1", "tok2"} {
		got, err := fsm.Store().GetRefreshTokenByHash(context.Background(), hash)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("expected nil for %q, got %+v", hash, got)
		}
	}
}

// TestCompoundDeleteRotationPolicy verifies the cascade: deleting a rotation
// policy clears the Policy reference on vaults that used it.
func TestCompoundDeleteRotationPolicy(t *testing.T) {
	fsm := New()

	policyID := newID()
	otherPolicyID := newID()
	vault1 := newID()
	vault2 := newID()
	vault3 := newID()

	// Create policies.
	applyCmd(t, fsm, command.NewPutRotationPolicy(config.RotationPolicyConfig{ID: policyID, Name: "target"}))
	applyCmd(t, fsm, command.NewPutRotationPolicy(config.RotationPolicyConfig{ID: otherPolicyID, Name: "other"}))

	// Create vaults: vault1 and vault2 reference the target policy, vault3 references the other.
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{ID: vault1, Name: "v1", Type: "file", Policy: &policyID}))
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{ID: vault2, Name: "v2", Type: "file", Policy: &policyID}))
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{ID: vault3, Name: "v3", Type: "file", Policy: &otherPolicyID}))

	// Delete the target policy.
	applyCmd(t, fsm, command.NewDeleteRotationPolicy(policyID))

	ctx := context.Background()

	// vault1 and vault2 should have nil Policy.
	for _, id := range []uuid.UUID{vault1, vault2} {
		v, err := fsm.Store().GetVault(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if v.Policy != nil {
			t.Errorf("vault %s still has policy %s", v.Name, v.Policy)
		}
	}

	// vault3 should still reference the other policy.
	v3, err := fsm.Store().GetVault(ctx, vault3)
	if err != nil {
		t.Fatal(err)
	}
	if v3.Policy == nil || *v3.Policy != otherPolicyID {
		t.Errorf("vault3 policy should be %s, got %v", otherPolicyID, v3.Policy)
	}
}

// TestCompoundDeleteRetentionPolicy verifies the cascade: deleting a retention
// policy removes matching retention rules from vaults.
func TestCompoundDeleteRetentionPolicy(t *testing.T) {
	fsm := New()

	policyID := newID()
	otherPolicyID := newID()
	vaultID := newID()

	// Create policies.
	applyCmd(t, fsm, command.NewPutRetentionPolicy(config.RetentionPolicyConfig{ID: policyID, Name: "target"}))
	applyCmd(t, fsm, command.NewPutRetentionPolicy(config.RetentionPolicyConfig{ID: otherPolicyID, Name: "other"}))

	// Create vault with two retention rules: one referencing each policy.
	applyCmd(t, fsm, command.NewPutVault(config.VaultConfig{
		ID: vaultID, Name: "vault", Type: "file",
		RetentionRules: []config.RetentionRule{
			{RetentionPolicyID: policyID, Action: config.RetentionActionExpire},
			{RetentionPolicyID: otherPolicyID, Action: config.RetentionActionExpire},
		},
	}))

	// Delete the target policy.
	applyCmd(t, fsm, command.NewDeleteRetentionPolicy(policyID))

	ctx := context.Background()
	v, err := fsm.Store().GetVault(ctx, vaultID)
	if err != nil {
		t.Fatal(err)
	}

	// Only the other policy rule should remain.
	if len(v.RetentionRules) != 1 {
		t.Fatalf("expected 1 retention rule, got %d", len(v.RetentionRules))
	}
	if v.RetentionRules[0].RetentionPolicyID != otherPolicyID {
		t.Errorf("remaining rule should reference %s, got %s", otherPolicyID, v.RetentionRules[0].RetentionPolicyID)
	}
}

// TestSnapshotRestore verifies the full round-trip: populate an FSM, snapshot,
// restore into a new FSM, and verify identical state.
func TestSnapshotRestore(t *testing.T) {
	fsm1 := New()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// Populate with various entities.
	filterID := newID()
	applyCmd(t, fsm1, command.NewPutFilter(config.FilterConfig{ID: filterID, Name: "f1", Expression: "*"}))

	maxAge := "1h"
	rpID := newID()
	applyCmd(t, fsm1, command.NewPutRotationPolicy(config.RotationPolicyConfig{ID: rpID, Name: "rp1", MaxAge: &maxAge}))

	retMaxAge := "720h"
	retID := newID()
	applyCmd(t, fsm1, command.NewPutRetentionPolicy(config.RetentionPolicyConfig{ID: retID, Name: "ret1", MaxAge: &retMaxAge}))

	vaultID := newID()
	applyCmd(t, fsm1, command.NewPutVault(config.VaultConfig{
		ID: vaultID, Name: "vault1", Type: "file",
		Filter: &filterID, Policy: &rpID, Enabled: true,
		Params: map[string]string{"path": "/data"},
		RetentionRules: []config.RetentionRule{
			{RetentionPolicyID: retID, Action: config.RetentionActionExpire},
		},
	}))

	ingID := newID()
	applyCmd(t, fsm1, command.NewPutIngester(config.IngesterConfig{
		ID: ingID, Name: "ing1", Type: "syslog-udp", Enabled: true,
		Params: map[string]string{"port": "514"},
	}))

	settingsCmd, err := command.NewPutServerSettings(config.ServerSettings{})
	if err != nil {
		t.Fatalf("NewPutServerSettings: %v", err)
	}
	applyCmd(t, fsm1, settingsCmd)

	certID := newID()
	applyCmd(t, fsm1, command.NewPutCertificate(config.CertPEM{
		ID: certID, Name: "cert1", CertPEM: "CERT", KeyPEM: "KEY",
	}))

	userID := newID()
	applyCmd(t, fsm1, command.NewCreateUser(config.User{
		ID: userID, Username: "alice", PasswordHash: "hash", Role: "admin",
		CreatedAt: now, UpdatedAt: now,
	}))

	tokenID := newID()
	applyCmd(t, fsm1, command.NewCreateRefreshToken(config.RefreshToken{
		ID: tokenID, UserID: userID, TokenHash: "snap-hash",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))

	// Take snapshot.
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Persist to buffer.
	var buf bytes.Buffer
	sink := &bufSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into new FSM.
	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify all entities.
	gotFilter, _ := fsm2.Store().GetFilter(ctx, filterID)
	if gotFilter == nil || gotFilter.Name != "f1" {
		t.Errorf("filter: %+v", gotFilter)
	}

	gotRP, _ := fsm2.Store().GetRotationPolicy(ctx, rpID)
	if gotRP == nil || gotRP.Name != "rp1" {
		t.Errorf("rotation policy: %+v", gotRP)
	}

	gotRet, _ := fsm2.Store().GetRetentionPolicy(ctx, retID)
	if gotRet == nil || gotRet.Name != "ret1" {
		t.Errorf("retention policy: %+v", gotRet)
	}

	gotVault, _ := fsm2.Store().GetVault(ctx, vaultID)
	if gotVault == nil || gotVault.Name != "vault1" || !gotVault.Enabled {
		t.Errorf("vault: %+v", gotVault)
	}
	if gotVault != nil && len(gotVault.RetentionRules) != 1 {
		t.Errorf("vault retention rules: %+v", gotVault.RetentionRules)
	}

	gotIng, _ := fsm2.Store().GetIngester(ctx, ingID)
	if gotIng == nil || gotIng.Name != "ing1" {
		t.Errorf("ingester: %+v", gotIng)
	}

	// Server settings were saved — verify they can be loaded.
	if _, ssErr := fsm2.Store().LoadServerSettings(ctx); ssErr != nil {
		t.Errorf("LoadServerSettings: %v", ssErr)
	}

	gotCert, _ := fsm2.Store().GetCertificate(ctx, certID)
	if gotCert == nil || gotCert.Name != "cert1" {
		t.Errorf("cert: %+v", gotCert)
	}

	gotUser, _ := fsm2.Store().GetUser(ctx, userID)
	if gotUser == nil || gotUser.Username != "alice" {
		t.Errorf("user: %+v", gotUser)
	}

	gotToken, _ := fsm2.Store().GetRefreshTokenByHash(ctx, "snap-hash")
	if gotToken == nil || gotToken.ID != tokenID {
		t.Errorf("refresh token: %+v", gotToken)
	}
}

// TestApplyAfterRestore verifies Apply continues to work after Restore.
func TestApplyAfterRestore(t *testing.T) {
	fsm1 := New()
	now := time.Now().UTC().Truncate(time.Second)

	applyCmd(t, fsm1, command.NewPutFilter(config.FilterConfig{ID: newID(), Name: "pre-snap", Expression: "*"}))
	applyCmd(t, fsm1, command.NewCreateUser(config.User{
		ID: newID(), Username: "pre", PasswordHash: "h", Role: "user",
		CreatedAt: now, UpdatedAt: now,
	}))

	// Snapshot + Restore.
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}

	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}

	// Apply new commands after restore.
	newFilterID := newID()
	applyCmd(t, fsm2, command.NewPutFilter(config.FilterConfig{
		ID: newFilterID, Name: "post-snap", Expression: "env=staging",
	}))

	got, err := fsm2.Store().GetFilter(context.Background(), newFilterID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "post-snap" {
		t.Fatalf("post-restore apply failed: %+v", got)
	}
}

// TestSnapshotEmptyStore verifies snapshot works on an empty store.
func TestSnapshotEmptyStore(t *testing.T) {
	fsm1 := New()

	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	cfg, err := fsm2.Store().Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config from empty snapshot, got %+v", cfg)
	}
}

// bufSink is a test raft.SnapshotSink backed by a bytes.Buffer.
type bufSink struct {
	buf *bytes.Buffer
}

func (s *bufSink) Write(p []byte) (n int, err error) { return s.buf.Write(p) }
func (s *bufSink) Close() error                      { return nil }
func (s *bufSink) Cancel() error                     { return nil }
func (s *bufSink) ID() string                        { return "test" }
