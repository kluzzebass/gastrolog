package raftfsm

import (
	"bytes"
	"context"
	"gastrolog/internal/glid"
	"io"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"

	"github.com/hashicorp/raft"
)

// applyCmd marshals a ConfigCommand and applies it to the FSM.
// Fails the test on marshal error or non-nil Apply result.
func applyCmd(t *testing.T, fsm *FSM, cmd *gastrologv1.SystemCommand) {
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

func newID() glid.GLID { return glid.New() }

func TestApplyPutRotationPolicy(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	maxSize := "64MB"
	applyCmd(t, fsm, command.NewPutRotationPolicy(system.RotationPolicyConfig{
		ID: id, Name: "rp", MaxSize: &maxSize,
	}))

	got, err := fsm.Store().GetRotationPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "rp" || got.MaxSize == nil || *got.MaxSize != "64MB" {
		t.Fatalf("unexpected rotation policy: %+v", got)
	}
}

func TestApplyDeleteRotationPolicy(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutRotationPolicy(system.RotationPolicyConfig{ID: id, Name: "rp"}))
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
	t.Parallel()
	fsm := New()
	id := newID()
	maxAge := "720h"
	maxSize := "50GB"
	applyCmd(t, fsm, command.NewPutRetentionPolicy(system.RetentionPolicyConfig{
		ID: id, Name: "ret", MaxAge: &maxAge, MaxSize: &maxSize,
	}))

	got, err := fsm.Store().GetRetentionPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "ret" || got.MaxAge == nil || *got.MaxAge != "720h" {
		t.Fatalf("unexpected retention policy: %+v", got)
	}
	// max_size (the combined drain-and-refuse bound) must survive the FSM
	// apply path too.
	if got.MaxSize == nil || *got.MaxSize != "50GB" {
		t.Fatalf("unexpected retention policy max size: %+v", got.MaxSize)
	}
}

func TestApplyDeleteRetentionPolicy(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutRetentionPolicy(system.RetentionPolicyConfig{ID: id, Name: "ret"}))
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
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{
		ID: id, Name: "vault", Enabled: true,
	}))

	got, err := fsm.Store().GetVault(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "vault" || !got.Enabled {
		t.Fatalf("unexpected vault: %+v", got)
	}
}

func TestApplyDeleteVault(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{ID: id, Name: "v"}))
	applyCmd(t, fsm, command.NewDeleteVault(id, false))

	got, err := fsm.Store().GetVault(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplyPutIngester(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutIngester(system.IngesterConfig{
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
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutIngester(system.IngesterConfig{ID: id, Name: "ing"}))
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
	t.Parallel()
	fsm := New()
	cmd, err := command.NewPutServerSettings(system.ServerSettings{
		Auth:      system.AuthConfig{JWTSecret: "test-secret"},
		Scheduler: system.SchedulerConfig{MaxConcurrentJobs: 4},
	}, "")
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
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCertificate(system.CertPEM{
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
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCertificate(system.CertPEM{ID: id, Name: "cert"}))
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateUser(system.User{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateRefreshToken(system.RefreshToken{
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
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewCreateRefreshToken(system.RefreshToken{
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
	t.Parallel()
	fsm := New()
	userID := newID()
	now := time.Now().UTC().Truncate(time.Second)

	applyCmd(t, fsm, command.NewCreateRefreshToken(system.RefreshToken{
		ID: newID(), UserID: userID, TokenHash: "tok1",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now,
	}))
	applyCmd(t, fsm, command.NewCreateRefreshToken(system.RefreshToken{
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

func TestApplyPutCloudService(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCloudService(system.CloudService{
		ID: id, Name: "prod-s3", Provider: "aws", Bucket: "my-bucket",
		Region: "us-east-1",
	}))

	got, err := fsm.Store().GetCloudService(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "prod-s3" || got.Provider != "aws" || got.Bucket != "my-bucket" {
		t.Fatalf("unexpected cloud service: %+v", got)
	}
}

func TestApplyDeleteCloudService(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	applyCmd(t, fsm, command.NewPutCloudService(system.CloudService{
		ID: id, Name: "svc", Provider: "aws", Bucket: "b",
	}))
	applyCmd(t, fsm, command.NewDeleteCloudService(id))

	got, err := fsm.Store().GetCloudService(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestApplySetNodeStorageConfig(t *testing.T) {
	t.Parallel()
	fsm := New()
	storageID := newID()
	applyCmd(t, fsm, command.NewSetNodeStorageConfig(system.NodeStorageConfig{
		NodeID: "node-1",
		FileStorages: []system.FileStorage{
			{ID: storageID, StorageClass: 1, Name: "fast", Path: "/data/fast"},
		},
	}))

	got, err := fsm.Store().GetNodeStorageConfig(context.Background(), "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.NodeID != "node-1" {
		t.Fatalf("unexpected node storage config: %+v", got)
	}
	if len(got.FileStorages) != 1 || got.FileStorages[0].Name != "fast" || got.FileStorages[0].StorageClass != 1 {
		t.Fatalf("unexpected file storages: %+v", got.FileStorages)
	}
}

// TestCompoundDeleteRotationPolicy verifies the cascade: deleting a rotation
// policy clears the RotationPolicyID reference on vaults that used it.
func TestCompoundDeleteRotationPolicy(t *testing.T) {
	t.Parallel()
	fsm := New()

	policyID := newID()
	otherPolicyID := newID()
	vault1 := newID()
	vault2 := newID()
	vault3 := newID()

	// Create policies.
	applyCmd(t, fsm, command.NewPutRotationPolicy(system.RotationPolicyConfig{ID: policyID, Name: "target"}))
	applyCmd(t, fsm, command.NewPutRotationPolicy(system.RotationPolicyConfig{ID: otherPolicyID, Name: "other"}))

	// Create vaults: vault1 and vault2 reference the target policy, vault3 references the other.
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{ID: vault1, Name: "v1", Type: system.VaultTypeMemory, RotationPolicyID: &policyID}))
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{ID: vault2, Name: "v2", Type: system.VaultTypeMemory, RotationPolicyID: &policyID}))
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{ID: vault3, Name: "v3", Type: system.VaultTypeMemory, RotationPolicyID: &otherPolicyID}))

	// Delete the target policy.
	applyCmd(t, fsm, command.NewDeleteRotationPolicy(policyID))

	ctx := context.Background()

	// vault1 and vault2 should have nil RotationPolicyID.
	for _, id := range []glid.GLID{vault1, vault2} {
		v, err := fsm.Store().GetVault(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if v.RotationPolicyID != nil {
			t.Errorf("vault %s still has rotation policy %s", v.Name, v.RotationPolicyID)
		}
	}

	// vault3 should still reference the other policy.
	v3, err := fsm.Store().GetVault(ctx, vault3)
	if err != nil {
		t.Fatal(err)
	}
	if v3.RotationPolicyID == nil || *v3.RotationPolicyID != otherPolicyID {
		t.Errorf("vault3 rotation policy should be %s, got %v", otherPolicyID, v3.RotationPolicyID)
	}
}

// TestCompoundDeleteRetentionPolicy verifies the cascade: deleting a retention
// policy removes matching retention rules from vaults.
func TestCompoundDeleteRetentionPolicy(t *testing.T) {
	t.Parallel()
	fsm := New()

	policyID := newID()
	otherPolicyID := newID()
	vaultID := newID()

	// Create policies.
	applyCmd(t, fsm, command.NewPutRetentionPolicy(system.RetentionPolicyConfig{ID: policyID, Name: "target"}))
	applyCmd(t, fsm, command.NewPutRetentionPolicy(system.RetentionPolicyConfig{ID: otherPolicyID, Name: "other"}))

	// Create vault with two retention rules: one referencing each policy.
	applyCmd(t, fsm, command.NewPutVault(system.VaultConfig{
		ID: vaultID, Name: "vault", Type: system.VaultTypeMemory,
		RetentionRules: []system.RetentionRule{
			{RetentionPolicyID: policyID},
			{RetentionPolicyID: otherPolicyID},
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

// applyCmdExpectError marshals a ConfigCommand, applies it, and returns
// the resulting error (or nil). Unlike applyCmd, it does NOT fail the
// test on apply error — that's the point.
func applyCmdExpectError(t *testing.T, fsm *FSM, cmd *gastrologv1.SystemCommand) error {
	t.Helper()
	data, err := command.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	result := fsm.Apply(&raft.Log{Data: data})
	if err, ok := result.(error); ok {
		return err
	}
	return nil
}

func putNode(t *testing.T, fsm *FSM, id glid.GLID, name string, state system.NodeState, since time.Time) {
	t.Helper()
	applyCmd(t, fsm, command.NewPutNodeConfig(system.NodeConfig{
		ID:         id,
		Name:       name,
		State:      state,
		StateSince: since,
	}))
}

func TestApplyPutNodeConfig(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	putNode(t, fsm, id, "node-1", system.NodeStateLive, now)

	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected node, got nil")
	}
	if got.Name != "node-1" {
		t.Errorf("name: got %q, want %q", got.Name, "node-1")
	}
	if got.State != system.NodeStateLive {
		t.Errorf("state: got %s, want %s", got.State, system.NodeStateLive)
	}
	if !got.StateSince.Equal(now) {
		t.Errorf("state_since: got %v, want %v", got.StateSince, now)
	}
}

func TestApplyDeleteNodeConfig(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	putNode(t, fsm, id, "n", system.NodeStateLive, time.Now().UTC().Truncate(time.Second))
	applyCmd(t, fsm, command.NewDeleteNodeConfig(id))

	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}

func TestApplySetNodeState_LegalTransition(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	start := time.Now().UTC().Truncate(time.Second)
	putNode(t, fsm, id, "n", system.NodeStateLive, start)

	transitionAt := start.Add(5 * time.Minute)
	applyCmd(t, fsm, command.NewSetNodeState(id, system.NodeStateMaintenance, transitionAt))

	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != system.NodeStateMaintenance {
		t.Errorf("state: got %s, want maintenance", got.State)
	}
	if !got.StateSince.Equal(transitionAt) {
		t.Errorf("state_since: got %v, want %v", got.StateSince, transitionAt)
	}
}

func TestApplySetNodeState_IllegalTransition(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	putNode(t, fsm, id, "n", system.NodeStateLive, now)

	// Live → Decommissioning is illegal (must go through Draining first).
	err := applyCmdExpectError(t, fsm, command.NewSetNodeState(id, system.NodeStateDecommissioning, now))
	if err == nil {
		t.Fatal("expected error for illegal transition Live → Decommissioning, got nil")
	}

	// State should not have changed.
	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != system.NodeStateLive {
		t.Errorf("state after rejected transition: got %s, want live", got.State)
	}
}

func TestApplySetNodeState_NotFound(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID() // never Put

	err := applyCmdExpectError(t, fsm, command.NewSetNodeState(id, system.NodeStateMaintenance, time.Now()))
	if err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

func TestApplySetNodeState_Idempotent(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	now := time.Now().UTC().Truncate(time.Second)
	putNode(t, fsm, id, "n", system.NodeStateLive, now)

	// Re-applying the same state is a no-op success — StateSince should
	// NOT be updated on the no-op path.
	later := now.Add(10 * time.Minute)
	applyCmd(t, fsm, command.NewSetNodeState(id, system.NodeStateLive, later))

	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.StateSince.Equal(now) {
		t.Errorf("idempotent re-set bumped StateSince: got %v, want %v (unchanged)",
			got.StateSince, now)
	}
}

func TestApplySetNodeState_LegacyMigration(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := newID()
	// Simulate a legacy record by putting State=Unknown directly via
	// the store.
	if err := fsm.Store().PutNode(context.Background(), system.NodeConfig{
		ID:   id,
		Name: "legacy",
	}); err != nil {
		t.Fatalf("seed legacy node: %v", err)
	}

	// Legacy Unknown is treated as Live for transition checks, so
	// transitioning to Maintenance should succeed.
	at := time.Now().UTC().Truncate(time.Second)
	applyCmd(t, fsm, command.NewSetNodeState(id, system.NodeStateMaintenance, at))

	got, err := fsm.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != system.NodeStateMaintenance {
		t.Errorf("state after legacy migration: got %s, want maintenance", got.State)
	}
}

func TestSnapshotRestore_NodeStatePreserved(t *testing.T) {
	t.Parallel()
	fsm1 := New()
	id := newID()
	at := time.Now().UTC().Truncate(time.Second)
	putNode(t, fsm1, id, "node-snap", system.NodeStateMaintenance, at)

	// Take snapshot.
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into fresh FSM.
	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err := fsm2.Store().GetNode(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected node after restore, got nil")
	}
	if got.State != system.NodeStateMaintenance {
		t.Errorf("state after restore: got %s, want maintenance", got.State)
	}
	if !got.StateSince.Equal(at) {
		t.Errorf("state_since after restore: got %v, want %v", got.StateSince, at)
	}
}

// TestSnapshotRestore verifies the full round-trip: populate an FSM, snapshot,
// restore into a new FSM, and verify identical state.
func TestSnapshotRestore(t *testing.T) {
	t.Parallel()
	fsm1 := New()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// Populate with various entities.
	// Match expressions live inline on routes; the route round-trip below
	// covers the snapshot path.

	maxAge := "1h"
	rpID := newID()
	applyCmd(t, fsm1, command.NewPutRotationPolicy(system.RotationPolicyConfig{ID: rpID, Name: "rp1", MaxAge: &maxAge}))

	retMaxAge := "720h"
	retID := newID()
	applyCmd(t, fsm1, command.NewPutRetentionPolicy(system.RetentionPolicyConfig{ID: retID, Name: "ret1", MaxAge: &retMaxAge}))

	vaultID := newID()

	applyCmd(t, fsm1, command.NewPutVault(system.VaultConfig{
		ID:               vaultID,
		Name:             "vault1",
		Enabled:          true,
		Type:             system.VaultTypeMemory,
		RotationPolicyID: &rpID,
		RetentionRules:   []system.RetentionRule{{RetentionPolicyID: retID}},
	}))

	ingID := newID()
	applyCmd(t, fsm1, command.NewPutIngester(system.IngesterConfig{
		ID: ingID, Name: "ing1", Enabled: true,
		Params: map[string]string{"port": "514"},
	}))

	settingsCmd, err := command.NewPutServerSettings(system.ServerSettings{}, "")
	if err != nil {
		t.Fatalf("NewPutServerSettings: %v", err)
	}
	applyCmd(t, fsm1, settingsCmd)

	certID := newID()
	applyCmd(t, fsm1, command.NewPutCertificate(system.CertPEM{
		ID: certID, Name: "cert1", CertPEM: "CERT", KeyPEM: "KEY",
	}))

	userID := newID()
	applyCmd(t, fsm1, command.NewCreateUser(system.User{
		ID: userID, Username: "alice", PasswordHash: "hash", Role: "admin",
		CreatedAt: now, UpdatedAt: now,
	}))

	tokenID := newID()
	applyCmd(t, fsm1, command.NewCreateRefreshToken(system.RefreshToken{
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

	_ = vaultID // the legacy mirror is gone — vault check above is the canonical assertion

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
	t.Parallel()
	fsm1 := New()
	now := time.Now().UTC().Truncate(time.Second)

	// Any entity works here: the assertion is about post-Restore Apply
	// behavior, not about which entity is mutated.
	preMaxAge := "1h"
	applyCmd(t, fsm1, command.NewPutRotationPolicy(system.RotationPolicyConfig{
		ID: newID(), Name: "pre-snap", MaxAge: &preMaxAge,
	}))
	applyCmd(t, fsm1, command.NewCreateUser(system.User{
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
	postMaxAge := "2h"
	newRotID := newID()
	applyCmd(t, fsm2, command.NewPutRotationPolicy(system.RotationPolicyConfig{
		ID: newRotID, Name: "post-snap", MaxAge: &postMaxAge,
	}))

	got, err := fsm2.Store().GetRotationPolicy(context.Background(), newRotID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "post-snap" {
		t.Fatalf("post-restore apply failed: %+v", got)
	}
}

// TestSnapshotEmptyStore verifies snapshot works on an empty store.
func TestSnapshotEmptyStore(t *testing.T) {
	t.Parallel()
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

// --- Apply-wait tracker ---

// TestApplyAdvancesApplyWait pins that every Apply advances the FSM's
// apply-wait tracker to the entry's index — including entries whose
// dispatch fails (the entry is consumed either way, matching raft's own
// applied-index semantics).
func TestApplyAdvancesApplyWait(t *testing.T) {
	t.Parallel()
	fsm := New()
	if got := fsm.ApplyWait().Applied(); got != 0 {
		t.Fatalf("fresh FSM Applied() = %d, want 0", got)
	}

	data, err := command.Marshal(command.NewPutRotationPolicy(system.RotationPolicyConfig{
		ID: newID(), Name: "aw-probe",
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	fsm.Apply(&raft.Log{Index: 4, Data: data})
	if got := fsm.ApplyWait().Applied(); got != 4 {
		t.Fatalf("Applied() after Apply(index 4) = %d, want 4", got)
	}

	// A failing entry (garbage payload) still advances the tracker.
	if res := fsm.Apply(&raft.Log{Index: 5, Data: []byte("not a protobuf")}); res == nil {
		t.Fatal("expected error result for garbage payload")
	}
	if got := fsm.ApplyWait().Applied(); got != 5 {
		t.Fatalf("Applied() after failing Apply(index 5) = %d, want 5", got)
	}
}

// TestSnapshotRestoreAdvancesApplyWait covers the follower-installs-snapshot
// path of the read-after-write barrier: when the target entry reaches the
// follower inside a snapshot instead of via log replication, Restore must
// release waiters up to the snapshot's embedded applied index — after the
// store swap, so a released waiter reads post-restore state.
func TestSnapshotRestoreAdvancesApplyWait(t *testing.T) {
	t.Parallel()
	fsm1 := New()
	id := newID()
	data, err := command.Marshal(command.NewPutRotationPolicy(system.RotationPolicyConfig{
		ID: id, Name: "snap-barrier-probe",
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	fsm1.Apply(&raft.Log{Index: 9, Data: data})

	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := New()
	done := make(chan error, 1)
	go func() { done <- fsm2.ApplyWait().Wait(context.Background(), 9) }()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Wait(9) across Restore: %v", err)
	}
	got, err := fsm2.Store().GetRotationPolicy(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("waiter released before restored state was readable")
	}
	if want := uint64(9); fsm2.ApplyWait().Applied() != want {
		t.Fatalf("Applied() after Restore = %d, want %d", fsm2.ApplyWait().Applied(), want)
	}
}
