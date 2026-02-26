// Package raftstore provides a config.Store backed by a single-node
// hashicorp/raft instance. Writes go through raft.Apply() which persists
// commands to the raft log (boltdb) before dispatching to the FSM. Reads
// delegate directly to the FSM's in-memory store.
//
// This gives us a persistent WAL + snapshot machinery for free. Multi-node
// consensus is out of scope here — see gastrolog-e1ig.
package raftstore

import (
	"context"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/command"
	"gastrolog/internal/config/raftfsm"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

var _ config.Store = (*Store)(nil)

// Store implements config.Store by routing writes through raft.Apply() for
// persistence and reading from the FSM's in-memory store.
type Store struct {
	fsm          *raftfsm.FSM
	raft         *raft.Raft
	applyTimeout time.Duration
}

// New creates a new Store.
func New(r *raft.Raft, fsm *raftfsm.FSM, applyTimeout time.Duration) *Store {
	return &Store{
		fsm:          fsm,
		raft:         r,
		applyTimeout: applyTimeout,
	}
}

// apply serializes a ConfigCommand and submits it through raft.Apply(),
// which persists to the log before dispatching to FSM.Apply().
func (s *Store) apply(cmd *gastrologv1.ConfigCommand) error {
	data, err := command.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	future := s.raft.Apply(data, s.applyTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Read methods — delegate to fsm.Store()
// ---------------------------------------------------------------------------

func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	return s.fsm.Store().Load(ctx)
}

func (s *Store) GetFilter(ctx context.Context, id uuid.UUID) (*config.FilterConfig, error) {
	return s.fsm.Store().GetFilter(ctx, id)
}

func (s *Store) ListFilters(ctx context.Context) ([]config.FilterConfig, error) {
	return s.fsm.Store().ListFilters(ctx)
}

func (s *Store) GetRotationPolicy(ctx context.Context, id uuid.UUID) (*config.RotationPolicyConfig, error) {
	return s.fsm.Store().GetRotationPolicy(ctx, id)
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]config.RotationPolicyConfig, error) {
	return s.fsm.Store().ListRotationPolicies(ctx)
}

func (s *Store) GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*config.RetentionPolicyConfig, error) {
	return s.fsm.Store().GetRetentionPolicy(ctx, id)
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]config.RetentionPolicyConfig, error) {
	return s.fsm.Store().ListRetentionPolicies(ctx)
}

func (s *Store) GetVault(ctx context.Context, id uuid.UUID) (*config.VaultConfig, error) {
	return s.fsm.Store().GetVault(ctx, id)
}

func (s *Store) ListVaults(ctx context.Context) ([]config.VaultConfig, error) {
	return s.fsm.Store().ListVaults(ctx)
}

func (s *Store) GetIngester(ctx context.Context, id uuid.UUID) (*config.IngesterConfig, error) {
	return s.fsm.Store().GetIngester(ctx, id)
}

func (s *Store) ListIngesters(ctx context.Context) ([]config.IngesterConfig, error) {
	return s.fsm.Store().ListIngesters(ctx)
}

func (s *Store) GetSetting(ctx context.Context, key string) (*string, error) {
	return s.fsm.Store().GetSetting(ctx, key)
}

func (s *Store) ListCertificates(ctx context.Context) ([]config.CertPEM, error) {
	return s.fsm.Store().ListCertificates(ctx)
}

func (s *Store) GetCertificate(ctx context.Context, id uuid.UUID) (*config.CertPEM, error) {
	return s.fsm.Store().GetCertificate(ctx, id)
}

func (s *Store) GetUser(ctx context.Context, id uuid.UUID) (*config.User, error) {
	return s.fsm.Store().GetUser(ctx, id)
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*config.User, error) {
	return s.fsm.Store().GetUserByUsername(ctx, username)
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	return s.fsm.Store().ListUsers(ctx)
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	return s.fsm.Store().CountUsers(ctx)
}

func (s *Store) GetUserPreferences(ctx context.Context, id uuid.UUID) (*string, error) {
	return s.fsm.Store().GetUserPreferences(ctx, id)
}

func (s *Store) GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*config.RefreshToken, error) {
	return s.fsm.Store().GetRefreshTokenByHash(ctx, tokenHash)
}

func (s *Store) ListRefreshTokens(ctx context.Context) ([]config.RefreshToken, error) {
	return s.fsm.Store().ListRefreshTokens(ctx)
}

// ---------------------------------------------------------------------------
// Write methods — serialize → raft.Apply
// ---------------------------------------------------------------------------

func (s *Store) PutFilter(ctx context.Context, cfg config.FilterConfig) error {
	return s.apply(command.NewPutFilter(cfg))
}

func (s *Store) DeleteFilter(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteFilter(id))
}

func (s *Store) PutRotationPolicy(ctx context.Context, cfg config.RotationPolicyConfig) error {
	return s.apply(command.NewPutRotationPolicy(cfg))
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteRotationPolicy(id))
}

func (s *Store) PutRetentionPolicy(ctx context.Context, cfg config.RetentionPolicyConfig) error {
	return s.apply(command.NewPutRetentionPolicy(cfg))
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteRetentionPolicy(id))
}

func (s *Store) PutVault(ctx context.Context, cfg config.VaultConfig) error {
	return s.apply(command.NewPutVault(cfg))
}

func (s *Store) DeleteVault(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteVault(id))
}

func (s *Store) PutIngester(ctx context.Context, cfg config.IngesterConfig) error {
	return s.apply(command.NewPutIngester(cfg))
}

func (s *Store) DeleteIngester(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteIngester(id))
}

func (s *Store) PutSetting(ctx context.Context, key string, value string) error {
	return s.apply(command.NewPutSetting(key, value))
}

func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	return s.apply(command.NewDeleteSetting(key))
}

func (s *Store) PutCertificate(ctx context.Context, cert config.CertPEM) error {
	return s.apply(command.NewPutCertificate(cert))
}

func (s *Store) DeleteCertificate(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteCertificate(id))
}

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	return s.apply(command.NewCreateUser(user))
}

func (s *Store) UpdatePassword(ctx context.Context, id uuid.UUID, passwordHash string) error {
	return s.apply(command.NewUpdatePassword(id, passwordHash))
}

func (s *Store) UpdateUserRole(ctx context.Context, id uuid.UUID, role string) error {
	return s.apply(command.NewUpdateUserRole(id, role))
}

func (s *Store) UpdateUsername(ctx context.Context, id uuid.UUID, username string) error {
	return s.apply(command.NewUpdateUsername(id, username))
}

func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteUser(id))
}

func (s *Store) InvalidateTokens(ctx context.Context, id uuid.UUID, at time.Time) error {
	return s.apply(command.NewInvalidateTokens(id, at))
}

func (s *Store) PutUserPreferences(ctx context.Context, id uuid.UUID, prefs string) error {
	return s.apply(command.NewPutUserPreferences(id, prefs))
}

func (s *Store) CreateRefreshToken(ctx context.Context, token config.RefreshToken) error {
	return s.apply(command.NewCreateRefreshToken(token))
}

func (s *Store) DeleteRefreshToken(ctx context.Context, id uuid.UUID) error {
	return s.apply(command.NewDeleteRefreshToken(id))
}

func (s *Store) DeleteUserRefreshTokens(ctx context.Context, userID uuid.UUID) error {
	return s.apply(command.NewDeleteUserRefreshTokens(userID))
}
