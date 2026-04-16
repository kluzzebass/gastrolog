// Package raftstore provides a system.Store backed by hashicorp/raft.
// Writes go through raft.Apply() which persists commands to the raft log
// (boltdb) before dispatching to the FSM. Reads delegate directly to the
// FSM's in-memory store.
//
// In multi-node mode, writes on a follower are transparently forwarded to
// the leader via the Forwarder interface.
package raftstore

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"
	"gastrolog/internal/system/raftfsm"

	"github.com/hashicorp/raft"
)

var _ system.Store = (*Store)(nil)

// Forwarder forwards pre-marshaled config commands to the Raft leader.
// Implemented by cluster.Forwarder in multi-node mode.
type Forwarder interface {
	Forward(ctx context.Context, data []byte) error
}

// Store implements system.Store by routing writes through raft.Apply() for
// persistence and reading from the FSM's in-memory store.
type Store struct {
	fsm          *raftfsm.FSM
	raft         *raft.Raft
	applyTimeout time.Duration
	forwarder    Forwarder // nil for single-node
}

// New creates a new Store.
func New(r *raft.Raft, fsm *raftfsm.FSM, applyTimeout time.Duration) *Store {
	return &Store{
		fsm:          fsm,
		raft:         r,
		applyTimeout: applyTimeout,
	}
}

// SetForwarder enables leader forwarding for multi-node clusters.
// When set, writes that fail with ErrNotLeader are forwarded to the
// leader's cluster port instead of returning an error.
func (s *Store) SetForwarder(f Forwarder) {
	s.forwarder = f
}

// apply serializes a ConfigCommand and submits it through raft.Apply().
// If this node is not the leader and a forwarder is configured, the command
// is forwarded to the leader transparently.
func (s *Store) apply(cmd *gastrologv1.SystemCommand) error {
	data, err := command.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	return s.applyRaw(data)
}

// applyRaw submits pre-marshaled command bytes through raft.Apply(),
// forwarding to the leader if this node is a follower.
func (s *Store) applyRaw(data []byte) error {
	future := s.raft.Apply(data, s.applyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) && s.forwarder != nil {
			ctx, cancel := context.WithTimeout(context.Background(), s.applyTimeout)
			defer cancel()
			return s.forwarder.Forward(ctx, data)
		}
		return fmt.Errorf("raft apply: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// ApplyRaw applies pre-marshaled command bytes. Used by the cluster
// ForwardApply handler on the leader to apply commands received from followers.
func (s *Store) ApplyRaw(data []byte) error {
	return s.applyRaw(data)
}

// ---------------------------------------------------------------------------
// Read methods — delegate to fsm.Store()
// ---------------------------------------------------------------------------

func (s *Store) Load(ctx context.Context) (*system.System, error) {
	return s.fsm.Store().Load(ctx)
}

func (s *Store) GetFilter(ctx context.Context, id glid.GLID) (*system.FilterConfig, error) {
	return s.fsm.Store().GetFilter(ctx, id)
}

func (s *Store) ListFilters(ctx context.Context) ([]system.FilterConfig, error) {
	return s.fsm.Store().ListFilters(ctx)
}

func (s *Store) GetRotationPolicy(ctx context.Context, id glid.GLID) (*system.RotationPolicyConfig, error) {
	return s.fsm.Store().GetRotationPolicy(ctx, id)
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]system.RotationPolicyConfig, error) {
	return s.fsm.Store().ListRotationPolicies(ctx)
}

func (s *Store) GetRetentionPolicy(ctx context.Context, id glid.GLID) (*system.RetentionPolicyConfig, error) {
	return s.fsm.Store().GetRetentionPolicy(ctx, id)
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]system.RetentionPolicyConfig, error) {
	return s.fsm.Store().ListRetentionPolicies(ctx)
}

func (s *Store) GetVault(ctx context.Context, id glid.GLID) (*system.VaultConfig, error) {
	return s.fsm.Store().GetVault(ctx, id)
}

func (s *Store) ListVaults(ctx context.Context) ([]system.VaultConfig, error) {
	return s.fsm.Store().ListVaults(ctx)
}

func (s *Store) GetIngester(ctx context.Context, id glid.GLID) (*system.IngesterConfig, error) {
	return s.fsm.Store().GetIngester(ctx, id)
}

func (s *Store) ListIngesters(ctx context.Context) ([]system.IngesterConfig, error) {
	return s.fsm.Store().ListIngesters(ctx)
}

func (s *Store) GetRoute(ctx context.Context, id glid.GLID) (*system.RouteConfig, error) {
	return s.fsm.Store().GetRoute(ctx, id)
}

func (s *Store) ListRoutes(ctx context.Context) ([]system.RouteConfig, error) {
	return s.fsm.Store().ListRoutes(ctx)
}

func (s *Store) GetManagedFile(ctx context.Context, id glid.GLID) (*system.ManagedFileConfig, error) {
	return s.fsm.Store().GetManagedFile(ctx, id)
}

func (s *Store) ListManagedFiles(ctx context.Context) ([]system.ManagedFileConfig, error) {
	return s.fsm.Store().ListManagedFiles(ctx)
}

func (s *Store) LoadServerSettings(ctx context.Context) (system.ServerSettings, error) {
	return s.fsm.Store().LoadServerSettings(ctx)
}

func (s *Store) GetNode(ctx context.Context, id glid.GLID) (*system.NodeConfig, error) {
	return s.fsm.Store().GetNode(ctx, id)
}

func (s *Store) ListNodes(ctx context.Context) ([]system.NodeConfig, error) {
	return s.fsm.Store().ListNodes(ctx)
}

func (s *Store) ListCertificates(ctx context.Context) ([]system.CertPEM, error) {
	return s.fsm.Store().ListCertificates(ctx)
}

func (s *Store) GetCertificate(ctx context.Context, id glid.GLID) (*system.CertPEM, error) {
	return s.fsm.Store().GetCertificate(ctx, id)
}

func (s *Store) GetUser(ctx context.Context, id glid.GLID) (*system.User, error) {
	return s.fsm.Store().GetUser(ctx, id)
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*system.User, error) {
	return s.fsm.Store().GetUserByUsername(ctx, username)
}

func (s *Store) ListUsers(ctx context.Context) ([]system.User, error) {
	return s.fsm.Store().ListUsers(ctx)
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	return s.fsm.Store().CountUsers(ctx)
}

func (s *Store) GetUserPreferences(ctx context.Context, id glid.GLID) (*string, error) {
	return s.fsm.Store().GetUserPreferences(ctx, id)
}

func (s *Store) GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*system.RefreshToken, error) {
	return s.fsm.Store().GetRefreshTokenByHash(ctx, tokenHash)
}

func (s *Store) ListRefreshTokens(ctx context.Context) ([]system.RefreshToken, error) {
	return s.fsm.Store().ListRefreshTokens(ctx)
}

func (s *Store) GetCloudService(ctx context.Context, id glid.GLID) (*system.CloudService, error) {
	return s.fsm.Store().GetCloudService(ctx, id)
}

func (s *Store) ListCloudServices(ctx context.Context) ([]system.CloudService, error) {
	return s.fsm.Store().ListCloudServices(ctx)
}

func (s *Store) GetTier(ctx context.Context, id glid.GLID) (*system.TierConfig, error) {
	return s.fsm.Store().GetTier(ctx, id)
}

func (s *Store) ListTiers(ctx context.Context) ([]system.TierConfig, error) {
	return s.fsm.Store().ListTiers(ctx)
}

func (s *Store) GetNodeStorageConfig(ctx context.Context, nodeID string) (*system.NodeStorageConfig, error) {
	return s.fsm.Store().GetNodeStorageConfig(ctx, nodeID)
}

func (s *Store) ListNodeStorageConfigs(ctx context.Context) ([]system.NodeStorageConfig, error) {
	return s.fsm.Store().ListNodeStorageConfigs(ctx)
}

// ---------------------------------------------------------------------------
// Write methods — serialize → raft.Apply
// ---------------------------------------------------------------------------

func (s *Store) PutFilter(ctx context.Context, cfg system.FilterConfig) error {
	return s.apply(command.NewPutFilter(cfg))
}

func (s *Store) DeleteFilter(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteFilter(id))
}

func (s *Store) PutRotationPolicy(ctx context.Context, cfg system.RotationPolicyConfig) error {
	return s.apply(command.NewPutRotationPolicy(cfg))
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteRotationPolicy(id))
}

func (s *Store) PutRetentionPolicy(ctx context.Context, cfg system.RetentionPolicyConfig) error {
	return s.apply(command.NewPutRetentionPolicy(cfg))
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteRetentionPolicy(id))
}

func (s *Store) PutVault(ctx context.Context, cfg system.VaultConfig) error {
	return s.apply(command.NewPutVault(cfg))
}

func (s *Store) DeleteVault(ctx context.Context, id glid.GLID, deleteData bool) error {
	return s.apply(command.NewDeleteVault(id, deleteData))
}

func (s *Store) PutIngester(ctx context.Context, cfg system.IngesterConfig) error {
	return s.apply(command.NewPutIngester(cfg))
}

func (s *Store) DeleteIngester(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteIngester(id))
}

func (s *Store) PutRoute(ctx context.Context, cfg system.RouteConfig) error {
	return s.apply(command.NewPutRoute(cfg))
}

func (s *Store) DeleteRoute(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteRoute(id))
}

func (s *Store) PutManagedFile(ctx context.Context, cfg system.ManagedFileConfig) error {
	return s.apply(command.NewPutManagedFile(cfg))
}

func (s *Store) DeleteManagedFile(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteManagedFile(id))
}

func (s *Store) SaveServerSettings(ctx context.Context, ss system.ServerSettings) error {
	cmd, err := command.NewPutServerSettings(ss)
	if err != nil {
		return err
	}
	return s.apply(cmd)
}

func (s *Store) PutClusterTLS(ctx context.Context, tls system.ClusterTLS) error {
	return s.apply(command.NewPutClusterTLS(tls))
}

func (s *Store) PutNode(ctx context.Context, node system.NodeConfig) error {
	return s.apply(command.NewPutNodeConfig(node))
}

func (s *Store) DeleteNode(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteNodeConfig(id))
}

func (s *Store) PutCertificate(ctx context.Context, cert system.CertPEM) error {
	return s.apply(command.NewPutCertificate(cert))
}

func (s *Store) DeleteCertificate(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteCertificate(id))
}

func (s *Store) CreateUser(ctx context.Context, user system.User) error {
	return s.apply(command.NewCreateUser(user))
}

func (s *Store) UpdatePassword(ctx context.Context, id glid.GLID, passwordHash string) error {
	return s.apply(command.NewUpdatePassword(id, passwordHash))
}

func (s *Store) UpdateUserRole(ctx context.Context, id glid.GLID, role string) error {
	return s.apply(command.NewUpdateUserRole(id, role))
}

func (s *Store) UpdateUsername(ctx context.Context, id glid.GLID, username string) error {
	return s.apply(command.NewUpdateUsername(id, username))
}

func (s *Store) DeleteUser(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteUser(id))
}

func (s *Store) InvalidateTokens(ctx context.Context, id glid.GLID, at time.Time) error {
	return s.apply(command.NewInvalidateTokens(id, at))
}

func (s *Store) PutUserPreferences(ctx context.Context, id glid.GLID, prefs string) error {
	return s.apply(command.NewPutUserPreferences(id, prefs))
}

func (s *Store) CreateRefreshToken(ctx context.Context, token system.RefreshToken) error {
	return s.apply(command.NewCreateRefreshToken(token))
}

func (s *Store) DeleteRefreshToken(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteRefreshToken(id))
}

func (s *Store) DeleteUserRefreshTokens(ctx context.Context, userID glid.GLID) error {
	return s.apply(command.NewDeleteUserRefreshTokens(userID))
}

func (s *Store) PutCloudService(ctx context.Context, svc system.CloudService) error {
	return s.apply(command.NewPutCloudService(svc))
}

func (s *Store) DeleteCloudService(ctx context.Context, id glid.GLID) error {
	return s.apply(command.NewDeleteCloudService(id))
}

func (s *Store) PutTier(ctx context.Context, tier system.TierConfig) error {
	return s.apply(command.NewPutTier(tier))
}

func (s *Store) DeleteTier(ctx context.Context, id glid.GLID, drain bool) error {
	return s.apply(command.NewDeleteTier(id, drain))
}

func (s *Store) SetNodeStorageConfig(ctx context.Context, cfg system.NodeStorageConfig) error {
	return s.apply(command.NewSetNodeStorageConfig(cfg))
}

// --- Runtime methods (delegate to inner store for reads, apply for writes) ---

func (s *Store) GetTierPlacements(ctx context.Context, tierID glid.GLID) ([]system.TierPlacement, error) {
	return s.fsm.Store().GetTierPlacements(ctx, tierID)
}

func (s *Store) SetTierPlacements(ctx context.Context, tierID glid.GLID, placements []system.TierPlacement) error {
	return s.apply(command.NewSetTierPlacements(tierID, placements))
}

func (s *Store) GetIngesterAlive(ctx context.Context, ingesterID glid.GLID) (map[string]bool, error) {
	return s.fsm.Store().GetIngesterAlive(ctx, ingesterID)
}

func (s *Store) SetIngesterAlive(ctx context.Context, ingesterID glid.GLID, nodeID string, alive bool) error {
	return s.apply(command.NewSetIngesterAlive(ingesterID, nodeID, alive))
}

func (s *Store) GetIngesterAssignment(ctx context.Context, ingesterID glid.GLID) (string, error) {
	return s.fsm.Store().GetIngesterAssignment(ctx, ingesterID)
}

func (s *Store) SetIngesterAssignment(ctx context.Context, ingesterID glid.GLID, nodeID string) error {
	return s.apply(command.NewSetIngesterAssignment(ingesterID, nodeID))
}

func (s *Store) GetSetupWizardDismissed(ctx context.Context) (bool, error) {
	return s.fsm.Store().GetSetupWizardDismissed(ctx)
}

func (s *Store) SetSetupWizardDismissed(ctx context.Context, dismissed bool) error {
	return s.apply(command.NewSetSetupWizardDismissed(dismissed))
}
