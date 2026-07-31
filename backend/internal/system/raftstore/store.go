// Package raftstore provides a system.Store backed by hashicorp/raft.
// Writes go through raft.Apply() which persists commands to the raft log
// (boltdb) before dispatching to the FSM. Reads delegate directly to the
// FSM's in-memory store.
//
// In multi-node mode, writes on a follower are transparently forwarded to
// the leader via the Forwarder interface.
package raftstore

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"
	"gastrolog/internal/system/raftfsm"

	"github.com/hashicorp/raft"
)

var _ system.Store = (*Store)(nil)

// Forwarder forwards pre-marshaled config commands to the Raft leader.
// Implemented by cluster.Forwarder in multi-node mode. Returns the Raft
// log index at which the leader applied the command so the follower can
// wait for its own FSM to catch up before reading post-mutation state.
type Forwarder interface {
	Forward(ctx context.Context, data []byte) (uint64, error)
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
// is forwarded to the leader transparently. The applied Raft log index is
// discarded here — callers that need it use applyRaw directly.
//
// The effective timeout is min(ctx deadline, s.applyTimeout). Callers that
// want a tighter bound (e.g. orchestrator shutdown under lost quorum) pass
// a context with a shorter deadline.
func (s *Store) apply(ctx context.Context, cmd *gastrologv1.SystemCommand) error {
	data, err := command.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	_, err = s.applyRaw(ctx, data)
	return err
}

// applyRaw submits pre-marshaled command bytes through raft.Apply(),
// forwarding to the leader if this node is a follower. Returns the Raft
// log index at which the command was applied.
//
// On the leader, raft.Apply is synchronous w.r.t. local FSM Apply, so the
// returned index is already reflected in the local FSM by the time this
// returns. On a follower, the command is forwarded to the leader for
// commit, and the follower then waits for its own local FSM to apply up
// to the returned index before returning. This guarantees that the caller
// can read post-mutation state from the local FSM immediately, closing the
// read-after-write race that otherwise leaves a follower serving stale
// config right after a write it just made.
func (s *Store) applyRaw(ctx context.Context, data []byte) (uint64, error) {
	// Retry while a leadership transfer is in progress before deciding what to
	// do with the error. ErrLeadershipTransferInProgress is NOT
	// ErrNotLeader — this node still leads, it is just refusing new entries
	// mid-handover — so forwarding would bounce straight back. The transfer
	// settles in milliseconds; afterwards this either applies or returns
	// ErrNotLeader and forwards. ErrLeadershipLost stays un-retried: the entry
	// may already be committed, and a duplicated config apply is worse than a
	// surfaced error.
	var future raft.ApplyFuture
	err := applyRetryingLeadershipTransfer(func() error {
		future = s.raft.Apply(data, s.effectiveTimeout(ctx))
		return future.Error()
	})
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) && s.forwarder != nil {
			return s.forwardAndWait(ctx, data)
		}
		return 0, fmt.Errorf("raft apply: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return future.Index(), err
		}
	}
	return future.Index(), nil
}

// forwardAndWait sends the command to the leader and blocks until the local
// FSM has applied up to the leader's index. Splits the follower-forward path
// out of applyRaw to keep nesting shallow.
func (s *Store) forwardAndWait(ctx context.Context, data []byte) (uint64, error) {
	fwdCtx, cancel := context.WithTimeout(ctx, s.effectiveTimeout(ctx))
	defer cancel()
	appliedIndex, err := s.forwarder.Forward(fwdCtx, data)
	if err != nil {
		return 0, err
	}
	if err := s.waitForLocalApply(ctx, appliedIndex); err != nil {
		return appliedIndex, err
	}
	return appliedIndex, nil
}

// waitForLocalApply blocks until the local FSM has applied at least the
// given index, bounded by the effective timeout. Used on followers after
// Forward to ensure post-mutation reads see the new state.
//
// Event-driven: the FSM advances its applywait.Tracker as it applies each
// committed entry (and on snapshot restore), waking this wait the moment
// the mutation is locally visible — no polling. The tracker must be fed
// by FSM.Apply rather than by raft.AppliedIndex(), which advances when an
// entry is enqueued for the FSM goroutine, before FSM.Apply runs, and so
// would release a reader while the store still showed pre-mutation state.
// Times out if the follower never catches up (partitioned, log truncated,
// etc.) so a stuck cluster surfaces as a client-visible error rather than
// a hang.
func (s *Store) waitForLocalApply(ctx context.Context, target uint64) error {
	if target == 0 {
		return nil
	}
	tracker := s.fsm.ApplyWait()
	waitCtx, cancel := context.WithTimeout(ctx, s.effectiveTimeout(ctx))
	defer cancel()
	if err := tracker.Wait(waitCtx, target); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("wait for local FSM apply at index %d: timeout (last applied %d)", target, tracker.Applied())
	}
	return nil
}

// effectiveTimeout returns min(ctx deadline, s.applyTimeout). Raft.Apply
// doesn't accept a context, only a duration — so we translate.
func (s *Store) effectiveTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < s.applyTimeout {
			if d < 0 {
				return 0
			}
			return d
		}
	}
	return s.applyTimeout
}

// ApplyRaw applies pre-marshaled command bytes and returns the Raft log
// index at which the command was applied. Used by the cluster ForwardApply
// handler on the leader to apply commands received from followers; the
// returned index is sent back to the follower so it can wait for its own
// FSM to catch up.
func (s *Store) ApplyRaw(data []byte) (uint64, error) {
	return s.applyRaw(context.Background(), data)
}

// Barrier commits a state-free catch-up-barrier command through Raft and
// blocks until the local FSM has applied it, guaranteeing the local FSM
// reflects every entry committed cluster-wide before the call returned.
//
// It reuses the read-after-write machinery (applyRaw + the event-driven
// apply-wait tracker): on the leader raft.Apply is synchronous, so the FSM is
// current on return; on a follower the barrier is forwarded to the leader and
// this blocks on the tracker until the local FSM applies up to the barrier's
// committed index — no polling. The barrier is an ordinary LogCommand (not a
// raft LogBarrier) so it flows through FSM.Apply, which the FSM-fed tracker
// requires to advance. Used by startup FSM catch-up; bound the wait by
// passing a ctx with a deadline.
func (s *Store) Barrier(ctx context.Context) error {
	data, err := command.Marshal(command.NewCatchupBarrier())
	if err != nil {
		return fmt.Errorf("marshal catchup barrier: %w", err)
	}
	_, err = s.applyRaw(ctx, data)
	return err
}

// ---------------------------------------------------------------------------
// Read methods — delegate to fsm.Store()
// ---------------------------------------------------------------------------

func (s *Store) Load(ctx context.Context) (*system.System, error) {
	return s.fsm.Store().Load(ctx)
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

func (s *Store) GetNodeStorageConfig(ctx context.Context, nodeID string) (*system.NodeStorageConfig, error) {
	return s.fsm.Store().GetNodeStorageConfig(ctx, nodeID)
}

func (s *Store) ListNodeStorageConfigs(ctx context.Context) ([]system.NodeStorageConfig, error) {
	return s.fsm.Store().ListNodeStorageConfigs(ctx)
}

// ---------------------------------------------------------------------------
// Write methods — serialize → raft.Apply
// ---------------------------------------------------------------------------

func (s *Store) PutRotationPolicy(ctx context.Context, cfg system.RotationPolicyConfig) error {
	return s.apply(ctx, command.NewPutRotationPolicy(cfg))
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteRotationPolicy(id))
}

func (s *Store) PutRetentionPolicy(ctx context.Context, cfg system.RetentionPolicyConfig) error {
	return s.apply(ctx, command.NewPutRetentionPolicy(cfg))
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteRetentionPolicy(id))
}

func (s *Store) PutVault(ctx context.Context, cfg system.VaultConfig) error {
	return s.apply(ctx, command.NewPutVault(cfg))
}

func (s *Store) DeleteVault(ctx context.Context, id glid.GLID, deleteData bool) error {
	return s.apply(ctx, command.NewDeleteVault(id, deleteData))
}

func (s *Store) PutIngester(ctx context.Context, cfg system.IngesterConfig) error {
	return s.apply(ctx, command.NewPutIngester(cfg))
}

func (s *Store) DeleteIngester(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteIngester(id))
}

func (s *Store) PutRoute(ctx context.Context, cfg system.RouteConfig) error {
	return s.apply(ctx, command.NewPutRoute(cfg))
}

func (s *Store) DeleteRoute(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteRoute(id))
}

func (s *Store) PutManagedFile(ctx context.Context, cfg system.ManagedFileConfig) error {
	return s.apply(ctx, command.NewPutManagedFile(cfg))
}

func (s *Store) DeleteManagedFile(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteManagedFile(id))
}

func (s *Store) SaveServerSettings(ctx context.Context, ss system.ServerSettings) error {
	cmd, err := command.NewPutServerSettings(ss, system.SaveServerSettingsNotifyKey(ctx))
	if err != nil {
		return err
	}
	return s.apply(ctx, cmd)
}

func (s *Store) PutClusterTLS(ctx context.Context, tls system.ClusterTLS) error {
	return s.apply(ctx, command.NewPutClusterTLS(tls))
}

func (s *Store) GetLogLevels(ctx context.Context) (system.LogLevelConfig, error) {
	return s.fsm.Store().GetLogLevels(ctx)
}

func (s *Store) PutLogLevels(ctx context.Context, cfg system.LogLevelConfig) error {
	return s.apply(ctx, command.NewPutLogLevels(cfg))
}

func (s *Store) PutNode(ctx context.Context, node system.NodeConfig) error {
	return s.apply(ctx, command.NewPutNodeConfig(node))
}

func (s *Store) DeleteNode(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteNodeConfig(id))
}

func (s *Store) SetNodeState(ctx context.Context, id glid.GLID, state system.NodeState, since time.Time) error {
	return s.apply(ctx, command.NewSetNodeState(id, state, since))
}

func (s *Store) PutCertificate(ctx context.Context, cert system.CertPEM) error {
	return s.apply(ctx, command.NewPutCertificate(cert))
}

func (s *Store) DeleteCertificate(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteCertificate(id))
}

func (s *Store) CreateUser(ctx context.Context, user system.User) error {
	return s.apply(ctx, command.NewCreateUser(user))
}

func (s *Store) UpdatePassword(ctx context.Context, id glid.GLID, passwordHash string) error {
	return s.apply(ctx, command.NewUpdatePassword(id, passwordHash))
}

func (s *Store) UpdateUserRole(ctx context.Context, id glid.GLID, role string) error {
	return s.apply(ctx, command.NewUpdateUserRole(id, role))
}

func (s *Store) UpdateUsername(ctx context.Context, id glid.GLID, username string) error {
	return s.apply(ctx, command.NewUpdateUsername(id, username))
}

func (s *Store) DeleteUser(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteUser(id))
}

func (s *Store) InvalidateTokens(ctx context.Context, id glid.GLID, at time.Time) error {
	return s.apply(ctx, command.NewInvalidateTokens(id, at))
}

func (s *Store) PutUserPreferences(ctx context.Context, id glid.GLID, prefs string) error {
	return s.apply(ctx, command.NewPutUserPreferences(id, prefs))
}

func (s *Store) CreateRefreshToken(ctx context.Context, token system.RefreshToken) error {
	return s.apply(ctx, command.NewCreateRefreshToken(token))
}

func (s *Store) DeleteRefreshToken(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteRefreshToken(id))
}

func (s *Store) DeleteUserRefreshTokens(ctx context.Context, userID glid.GLID) error {
	return s.apply(ctx, command.NewDeleteUserRefreshTokens(userID))
}

func (s *Store) PutCloudService(ctx context.Context, svc system.CloudService) error {
	return s.apply(ctx, command.NewPutCloudService(svc))
}

func (s *Store) DeleteCloudService(ctx context.Context, id glid.GLID) error {
	return s.apply(ctx, command.NewDeleteCloudService(id))
}

func (s *Store) SetNodeStorageConfig(ctx context.Context, cfg system.NodeStorageConfig) error {
	return s.apply(ctx, command.NewSetNodeStorageConfig(cfg))
}

// --- Runtime methods (delegate to inner store for reads, apply for writes) ---

func (s *Store) GetVaultPlacements(ctx context.Context, vaultID glid.GLID) ([]system.VaultPlacement, error) {
	return s.fsm.Store().GetVaultPlacements(ctx, vaultID)
}

func (s *Store) SetVaultPlacements(ctx context.Context, vaultID glid.GLID, placements []system.VaultPlacement) error {
	return s.apply(ctx, command.NewSetVaultPlacements(vaultID, placements))
}

func (s *Store) GetIngesterAlive(ctx context.Context, ingesterID glid.GLID) (map[string]bool, error) {
	return s.fsm.Store().GetIngesterAlive(ctx, ingesterID)
}

func (s *Store) SetIngesterAlive(ctx context.Context, ingesterID glid.GLID, nodeID string, alive bool) error {
	return s.apply(ctx, command.NewSetIngesterAlive(ingesterID, nodeID, alive))
}

func (s *Store) GetIngesterCheckpoint(ctx context.Context, ingesterID glid.GLID) ([]byte, error) {
	return s.fsm.Store().GetIngesterCheckpoint(ctx, ingesterID)
}

func (s *Store) SetIngesterCheckpoint(ctx context.Context, ingesterID glid.GLID, data []byte) error {
	return s.apply(ctx, command.NewSetIngesterCheckpoint(ingesterID, data))
}

func (s *Store) GetIngesterAssignment(ctx context.Context, ingesterID glid.GLID) (string, error) {
	return s.fsm.Store().GetIngesterAssignment(ctx, ingesterID)
}

func (s *Store) SetIngesterAssignment(ctx context.Context, ingesterID glid.GLID, nodeID string) error {
	return s.apply(ctx, command.NewSetIngesterAssignment(ingesterID, nodeID))
}

func (s *Store) GetSetupWizardDismissed(ctx context.Context) (bool, error) {
	return s.fsm.Store().GetSetupWizardDismissed(ctx)
}

func (s *Store) SetSetupWizardDismissed(ctx context.Context, dismissed bool) error {
	return s.apply(ctx, command.NewSetSetupWizardDismissed(dismissed))
}

// applyRetryingLeadershipTransfer mirrors cluster's helper of the same name for
// the config store's own Raft group. Duplicated rather than shared because
// importing internal/cluster here would invert the dependency — raftstore is
// below cluster, not beside it. The BOUNDS are intentionally identical; if one
// changes the other should, and this comment is the link between them.
func applyRetryingLeadershipTransfer(apply func() error) error {
	const (
		attempts = 5
		backoff  = 20 * time.Millisecond
	)
	var err error
	for attempt := range attempts {
		err = apply()
		if !errors.Is(err, raft.ErrLeadershipTransferInProgress) {
			return err
		}
		if attempt < attempts-1 {
			time.Sleep(backoff)
		}
	}
	return err
}
