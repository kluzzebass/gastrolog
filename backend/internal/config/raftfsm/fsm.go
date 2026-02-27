// Package raftfsm provides a Raft FSM that applies config commands to an
// in-memory config store. It bridges Raft's replicated log with the config
// system, handling command dispatch, snapshots for log compaction, and
// restore for follower catch-up.
package raftfsm

import (
	"context"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/config/command"
	"gastrolog/internal/config/memory"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

// NotifyKind identifies the type of config mutation that was applied.
type NotifyKind int

const (
	NotifyVaultPut NotifyKind = iota + 1
	NotifyVaultDeleted
	NotifyFilterPut
	NotifyFilterDeleted
	NotifyRotationPolicyPut
	NotifyRotationPolicyDeleted
	NotifyRetentionPolicyPut
	NotifyRetentionPolicyDeleted
	NotifyIngesterPut
	NotifyIngesterDeleted
	NotifySettingPut
)

// Notification describes a config mutation that the FSM just applied.
type Notification struct {
	Kind NotifyKind
	ID   uuid.UUID // entity ID (zero for settings)
	Key  string    // settings key (empty for entity mutations)
}

// Option configures the FSM at construction time.
type Option func(*FSM)

// WithOnApply registers a callback that fires synchronously after the FSM
// successfully applies a config-entity command. The callback runs inside
// raft.Apply, so it completes before the cfgStore write method returns.
func WithOnApply(fn func(Notification)) Option {
	return func(f *FSM) {
		f.onApply = fn
	}
}

// FSM implements raft.FSM by dispatching deserialized ConfigCommands to an
// in-memory config store.
type FSM struct {
	store   *memory.Store
	onApply func(Notification)
}

var _ raft.FSM = (*FSM)(nil)

// New creates a new FSM with a fresh in-memory store.
func New(opts ...Option) *FSM {
	f := &FSM{store: memory.NewStore()}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Store returns the underlying in-memory store for serving reads.
func (f *FSM) Store() *memory.Store {
	return f.store
}

// Apply deserializes a committed Raft log entry and dispatches to the store.
// Returns nil on success or an error on failure.
func (f *FSM) Apply(l *raft.Log) any {
	cmd, err := command.Unmarshal(l.Data)
	if err != nil {
		return fmt.Errorf("unmarshal config command: %w", err)
	}

	ctx := context.Background()

	switch cmd.Command.(type) {
	case *gastrologv1.ConfigCommand_PutFilter,
		*gastrologv1.ConfigCommand_DeleteFilter,
		*gastrologv1.ConfigCommand_PutRotationPolicy,
		*gastrologv1.ConfigCommand_DeleteRotationPolicy,
		*gastrologv1.ConfigCommand_PutRetentionPolicy,
		*gastrologv1.ConfigCommand_DeleteRetentionPolicy,
		*gastrologv1.ConfigCommand_PutVault,
		*gastrologv1.ConfigCommand_DeleteVault,
		*gastrologv1.ConfigCommand_PutIngester,
		*gastrologv1.ConfigCommand_DeleteIngester,
		*gastrologv1.ConfigCommand_PutSetting,
		*gastrologv1.ConfigCommand_DeleteSetting,
		*gastrologv1.ConfigCommand_PutCertificate,
		*gastrologv1.ConfigCommand_DeleteCertificate,
		*gastrologv1.ConfigCommand_PutNodeConfig,
		*gastrologv1.ConfigCommand_DeleteNodeConfig:
		return f.applyConfig(ctx, cmd)

	case *gastrologv1.ConfigCommand_CreateUser,
		*gastrologv1.ConfigCommand_UpdatePassword,
		*gastrologv1.ConfigCommand_UpdateUserRole,
		*gastrologv1.ConfigCommand_UpdateUsername,
		*gastrologv1.ConfigCommand_DeleteUser,
		*gastrologv1.ConfigCommand_InvalidateTokens,
		*gastrologv1.ConfigCommand_PutUserPreferences:
		return f.applyUser(ctx, cmd)

	case *gastrologv1.ConfigCommand_CreateRefreshToken,
		*gastrologv1.ConfigCommand_DeleteRefreshToken,
		*gastrologv1.ConfigCommand_DeleteUserRefreshTokens:
		return f.applyRefreshToken(ctx, cmd)

	default:
		return fmt.Errorf("unknown config command type: %T", cmd.Command)
	}
}

// applyConfig dispatches config-entity commands (filters, policies, vaults,
// ingesters, settings, certificates) and fires a notification on success.
func (f *FSM) applyConfig(ctx context.Context, cmd *gastrologv1.ConfigCommand) error {
	note, err := f.dispatchConfig(ctx, cmd)
	if err != nil {
		return err
	}
	if note != nil && f.onApply != nil {
		f.onApply(*note)
	}
	return nil
}

// dispatchConfig routes a config command to the store and returns a
// notification describing the mutation, or nil for commands that don't
// need orchestrator side effects (settings delete, certificates).
func (f *FSM) dispatchConfig(ctx context.Context, cmd *gastrologv1.ConfigCommand) (*Notification, error) {
	switch c := cmd.Command.(type) {
	case *gastrologv1.ConfigCommand_PutFilter:
		return f.applyPutFilter(ctx, c.PutFilter)
	case *gastrologv1.ConfigCommand_DeleteFilter:
		return f.applyDeleteFilter(ctx, c.DeleteFilter)
	case *gastrologv1.ConfigCommand_PutRotationPolicy:
		return f.applyPutRotationPolicy(ctx, c.PutRotationPolicy)
	case *gastrologv1.ConfigCommand_DeleteRotationPolicy:
		return f.applyDeleteRotationPolicy(ctx, c.DeleteRotationPolicy)
	case *gastrologv1.ConfigCommand_PutRetentionPolicy:
		return f.applyPutRetentionPolicy(ctx, c.PutRetentionPolicy)
	case *gastrologv1.ConfigCommand_DeleteRetentionPolicy:
		return f.applyDeleteRetentionPolicy(ctx, c.DeleteRetentionPolicy)
	case *gastrologv1.ConfigCommand_PutVault:
		return f.applyPutVault(ctx, c.PutVault)
	case *gastrologv1.ConfigCommand_DeleteVault:
		return f.applyDeleteVault(ctx, c.DeleteVault)
	case *gastrologv1.ConfigCommand_PutIngester:
		return f.applyPutIngester(ctx, c.PutIngester)
	case *gastrologv1.ConfigCommand_DeleteIngester:
		return f.applyDeleteIngester(ctx, c.DeleteIngester)
	case *gastrologv1.ConfigCommand_PutSetting:
		return f.applyPutSetting(ctx, c.PutSetting)
	case *gastrologv1.ConfigCommand_DeleteSetting:
		// No-op: settings KV was removed, but we keep this case for backward compat
		// with old raft log entries.
		return nil, nil
	case *gastrologv1.ConfigCommand_PutCertificate:
		cert, err := command.ExtractPutCertificate(c.PutCertificate)
		if err != nil {
			return nil, err
		}
		return nil, f.store.PutCertificate(ctx, cert)
	case *gastrologv1.ConfigCommand_DeleteCertificate:
		id, err := command.ExtractDeleteCertificate(c.DeleteCertificate)
		if err != nil {
			return nil, err
		}
		return nil, f.store.DeleteCertificate(ctx, id)
	case *gastrologv1.ConfigCommand_PutNodeConfig:
		node, err := command.ExtractPutNodeConfig(c.PutNodeConfig)
		if err != nil {
			return nil, err
		}
		return nil, f.store.PutNode(ctx, node)
	case *gastrologv1.ConfigCommand_DeleteNodeConfig:
		id, err := command.ExtractDeleteNodeConfig(c.DeleteNodeConfig)
		if err != nil {
			return nil, err
		}
		return nil, f.store.DeleteNode(ctx, id)
	default:
		return nil, fmt.Errorf("unexpected config command: %T", c)
	}
}

func (f *FSM) applyPutFilter(ctx context.Context, pb *gastrologv1.PutFilterCommand) (*Notification, error) {
	cfg, err := command.ExtractPutFilter(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutFilter(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyFilterPut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteFilter(ctx context.Context, pb *gastrologv1.DeleteFilterCommand) (*Notification, error) {
	id, err := command.ExtractDeleteFilter(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteFilter(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyFilterDeleted, ID: id}, nil
}

func (f *FSM) applyPutRotationPolicy(ctx context.Context, pb *gastrologv1.PutRotationPolicyCommand) (*Notification, error) {
	cfg, err := command.ExtractPutRotationPolicy(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutRotationPolicy(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRotationPolicyPut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteRotationPolicy(ctx context.Context, pb *gastrologv1.DeleteRotationPolicyCommand) (*Notification, error) {
	id, err := command.ExtractDeleteRotationPolicy(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteRotationPolicy(ctx, id); err != nil {
		return nil, err
	}
	if err := f.cascadeDeleteRotationPolicy(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRotationPolicyDeleted, ID: id}, nil
}

func (f *FSM) applyPutRetentionPolicy(ctx context.Context, pb *gastrologv1.PutRetentionPolicyCommand) (*Notification, error) {
	cfg, err := command.ExtractPutRetentionPolicy(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutRetentionPolicy(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRetentionPolicyPut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteRetentionPolicy(ctx context.Context, pb *gastrologv1.DeleteRetentionPolicyCommand) (*Notification, error) {
	id, err := command.ExtractDeleteRetentionPolicy(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteRetentionPolicy(ctx, id); err != nil {
		return nil, err
	}
	if err := f.cascadeDeleteRetentionPolicy(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRetentionPolicyDeleted, ID: id}, nil
}

func (f *FSM) applyPutVault(ctx context.Context, pb *gastrologv1.PutVaultCommand) (*Notification, error) {
	cfg, err := command.ExtractPutVault(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutVault(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyVaultPut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteVault(ctx context.Context, pb *gastrologv1.DeleteVaultCommand) (*Notification, error) {
	id, err := command.ExtractDeleteVault(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteVault(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyVaultDeleted, ID: id}, nil
}

func (f *FSM) applyPutIngester(ctx context.Context, pb *gastrologv1.PutIngesterCommand) (*Notification, error) {
	cfg, err := command.ExtractPutIngester(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutIngester(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyIngesterPut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteIngester(ctx context.Context, pb *gastrologv1.DeleteIngesterCommand) (*Notification, error) {
	id, err := command.ExtractDeleteIngester(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteIngester(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyIngesterDeleted, ID: id}, nil
}

func (f *FSM) applyPutSetting(ctx context.Context, pb *gastrologv1.PutSettingCommand) (*Notification, error) {
	key, value := command.ExtractPutSetting(pb)
	if key != "server" {
		// Non-server settings were never used; ignore for backward compat.
		return nil, nil
	}
	auth, query, sched, tls, lookup, dismissed, err := command.ExtractPutServerSettings(value)
	if err != nil {
		return nil, err
	}
	if err := f.store.SaveServerSettings(ctx, auth, query, sched, tls, lookup, dismissed); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifySettingPut, Key: key}, nil
}

// applyUser dispatches user-management commands.
func (f *FSM) applyUser(ctx context.Context, cmd *gastrologv1.ConfigCommand) error {
	switch c := cmd.Command.(type) {
	case *gastrologv1.ConfigCommand_CreateUser:
		user, err := command.ExtractCreateUser(c.CreateUser)
		if err != nil {
			return err
		}
		return f.store.CreateUser(ctx, user)

	case *gastrologv1.ConfigCommand_UpdatePassword:
		id, hash, err := command.ExtractUpdatePassword(c.UpdatePassword)
		if err != nil {
			return err
		}
		return f.store.UpdatePassword(ctx, id, hash)

	case *gastrologv1.ConfigCommand_UpdateUserRole:
		id, role, err := command.ExtractUpdateUserRole(c.UpdateUserRole)
		if err != nil {
			return err
		}
		return f.store.UpdateUserRole(ctx, id, role)

	case *gastrologv1.ConfigCommand_UpdateUsername:
		id, username, err := command.ExtractUpdateUsername(c.UpdateUsername)
		if err != nil {
			return err
		}
		return f.store.UpdateUsername(ctx, id, username)

	case *gastrologv1.ConfigCommand_DeleteUser:
		id, err := command.ExtractDeleteUser(c.DeleteUser)
		if err != nil {
			return err
		}
		return f.store.DeleteUser(ctx, id)

	case *gastrologv1.ConfigCommand_InvalidateTokens:
		id, at, err := command.ExtractInvalidateTokens(c.InvalidateTokens)
		if err != nil {
			return err
		}
		return f.store.InvalidateTokens(ctx, id, at)

	case *gastrologv1.ConfigCommand_PutUserPreferences:
		id, prefs, err := command.ExtractPutUserPreferences(c.PutUserPreferences)
		if err != nil {
			return err
		}
		return f.store.PutUserPreferences(ctx, id, prefs)

	default:
		return fmt.Errorf("unexpected user command: %T", c)
	}
}

// applyRefreshToken dispatches refresh-token commands.
func (f *FSM) applyRefreshToken(ctx context.Context, cmd *gastrologv1.ConfigCommand) error {
	switch c := cmd.Command.(type) {
	case *gastrologv1.ConfigCommand_CreateRefreshToken:
		token, err := command.ExtractCreateRefreshToken(c.CreateRefreshToken)
		if err != nil {
			return err
		}
		return f.store.CreateRefreshToken(ctx, token)

	case *gastrologv1.ConfigCommand_DeleteRefreshToken:
		id, err := command.ExtractDeleteRefreshToken(c.DeleteRefreshToken)
		if err != nil {
			return err
		}
		return f.store.DeleteRefreshToken(ctx, id)

	case *gastrologv1.ConfigCommand_DeleteUserRefreshTokens:
		userID, err := command.ExtractDeleteUserRefreshTokens(c.DeleteUserRefreshTokens)
		if err != nil {
			return err
		}
		return f.store.DeleteUserRefreshTokens(ctx, userID)

	default:
		return fmt.Errorf("unexpected refresh token command: %T", c)
	}
}

// cascadeDeleteRotationPolicy clears rotation policy references from vaults.
func (f *FSM) cascadeDeleteRotationPolicy(ctx context.Context, policyID uuid.UUID) error {
	vaults, err := f.store.ListVaults(ctx)
	if err != nil {
		return fmt.Errorf("list vaults for cascade: %w", err)
	}
	for _, v := range vaults {
		if v.Policy != nil && *v.Policy == policyID {
			v.Policy = nil
			if err := f.store.PutVault(ctx, v); err != nil {
				return fmt.Errorf("cascade update vault %s: %w", v.ID, err)
			}
		}
	}
	return nil
}

// cascadeDeleteRetentionPolicy removes retention rules referencing the policy from vaults.
func (f *FSM) cascadeDeleteRetentionPolicy(ctx context.Context, policyID uuid.UUID) error {
	vaults, err := f.store.ListVaults(ctx)
	if err != nil {
		return fmt.Errorf("list vaults for cascade: %w", err)
	}
	for _, v := range vaults {
		modified := false
		filtered := v.RetentionRules[:0]
		for _, rule := range v.RetentionRules {
			if rule.RetentionPolicyID == policyID {
				modified = true
				continue
			}
			filtered = append(filtered, rule)
		}
		if modified {
			v.RetentionRules = filtered
			if err := f.store.PutVault(ctx, v); err != nil {
				return fmt.Errorf("cascade update vault %s: %w", v.ID, err)
			}
		}
	}
	return nil
}

// Snapshot captures the current config state for Raft log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	ctx := context.Background()

	cfg, err := f.store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load config for snapshot: %w", err)
	}
	if cfg == nil {
		cfg = &config.Config{}
	}

	users, err := f.store.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users for snapshot: %w", err)
	}

	tokens, err := f.store.ListRefreshTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("list refresh tokens for snapshot: %w", err)
	}

	nodes, err := f.store.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes for snapshot: %w", err)
	}

	snap := command.BuildSnapshot(cfg, users, tokens, nodes)
	data, err := command.MarshalSnapshot(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM's state with a snapshot.
// Raft guarantees this is never called concurrently with Apply or Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error { //nolint:gocognit // snapshot restore is inherently complex
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	snap, err := command.UnmarshalSnapshot(data)
	if err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	cfg, users, tokens, nodes, err := command.RestoreSnapshot(snap)
	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	newStore := memory.NewStore()
	ctx := context.Background()

	// Populate config entities.
	for _, fc := range cfg.Filters {
		if err := newStore.PutFilter(ctx, fc); err != nil {
			return fmt.Errorf("restore filter %s: %w", fc.ID, err)
		}
	}
	for _, rp := range cfg.RotationPolicies {
		if err := newStore.PutRotationPolicy(ctx, rp); err != nil {
			return fmt.Errorf("restore rotation policy %s: %w", rp.ID, err)
		}
	}
	for _, rp := range cfg.RetentionPolicies {
		if err := newStore.PutRetentionPolicy(ctx, rp); err != nil {
			return fmt.Errorf("restore retention policy %s: %w", rp.ID, err)
		}
	}
	for _, v := range cfg.Vaults {
		if err := newStore.PutVault(ctx, v); err != nil {
			return fmt.Errorf("restore vault %s: %w", v.ID, err)
		}
	}
	for _, ing := range cfg.Ingesters {
		if err := newStore.PutIngester(ctx, ing); err != nil {
			return fmt.Errorf("restore ingester %s: %w", ing.ID, err)
		}
	}
	// Restore server settings from Config fields (populated by RestoreSnapshot).
	// Only call SaveServerSettings if the snapshot actually contained settings;
	// otherwise the empty save would make Load() return a non-nil Config.
	if settings := snap.GetSettings(); len(settings) > 0 {
		if _, ok := settings["server"]; ok {
			if err := newStore.SaveServerSettings(ctx, cfg.Auth, cfg.Query, cfg.Scheduler, cfg.TLS, cfg.Lookup, cfg.SetupWizardDismissed); err != nil {
				return fmt.Errorf("restore server settings: %w", err)
			}
		}
	}
	for _, cert := range cfg.Certs {
		if err := newStore.PutCertificate(ctx, cert); err != nil {
			return fmt.Errorf("restore certificate %s: %w", cert.ID, err)
		}
	}

	// Populate users.
	for _, u := range users {
		if err := newStore.CreateUser(ctx, u); err != nil {
			return fmt.Errorf("restore user %s: %w", u.ID, err)
		}
	}

	// Populate refresh tokens.
	for _, t := range tokens {
		if err := newStore.CreateRefreshToken(ctx, t); err != nil {
			return fmt.Errorf("restore refresh token %s: %w", t.ID, err)
		}
	}

	// Populate nodes.
	for _, n := range nodes {
		if err := newStore.PutNode(ctx, n); err != nil {
			return fmt.Errorf("restore node %s: %w", n.ID, err)
		}
	}

	f.store = newStore
	return nil
}

// fsmSnapshot holds serialized snapshot data.
type fsmSnapshot struct {
	data []byte
}

var _ raft.FSMSnapshot = (*fsmSnapshot)(nil)

// Persist writes the snapshot data to the sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}
	return sink.Close()
}

// Release is a no-op.
func (s *fsmSnapshot) Release() {}
