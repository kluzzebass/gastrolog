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

// FSM implements raft.FSM by dispatching deserialized ConfigCommands to an
// in-memory config store.
type FSM struct {
	store *memory.Store
}

var _ raft.FSM = (*FSM)(nil)

// New creates a new FSM with a fresh in-memory store.
func New() *FSM {
	return &FSM{store: memory.NewStore()}
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
		*gastrologv1.ConfigCommand_DeleteCertificate:
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
// ingesters, settings, certificates).
func (f *FSM) applyConfig(ctx context.Context, cmd *gastrologv1.ConfigCommand) error {
	switch c := cmd.Command.(type) {
	case *gastrologv1.ConfigCommand_PutFilter:
		cfg, err := command.ExtractPutFilter(c.PutFilter)
		if err != nil {
			return err
		}
		return f.store.PutFilter(ctx, cfg)

	case *gastrologv1.ConfigCommand_DeleteFilter:
		id, err := command.ExtractDeleteFilter(c.DeleteFilter)
		if err != nil {
			return err
		}
		return f.store.DeleteFilter(ctx, id)

	case *gastrologv1.ConfigCommand_PutRotationPolicy:
		cfg, err := command.ExtractPutRotationPolicy(c.PutRotationPolicy)
		if err != nil {
			return err
		}
		return f.store.PutRotationPolicy(ctx, cfg)

	case *gastrologv1.ConfigCommand_DeleteRotationPolicy:
		id, err := command.ExtractDeleteRotationPolicy(c.DeleteRotationPolicy)
		if err != nil {
			return err
		}
		if err := f.store.DeleteRotationPolicy(ctx, id); err != nil {
			return err
		}
		return f.cascadeDeleteRotationPolicy(ctx, id)

	case *gastrologv1.ConfigCommand_PutRetentionPolicy:
		cfg, err := command.ExtractPutRetentionPolicy(c.PutRetentionPolicy)
		if err != nil {
			return err
		}
		return f.store.PutRetentionPolicy(ctx, cfg)

	case *gastrologv1.ConfigCommand_DeleteRetentionPolicy:
		id, err := command.ExtractDeleteRetentionPolicy(c.DeleteRetentionPolicy)
		if err != nil {
			return err
		}
		if err := f.store.DeleteRetentionPolicy(ctx, id); err != nil {
			return err
		}
		return f.cascadeDeleteRetentionPolicy(ctx, id)

	case *gastrologv1.ConfigCommand_PutVault:
		cfg, err := command.ExtractPutVault(c.PutVault)
		if err != nil {
			return err
		}
		return f.store.PutVault(ctx, cfg)

	case *gastrologv1.ConfigCommand_DeleteVault:
		id, err := command.ExtractDeleteVault(c.DeleteVault)
		if err != nil {
			return err
		}
		return f.store.DeleteVault(ctx, id)

	case *gastrologv1.ConfigCommand_PutIngester:
		cfg, err := command.ExtractPutIngester(c.PutIngester)
		if err != nil {
			return err
		}
		return f.store.PutIngester(ctx, cfg)

	case *gastrologv1.ConfigCommand_DeleteIngester:
		id, err := command.ExtractDeleteIngester(c.DeleteIngester)
		if err != nil {
			return err
		}
		return f.store.DeleteIngester(ctx, id)

	case *gastrologv1.ConfigCommand_PutSetting:
		key, value := command.ExtractPutSetting(c.PutSetting)
		return f.store.PutSetting(ctx, key, value)

	case *gastrologv1.ConfigCommand_DeleteSetting:
		key := command.ExtractDeleteSetting(c.DeleteSetting)
		return f.store.DeleteSetting(ctx, key)

	case *gastrologv1.ConfigCommand_PutCertificate:
		cert, err := command.ExtractPutCertificate(c.PutCertificate)
		if err != nil {
			return err
		}
		return f.store.PutCertificate(ctx, cert)

	case *gastrologv1.ConfigCommand_DeleteCertificate:
		id, err := command.ExtractDeleteCertificate(c.DeleteCertificate)
		if err != nil {
			return err
		}
		return f.store.DeleteCertificate(ctx, id)

	default:
		return fmt.Errorf("unexpected config command: %T", c)
	}
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

	snap := command.BuildSnapshot(cfg, users, tokens)
	data, err := command.MarshalSnapshot(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM's state with a snapshot.
// Raft guarantees this is never called concurrently with Apply or Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	snap, err := command.UnmarshalSnapshot(data)
	if err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	cfg, users, tokens, err := command.RestoreSnapshot(snap)
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
	for key, value := range cfg.Settings {
		if err := newStore.PutSetting(ctx, key, value); err != nil {
			return fmt.Errorf("restore setting %q: %w", key, err)
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
