// Package raftfsm provides a Raft FSM that applies config commands to an
// in-memory config store. It bridges Raft's replicated log with the config
// system, handling command dispatch, snapshots for log compaction, and
// restore for follower catch-up.
package raftfsm

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"io"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"
	"gastrolog/internal/system/memory"

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
	NotifyClusterTLSPut
	NotifyRoutePut
	NotifyRouteDeleted
	NotifyNodeConfigPut
	NotifyNodeConfigDeleted
	NotifyManagedFilePut
	NotifyManagedFileDeleted
	NotifyCloudServicePut
	NotifyCloudServiceDeleted
	NotifyNodeStorageConfigSet
	NotifyTierPut
	NotifyTierDeleted
	NotifyTierPlacementsSet
	NotifyIngesterAliveSet
	NotifySetupWizardDismissedSet
)

// Notification describes a config mutation that the FSM just applied.
type Notification struct {
	Kind       NotifyKind
	ID         glid.GLID // entity ID (zero for settings)
	Name       string    // entity name (populated on deletes where config is read pre-delete)
	Key        string    // settings key (empty for entity mutations)
	NodeID     string    // owning node (populated on vault deletes)
	NodeIDs    []string  // allowed nodes (populated on ingester deletes)
	Dir        string    // file vault directory (populated on file vault deletes)
	DeleteData bool      // when true, vault data directory should be removed from disk
	Drain      bool      // when true, drain tier data to next tier before deleting
	Index      uint64    // Raft log index of this mutation (monotonically increasing config version)
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
	case *gastrologv1.SystemCommand_PutFilter,
		*gastrologv1.SystemCommand_DeleteFilter,
		*gastrologv1.SystemCommand_PutRotationPolicy,
		*gastrologv1.SystemCommand_DeleteRotationPolicy,
		*gastrologv1.SystemCommand_PutRetentionPolicy,
		*gastrologv1.SystemCommand_DeleteRetentionPolicy,
		*gastrologv1.SystemCommand_PutVault,
		*gastrologv1.SystemCommand_DeleteVault,
		*gastrologv1.SystemCommand_PutIngester,
		*gastrologv1.SystemCommand_DeleteIngester,
		*gastrologv1.SystemCommand_PutSetting,
		*gastrologv1.SystemCommand_DeleteSetting,
		*gastrologv1.SystemCommand_PutCertificate,
		*gastrologv1.SystemCommand_DeleteCertificate,
		*gastrologv1.SystemCommand_PutNodeConfig,
		*gastrologv1.SystemCommand_DeleteNodeConfig,
		*gastrologv1.SystemCommand_PutClusterTls,
		*gastrologv1.SystemCommand_PutRoute,
		*gastrologv1.SystemCommand_DeleteRoute,
		*gastrologv1.SystemCommand_PutManagedFile,
		*gastrologv1.SystemCommand_DeleteManagedFile,
		*gastrologv1.SystemCommand_PutCloudService,
		*gastrologv1.SystemCommand_DeleteCloudService,
		*gastrologv1.SystemCommand_SetNodeStorageConfig,
		*gastrologv1.SystemCommand_PutTier,
		*gastrologv1.SystemCommand_DeleteTier,
		*gastrologv1.SystemCommand_SetTierPlacements,
		*gastrologv1.SystemCommand_SetIngesterAlive,
		*gastrologv1.SystemCommand_SetSetupWizardDismissed:
		return f.applyConfig(ctx, cmd, l.Index)

	case *gastrologv1.SystemCommand_CreateUser,
		*gastrologv1.SystemCommand_UpdatePassword,
		*gastrologv1.SystemCommand_UpdateUserRole,
		*gastrologv1.SystemCommand_UpdateUsername,
		*gastrologv1.SystemCommand_DeleteUser,
		*gastrologv1.SystemCommand_InvalidateTokens,
		*gastrologv1.SystemCommand_PutUserPreferences:
		return f.applyUser(ctx, cmd)

	case *gastrologv1.SystemCommand_CreateRefreshToken,
		*gastrologv1.SystemCommand_DeleteRefreshToken,
		*gastrologv1.SystemCommand_DeleteUserRefreshTokens:
		return f.applyRefreshToken(ctx, cmd)

	default:
		return fmt.Errorf("unknown config command type: %T", cmd.Command)
	}
}

// applyConfig dispatches config-entity commands (filters, policies, vaults,
// ingesters, settings, certificates) and fires a notification on success.
// The raftIndex is the Raft log index for this entry, threaded through
// to the notification so the dispatcher can broadcast it as a config version.
func (f *FSM) applyConfig(ctx context.Context, cmd *gastrologv1.SystemCommand, raftIndex uint64) error {
	note, err := f.dispatchConfig(ctx, cmd)
	if err != nil {
		return err
	}
	if note != nil && f.onApply != nil {
		note.Index = raftIndex
		f.onApply(*note)
	}
	return nil
}

// dispatchConfig routes a config command to the store and returns a
// notification describing the mutation, or nil for commands that don't
// need orchestrator side effects (settings delete, certificates).
func (f *FSM) dispatchConfig(ctx context.Context, cmd *gastrologv1.SystemCommand) (*Notification, error) { //nolint:gocyclo // flat dispatch, grows linearly with command count
	switch c := cmd.Command.(type) {
	case *gastrologv1.SystemCommand_PutFilter:
		return f.applyPutFilter(ctx, c.PutFilter)
	case *gastrologv1.SystemCommand_DeleteFilter:
		return f.applyDeleteFilter(ctx, c.DeleteFilter)
	case *gastrologv1.SystemCommand_PutRotationPolicy:
		return f.applyPutRotationPolicy(ctx, c.PutRotationPolicy)
	case *gastrologv1.SystemCommand_DeleteRotationPolicy:
		return f.applyDeleteRotationPolicy(ctx, c.DeleteRotationPolicy)
	case *gastrologv1.SystemCommand_PutRetentionPolicy:
		return f.applyPutRetentionPolicy(ctx, c.PutRetentionPolicy)
	case *gastrologv1.SystemCommand_DeleteRetentionPolicy:
		return f.applyDeleteRetentionPolicy(ctx, c.DeleteRetentionPolicy)
	case *gastrologv1.SystemCommand_PutVault:
		return f.applyPutVault(ctx, c.PutVault)
	case *gastrologv1.SystemCommand_DeleteVault:
		return f.applyDeleteVault(ctx, c.DeleteVault)
	case *gastrologv1.SystemCommand_PutIngester:
		return f.applyPutIngester(ctx, c.PutIngester)
	case *gastrologv1.SystemCommand_DeleteIngester:
		return f.applyDeleteIngester(ctx, c.DeleteIngester)
	case *gastrologv1.SystemCommand_PutSetting:
		return f.applyPutSetting(ctx, c.PutSetting)
	case *gastrologv1.SystemCommand_DeleteSetting:
		// No-op: settings KV was removed, but we keep this case for backward compat
		// with old raft log entries.
		return nil, nil
	case *gastrologv1.SystemCommand_PutCertificate:
		cert, err := command.ExtractPutCertificate(c.PutCertificate)
		if err != nil {
			return nil, err
		}
		return nil, f.store.PutCertificate(ctx, cert)
	case *gastrologv1.SystemCommand_DeleteCertificate:
		id, err := command.ExtractDeleteCertificate(c.DeleteCertificate)
		if err != nil {
			return nil, err
		}
		return nil, f.store.DeleteCertificate(ctx, id)
	case *gastrologv1.SystemCommand_PutNodeConfig:
		node, err := command.ExtractPutNodeConfig(c.PutNodeConfig)
		if err != nil {
			return nil, err
		}
		if err := f.store.PutNode(ctx, node); err != nil {
			return nil, err
		}
		return &Notification{Kind: NotifyNodeConfigPut, ID: node.ID}, nil
	case *gastrologv1.SystemCommand_DeleteNodeConfig:
		id, err := command.ExtractDeleteNodeConfig(c.DeleteNodeConfig)
		if err != nil {
			return nil, err
		}
		if err := f.store.DeleteNode(ctx, id); err != nil {
			return nil, err
		}
		return &Notification{Kind: NotifyNodeConfigDeleted, ID: id}, nil
	case *gastrologv1.SystemCommand_PutClusterTls:
		tls := command.ExtractPutClusterTLS(c.PutClusterTls)
		if err := f.store.PutClusterTLS(ctx, tls); err != nil {
			return nil, err
		}
		return &Notification{Kind: NotifyClusterTLSPut}, nil
	case *gastrologv1.SystemCommand_PutRoute:
		return f.applyPutRoute(ctx, c.PutRoute)
	case *gastrologv1.SystemCommand_DeleteRoute:
		return f.applyDeleteRoute(ctx, c.DeleteRoute)
	case *gastrologv1.SystemCommand_PutManagedFile:
		return f.applyPutManagedFile(ctx, c.PutManagedFile)
	case *gastrologv1.SystemCommand_DeleteManagedFile:
		return f.applyDeleteManagedFile(ctx, c.DeleteManagedFile)
	case *gastrologv1.SystemCommand_PutCloudService:
		return f.applyPutCloudService(ctx, c.PutCloudService)
	case *gastrologv1.SystemCommand_DeleteCloudService:
		return f.applyDeleteCloudService(ctx, c.DeleteCloudService)
	case *gastrologv1.SystemCommand_SetNodeStorageConfig:
		return f.applySetNodeStorageConfig(ctx, c.SetNodeStorageConfig)
	case *gastrologv1.SystemCommand_PutTier:
		return f.applyPutTier(ctx, c.PutTier)
	case *gastrologv1.SystemCommand_DeleteTier:
		return f.applyDeleteTier(ctx, c.DeleteTier)
	case *gastrologv1.SystemCommand_SetTierPlacements:
		return f.applySetTierPlacements(ctx, c.SetTierPlacements)
	case *gastrologv1.SystemCommand_SetIngesterAlive:
		return f.applySetIngesterAlive(ctx, c.SetIngesterAlive)
	case *gastrologv1.SystemCommand_SetSetupWizardDismissed:
		if err := f.store.SetSetupWizardDismissed(ctx, c.SetSetupWizardDismissed.GetDismissed()); err != nil {
			return nil, err
		}
		return &Notification{Kind: NotifySetupWizardDismissedSet}, nil
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
	// Read vault config before deleting — the dispatcher needs the name
	// for cleanup logging.
	note := &Notification{Kind: NotifyVaultDeleted, ID: id, DeleteData: pb.GetDeleteData()}
	if existing, _ := f.store.GetVault(ctx, id); existing != nil {
		note.Name = existing.Name
	}
	if err := f.store.DeleteVault(ctx, id, false); err != nil {
		return nil, err
	}
	return note, nil
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
	// Read ingester config before deleting so the dispatcher has name/node info.
	note := &Notification{Kind: NotifyIngesterDeleted, ID: id}
	if existing, _ := f.store.GetIngester(ctx, id); existing != nil {
		note.Name = existing.Name
		note.NodeIDs = existing.NodeIDs
	}
	if err := f.store.DeleteIngester(ctx, id); err != nil {
		return nil, err
	}
	return note, nil
}

func (f *FSM) applyPutSetting(ctx context.Context, pb *gastrologv1.PutSettingCommand) (*Notification, error) {
	key, value := command.ExtractPutSetting(pb)
	if key != "server" {
		// Non-server settings were never used; ignore for backward compat.
		return nil, nil
	}
	ss, err := command.ExtractPutServerSettings(value)
	if err != nil {
		return nil, err
	}
	if err := f.store.SaveServerSettings(ctx, ss); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifySettingPut, Key: key}, nil
}

func (f *FSM) applyPutRoute(ctx context.Context, pb *gastrologv1.PutRouteCommand) (*Notification, error) {
	cfg, err := command.ExtractPutRoute(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutRoute(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRoutePut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteRoute(ctx context.Context, pb *gastrologv1.DeleteRouteCommand) (*Notification, error) {
	id, err := command.ExtractDeleteRoute(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteRoute(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyRouteDeleted, ID: id}, nil
}

func (f *FSM) applyPutManagedFile(ctx context.Context, pb *gastrologv1.PutManagedFileCommand) (*Notification, error) {
	cfg, err := command.ExtractPutManagedFile(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutManagedFile(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyManagedFilePut, ID: cfg.ID}, nil
}

func (f *FSM) applyDeleteManagedFile(ctx context.Context, pb *gastrologv1.DeleteManagedFileCommand) (*Notification, error) {
	id, err := command.ExtractDeleteManagedFile(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteManagedFile(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyManagedFileDeleted, ID: id}, nil
}

func (f *FSM) applyPutCloudService(ctx context.Context, pb *gastrologv1.PutCloudServiceCommand) (*Notification, error) {
	svc, err := command.ExtractPutCloudService(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutCloudService(ctx, svc); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyCloudServicePut, ID: svc.ID}, nil
}

func (f *FSM) applyDeleteCloudService(ctx context.Context, pb *gastrologv1.DeleteCloudServiceCommand) (*Notification, error) {
	id, err := command.ExtractDeleteCloudService(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteCloudService(ctx, id); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyCloudServiceDeleted, ID: id}, nil
}

func (f *FSM) applySetNodeStorageConfig(ctx context.Context, pb *gastrologv1.SetNodeStorageConfigCommand) (*Notification, error) {
	cfg, err := command.ExtractSetNodeStorageConfig(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.SetNodeStorageConfig(ctx, cfg); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyNodeStorageConfigSet}, nil
}

func (f *FSM) applyPutTier(ctx context.Context, pb *gastrologv1.PutTierCommand) (*Notification, error) {
	tier, err := command.ExtractPutTier(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.PutTier(ctx, tier); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyTierPut, ID: tier.ID}, nil
}

func (f *FSM) applyDeleteTier(ctx context.Context, pb *gastrologv1.DeleteTierCommand) (*Notification, error) {
	id, err := command.ExtractDeleteTier(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.DeleteTier(ctx, id, pb.GetDrain()); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyTierDeleted, ID: id, Drain: pb.GetDrain()}, nil
}

func (f *FSM) applySetTierPlacements(ctx context.Context, pb *gastrologv1.SetTierPlacementsCommand) (*Notification, error) {
	tierID, placements, err := command.ExtractSetTierPlacements(pb)
	if err != nil {
		return nil, err
	}
	if err := f.store.SetTierPlacements(ctx, tierID, placements); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyTierPlacementsSet, ID: tierID}, nil
}

func (f *FSM) applySetIngesterAlive(ctx context.Context, cmd *gastrologv1.SetIngesterAliveCommand) (*Notification, error) {
	ingesterID := glid.FromBytes(cmd.GetIngesterId())
	if err := f.store.SetIngesterAlive(ctx, ingesterID, cmd.GetNodeId(), cmd.GetAlive()); err != nil {
		return nil, err
	}
	return &Notification{Kind: NotifyIngesterAliveSet, ID: ingesterID}, nil
}

// applyUser dispatches user-management commands.
func (f *FSM) applyUser(ctx context.Context, cmd *gastrologv1.SystemCommand) error {
	switch c := cmd.Command.(type) {
	case *gastrologv1.SystemCommand_CreateUser:
		user, err := command.ExtractCreateUser(c.CreateUser)
		if err != nil {
			return err
		}
		return f.store.CreateUser(ctx, user)

	case *gastrologv1.SystemCommand_UpdatePassword:
		id, hash, err := command.ExtractUpdatePassword(c.UpdatePassword)
		if err != nil {
			return err
		}
		return f.store.UpdatePassword(ctx, id, hash)

	case *gastrologv1.SystemCommand_UpdateUserRole:
		id, role, err := command.ExtractUpdateUserRole(c.UpdateUserRole)
		if err != nil {
			return err
		}
		return f.store.UpdateUserRole(ctx, id, role)

	case *gastrologv1.SystemCommand_UpdateUsername:
		id, username, err := command.ExtractUpdateUsername(c.UpdateUsername)
		if err != nil {
			return err
		}
		return f.store.UpdateUsername(ctx, id, username)

	case *gastrologv1.SystemCommand_DeleteUser:
		id, err := command.ExtractDeleteUser(c.DeleteUser)
		if err != nil {
			return err
		}
		return f.store.DeleteUser(ctx, id)

	case *gastrologv1.SystemCommand_InvalidateTokens:
		id, at, err := command.ExtractInvalidateTokens(c.InvalidateTokens)
		if err != nil {
			return err
		}
		return f.store.InvalidateTokens(ctx, id, at)

	case *gastrologv1.SystemCommand_PutUserPreferences:
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
func (f *FSM) applyRefreshToken(ctx context.Context, cmd *gastrologv1.SystemCommand) error {
	switch c := cmd.Command.(type) {
	case *gastrologv1.SystemCommand_CreateRefreshToken:
		token, err := command.ExtractCreateRefreshToken(c.CreateRefreshToken)
		if err != nil {
			return err
		}
		return f.store.CreateRefreshToken(ctx, token)

	case *gastrologv1.SystemCommand_DeleteRefreshToken:
		id, err := command.ExtractDeleteRefreshToken(c.DeleteRefreshToken)
		if err != nil {
			return err
		}
		return f.store.DeleteRefreshToken(ctx, id)

	case *gastrologv1.SystemCommand_DeleteUserRefreshTokens:
		userID, err := command.ExtractDeleteUserRefreshTokens(c.DeleteUserRefreshTokens)
		if err != nil {
			return err
		}
		return f.store.DeleteUserRefreshTokens(ctx, userID)

	default:
		return fmt.Errorf("unexpected refresh token command: %T", c)
	}
}

// cascadeDeleteRotationPolicy clears rotation policy references from tiers.
func (f *FSM) cascadeDeleteRotationPolicy(ctx context.Context, policyID glid.GLID) error {
	tiers, err := f.store.ListTiers(ctx)
	if err != nil {
		return fmt.Errorf("list tiers for cascade: %w", err)
	}
	for _, t := range tiers {
		if t.RotationPolicyID != nil && *t.RotationPolicyID == policyID {
			t.RotationPolicyID = nil
			if err := f.store.PutTier(ctx, t); err != nil {
				return fmt.Errorf("cascade update tier %s: %w", t.ID, err)
			}
		}
	}
	return nil
}

// cascadeDeleteRetentionPolicy removes retention rules referencing the policy from tiers.
func (f *FSM) cascadeDeleteRetentionPolicy(ctx context.Context, policyID glid.GLID) error {
	tiers, err := f.store.ListTiers(ctx)
	if err != nil {
		return fmt.Errorf("list tiers for cascade: %w", err)
	}
	for _, t := range tiers {
		modified := false
		filtered := t.RetentionRules[:0]
		for _, rule := range t.RetentionRules {
			if rule.RetentionPolicyID == policyID {
				modified = true
				continue
			}
			filtered = append(filtered, rule)
		}
		if modified {
			t.RetentionRules = filtered
			if err := f.store.PutTier(ctx, t); err != nil {
				return fmt.Errorf("cascade update tier %s: %w", t.ID, err)
			}
		}
	}
	return nil
}

// Snapshot captures the current config state for Raft log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	ctx := context.Background()

	sys, err := f.store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load system for snapshot: %w", err)
	}
	if sys == nil {
		sys = &system.System{}
	}

	users, err := f.store.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users for snapshot: %w", err)
	}

	tokens, err := f.store.ListRefreshTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("list refresh tokens for snapshot: %w", err)
	}

	snap := command.BuildSnapshot(sys, users, tokens)

	data, err := command.MarshalSnapshot(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM's state with a snapshot.
// Raft guarantees this is never called concurrently with Apply or Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error { //nolint:gocognit,gocyclo // snapshot restore is inherently complex
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	snap, err := command.UnmarshalSnapshot(data)
	if err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	sys, users, tokens, err := command.RestoreSnapshot(snap)
	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	newStore := memory.NewStore()
	ctx := context.Background()
	cfg := &sys.Config
	rt := &sys.Runtime

	// Config entities.
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
	for _, route := range cfg.Routes {
		if err := newStore.PutRoute(ctx, route); err != nil {
			return fmt.Errorf("restore route %s: %w", route.ID, err)
		}
	}
	if settings := snap.GetSettings(); len(settings) > 0 {
		if _, ok := settings["server"]; ok {
			if err := newStore.SaveServerSettings(ctx, system.ServerSettings{
				Auth:      cfg.Auth,
				Query:     cfg.Query,
				Scheduler: cfg.Scheduler,
				TLS:       cfg.TLS,
				Lookup:    cfg.Lookup,
				MaxMind:   cfg.MaxMind,
				Cluster:   cfg.Cluster,
			}); err != nil {
				return fmt.Errorf("restore server settings: %w", err)
			}
		}
	}
	for _, mf := range cfg.ManagedFiles {
		if err := newStore.PutManagedFile(ctx, mf); err != nil {
			return fmt.Errorf("restore managed file %s: %w", mf.ID, err)
		}
	}
	for _, cert := range cfg.Certs {
		if err := newStore.PutCertificate(ctx, cert); err != nil {
			return fmt.Errorf("restore certificate %s: %w", cert.ID, err)
		}
	}
	for _, cs := range cfg.CloudServices {
		if err := newStore.PutCloudService(ctx, cs); err != nil {
			return fmt.Errorf("restore cloud service %s: %w", cs.ID, err)
		}
	}
	for _, tier := range cfg.Tiers {
		if err := newStore.PutTier(ctx, tier); err != nil {
			return fmt.Errorf("restore tier %s: %w", tier.ID, err)
		}
	}

	// Users and tokens.
	for _, u := range users {
		if err := newStore.CreateUser(ctx, u); err != nil {
			return fmt.Errorf("restore user %s: %w", u.ID, err)
		}
	}
	for _, t := range tokens {
		if err := newStore.CreateRefreshToken(ctx, t); err != nil {
			return fmt.Errorf("restore refresh token %s: %w", t.ID, err)
		}
	}

	// Runtime: nodes, storage, TLS.
	for _, n := range rt.Nodes {
		if err := newStore.PutNode(ctx, n); err != nil {
			return fmt.Errorf("restore node %s: %w", n.ID, err)
		}
	}
	for _, nsc := range rt.NodeStorageConfigs {
		if err := newStore.SetNodeStorageConfig(ctx, nsc); err != nil {
			return fmt.Errorf("restore node storage config %s: %w", nsc.NodeID, err)
		}
	}
	if rt.ClusterTLS != nil {
		if err := newStore.PutClusterTLS(ctx, *rt.ClusterTLS); err != nil {
			return fmt.Errorf("restore cluster TLS: %w", err)
		}
	}
	for tierID, placements := range rt.TierPlacements {
		if err := newStore.SetTierPlacements(ctx, tierID, placements); err != nil {
			return fmt.Errorf("restore tier placements %s: %w", tierID, err)
		}
	}
	for ingesterID, nodes := range rt.IngesterAlive {
		for nodeID, alive := range nodes {
			if err := newStore.SetIngesterAlive(ctx, ingesterID, nodeID, alive); err != nil {
				return fmt.Errorf("restore ingester alive %s: %w", ingesterID, err)
			}
		}
	}
	if err := newStore.SetSetupWizardDismissed(ctx, rt.SetupWizardDismissed); err != nil {
		return fmt.Errorf("restore setup wizard dismissed: %w", err)
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
