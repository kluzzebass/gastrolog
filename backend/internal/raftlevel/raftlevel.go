// Package raftlevel keeps a Raft log entry from reaching a node that cannot
// apply it.
//
// Every binary has a protocol level, Binary: the highest command level it can
// apply. Every command type in the cluster-ctl log (SystemCommand) and in the
// vault-ctl logs (VaultRaftCommand and the vault chunk FSM's VaultCtlCommand)
// has a minimum level in the level table. The cluster commits an active level
// in cluster-ctl, raised only once every member of the cluster-ctl Raft
// configuration reports a binary at that level or above, and never lowered.
// A proposer refuses any command whose level is above the active level, and
// the node that hands an entry to raft.Apply refuses any entry it cannot
// decode itself.
//
// The rule that keeps the table complete: new behaviour is a new command
// type. An existing command's apply semantics never change, so a command type
// means the same thing to every binary that knows it, and a binary that does
// not know it never sees it while the gate holds.
//
// A release that adds a command type raises Binary and enters the new type in
// the table at that level.
package raftlevel

import (
	"errors"
	"fmt"
	"maps"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Binary is this binary's protocol level.
const Binary uint32 = 1

// Base is the level every binary that carries the gate supports. A cluster
// that has not committed an active level runs at Base.
const Base uint32 = 1

var (
	// ErrCommandAboveClusterLevel refuses a proposal whose command type needs
	// a higher protocol level than the cluster's active level.
	ErrCommandAboveClusterLevel = errors.New("command needs a higher protocol level than the cluster's active level")
	// ErrUnknownCommand marks an entry whose command type this binary does
	// not know.
	ErrUnknownCommand = errors.New("command type unknown to this binary")
	// ErrUndecodableCommand marks an entry this binary cannot decode.
	ErrUndecodableCommand = errors.New("command cannot be decoded by this binary")
	// ErrBinaryBelowActiveLevel marks a binary whose protocol level is below
	// the cluster's committed active level.
	ErrBinaryBelowActiveLevel = errors.New("binary protocol level is below the cluster's active protocol level")
)

// Table maps each command oneof case, by its proto full name, to the
// protocol level the case needs.
type Table map[protoreflect.FullName]uint32

// With returns a copy of t with name at level.
func (t Table) With(name protoreflect.FullName, level uint32) Table {
	out := maps.Clone(t)
	out[name] = level
	return out
}

var (
	systemCommand    = (&gastrologv1.SystemCommand{}).ProtoReflect().Descriptor()
	vaultRaftCommand = (&gastrologv1.VaultRaftCommand{}).ProtoReflect().Descriptor()
	vaultCtlCommand  = (&gastrologv1.VaultCtlCommand{}).ProtoReflect().Descriptor()
)

// CommandMessages are the Raft log command messages the table covers. Each
// has a single oneof named "command".
var CommandMessages = []protoreflect.MessageDescriptor{systemCommand, vaultRaftCommand, vaultCtlCommand}

// Case returns the full name of a command oneof case.
func Case(msg protoreflect.MessageDescriptor, field protoreflect.Name) protoreflect.FullName {
	fd := msg.Fields().ByName(field)
	if fd == nil || fd.ContainingOneof() == nil {
		panic(fmt.Sprintf("raftlevel: %s has no oneof case %s", msg.FullName(), field))
	}
	return fd.FullName()
}

func at(level uint32, msg protoreflect.MessageDescriptor, fields ...protoreflect.Name) Table {
	t := make(Table, len(fields))
	for _, f := range fields {
		t[Case(msg, f)] = level
	}
	return t
}

func merge(tables ...Table) Table {
	out := Table{}
	for _, t := range tables {
		maps.Copy(out, t)
	}
	return out
}

var defaultTable = merge(
	at(1, systemCommand,
		"put_rotation_policy", "delete_rotation_policy",
		"put_retention_policy", "delete_retention_policy",
		"put_vault", "delete_vault",
		"put_ingester", "delete_ingester",
		"put_setting", "delete_setting",
		"put_certificate", "delete_certificate",
		"create_user", "update_password", "update_user_role", "update_username",
		"delete_user", "invalidate_tokens", "put_user_preferences",
		"create_refresh_token", "delete_refresh_token", "delete_user_refresh_tokens",
		"rotate_refresh_token",
		"put_node_config", "delete_node_config", "mark_node_removed", "set_node_state",
		"put_cluster_tls",
		"put_route", "delete_route",
		"put_managed_file", "delete_managed_file",
		"put_cloud_service", "delete_cloud_service",
		"set_node_storage_config", "set_vault_placements", "set_setup_wizard_dismissed",
		"set_ingester_alive", "set_ingester_assignment", "set_ingester_checkpoint",
		"put_log_levels",
		"catchup_barrier",
		"set_protocol_level",
	),
	at(1, vaultRaftCommand, "noop", "vault_scoped"),
	at(1, vaultCtlCommand,
		"create_chunk", "seal_chunk", "upload_chunk",
		"retention_pending", "request_delete", "ack_delete", "finalize_delete",
		"prune_node", "attach_offsets", "begin_seal", "repatriate_chunk",
		"publish_completed_segment", "publish_completed_segments",
		"open_chunk_manifest", "add_open_chunk_segment_ref", "add_open_chunk_segment_refs",
		"seal_open_chunk_manifest", "discard_open_chunk_manifest", "discard_unbuildable_manifests",
		"release_segments", "ack_segment_holder",
		"ack_chunk_holder", "revoke_chunk_holder",
		"clear_transfer_source", "archive_chunk",
		"confirm_segment_purge", "prune_released_segments",
		"confirm_chunk_purge", "prune_chunk_tombstones",
	),
)

// DefaultTable returns a copy of this binary's level table.
func DefaultTable() Table {
	return maps.Clone(defaultTable)
}

// commandLevel returns the level of the case set in m's command oneof.
func (t Table) commandLevel(m protoreflect.Message) (uint32, protoreflect.FullName, error) {
	fd := m.WhichOneof(m.Descriptor().Oneofs().ByName("command"))
	if fd == nil {
		if len(m.GetUnknown()) > 0 {
			return 0, "", fmt.Errorf("%w: %s carries a field this binary does not know", ErrUnknownCommand, m.Descriptor().Name())
		}
		return 0, "", fmt.Errorf("%w: %s carries no command", ErrUnknownCommand, m.Descriptor().Name())
	}
	level, ok := t[fd.FullName()]
	if !ok {
		return 0, "", fmt.Errorf("%w: %s has no protocol level", ErrUnknownCommand, fd.FullName())
	}
	return level, fd.FullName(), nil
}

// SystemCommandLevel returns the protocol level a cluster-ctl command needs.
func (t Table) SystemCommandLevel(cmd *gastrologv1.SystemCommand) (uint32, protoreflect.FullName, error) {
	return t.commandLevel(cmd.ProtoReflect())
}

// VaultCtlCommandLevel returns the protocol level a vault chunk FSM command
// needs.
func (t Table) VaultCtlCommandLevel(cmd *gastrologv1.VaultCtlCommand) (uint32, protoreflect.FullName, error) {
	return t.commandLevel(cmd.ProtoReflect())
}

// VaultRaftCommandLevel returns the protocol level a vault-ctl entry needs:
// the higher of its envelope's and, for a vault-scoped entry, its inner
// command's.
func (t Table) VaultRaftCommandLevel(cmd *gastrologv1.VaultRaftCommand) (uint32, protoreflect.FullName, error) {
	level, name, err := t.commandLevel(cmd.ProtoReflect())
	if err != nil {
		return 0, "", err
	}
	scoped := cmd.GetVaultScoped()
	if scoped == nil {
		return level, name, nil
	}
	inner, innerName, err := t.commandLevel(scoped.GetCommand().ProtoReflect())
	if err != nil {
		return 0, "", err
	}
	if inner > level {
		return inner, innerName, nil
	}
	return level, name, nil
}

// Gate refuses proposals the cluster cannot apply on every member. A nil
// *Gate gates against this binary's table at Base.
type Gate struct {
	active func() uint32
	table  Table
}

// NewGate returns a gate over this binary's table. active reads the
// cluster's committed active level.
func NewGate(active func() uint32) *Gate {
	return &Gate{active: active, table: defaultTable}
}

// WithTable returns a copy of g that gates against t.
func (g *Gate) WithTable(t Table) *Gate {
	out := &Gate{table: t}
	if g != nil {
		out.active = g.active
	}
	return out
}

// ActiveLevel returns the level the gate admits commands up to: the
// committed active level, and never less than Base.
func (g *Gate) ActiveLevel() uint32 {
	if g == nil || g.active == nil {
		return Base
	}
	return max(g.active(), Base)
}

func (g *Gate) levels() Table {
	if g == nil || g.table == nil {
		return defaultTable
	}
	return g.table
}

func (g *Gate) admit(level uint32, name protoreflect.FullName) error {
	if active := g.ActiveLevel(); level > active {
		return fmt.Errorf("%w: %s needs protocol level %d, the cluster's active level is %d",
			ErrCommandAboveClusterLevel, name, level, active)
	}
	return nil
}

// CheckSystemCommand admits a marshaled cluster-ctl command, or says why not.
func (g *Gate) CheckSystemCommand(data []byte) error {
	var cmd gastrologv1.SystemCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("%w: %w", ErrUndecodableCommand, err)
	}
	level, name, err := g.levels().SystemCommandLevel(&cmd)
	if err != nil {
		return err
	}
	return g.admit(level, name)
}

// CheckVaultRaftCommand admits a marshaled vault-ctl entry, or says why not.
func (g *Gate) CheckVaultRaftCommand(data []byte) error {
	var cmd gastrologv1.VaultRaftCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("%w: %w", ErrUndecodableCommand, err)
	}
	level, name, err := g.levels().VaultRaftCommandLevel(&cmd)
	if err != nil {
		return err
	}
	return g.admit(level, name)
}

// CheckVaultCtlCommand admits a marshaled vault chunk FSM command, or says
// why not.
func (g *Gate) CheckVaultCtlCommand(data []byte) error {
	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("%w: %w", ErrUndecodableCommand, err)
	}
	level, name, err := g.levels().VaultCtlCommandLevel(&cmd)
	if err != nil {
		return err
	}
	return g.admit(level, name)
}

// IsRefusal reports whether err is a gate refusal.
func IsRefusal(err error) bool {
	return errors.Is(err, ErrCommandAboveClusterLevel) ||
		errors.Is(err, ErrUnknownCommand) ||
		errors.Is(err, ErrUndecodableCommand)
}

// UnknownCommand describes a decoded command whose type an FSM does not
// handle.
func UnknownCommand(cmd proto.Message) error {
	m := cmd.ProtoReflect()
	if fd := m.WhichOneof(m.Descriptor().Oneofs().ByName("command")); fd != nil {
		return fmt.Errorf("%w: no apply for %s", ErrUnknownCommand, fd.FullName())
	}
	_, _, err := defaultTable.commandLevel(m)
	return err
}
