package vaultraft

import (
	"errors"
	"fmt"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	"google.golang.org/protobuf/proto"
)

// Vault control-plane FSM commands are encoded as gastrologv1.VaultRaftCommand
// (a oneof of NoopCommand / VaultScopedCommand). The legacy opcode bytes are
// retained as named constants for the WAL inspector tooling. gastrolog-5lrg7.
const (
	// OpNoop is a no-op replicated command for tests and liveness checks.
	OpNoop byte = 1
	// OpVaultChunkFSM wraps a chunk-metadata command scoped to a
	// vault-instance GLID. See MarshalVaultChunkCommand.
	OpVaultChunkFSM byte = 2
)

// mustMarshal marshals a VaultRaftCommand. proto.Marshal of these in-memory
// messages cannot fail; a non-nil error indicates a programmer error and
// panics rather than returning corrupt bytes.
func mustMarshal(cmd *gastrologv1.VaultRaftCommand) []byte {
	b, err := proto.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("vaultraft: marshal command: %v", err))
	}
	return b
}

// marshalVaultScoped wraps a typed inner command in a vault-scoped envelope,
// avoiding a re-marshal round-trip on the (hot) seq/fence paths.
func marshalVaultScoped(vaultID glid.GLID, inner *gastrologv1.VaultCtlCommand) []byte {
	return mustMarshal(&gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_VaultScoped{
		VaultScoped: &gastrologv1.VaultScopedCommand{VaultId: vaultID[:], Command: inner},
	}})
}

// MarshalNoop returns a replicated command that Apply accepts as a no-op.
func MarshalNoop() []byte {
	return mustMarshal(&gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_Noop{Noop: &gastrologv1.NoopCommand{}}})
}

// MarshalVaultChunkCommand builds a vault control-plane log entry that applies
// chunkWire to the vaultctlfsm sub-state for vaultID. chunkWire must be a
// marshaled vaultctlfsm.VaultCtlCommand (e.g. output of
// vaultctlfsm.MarshalCreateChunk).
func MarshalVaultChunkCommand(vaultID glid.GLID, chunkWire []byte) []byte {
	var inner gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(chunkWire, &inner); err != nil {
		panic(fmt.Sprintf("vaultraft: decode inner command: %v", err))
	}
	return marshalVaultScoped(vaultID, &inner)
}

func marshalVaultSubCommand(vaultID glid.GLID, inner *gastrologv1.VaultCtlCommand) ([]byte, error) {
	if inner == nil || inner.GetCommand() == nil {
		return nil, errors.New("vaultraft: empty vault sub-command")
	}
	return marshalVaultScoped(vaultID, inner), nil
}

// MarshalVaultReserveSeqRange replicates a destination-vault sequence lease
// reservation for vaultID. holderEpoch must match the current allocator epoch.
func MarshalVaultReserveSeqRange(vaultID glid.GLID, holderID string, holderEpoch, count uint64) ([]byte, error) {
	inner, err := vaultctlfsm.NewReserveSeqRange(holderID, holderEpoch, count)
	if err != nil {
		return nil, err
	}
	return marshalVaultSubCommand(vaultID, inner)
}

// MarshalVaultBurnSeqLeaseTail records the consumed prefix of the holder's
// active lease and burns any remaining tail as an unassigned gap.
func MarshalVaultBurnSeqLeaseTail(vaultID glid.GLID, holderID string, holderEpoch, consumedEnd uint64) ([]byte, error) {
	inner, err := vaultctlfsm.NewBurnSeqLeaseTail(holderID, holderEpoch, consumedEnd)
	if err != nil {
		return nil, err
	}
	return marshalVaultSubCommand(vaultID, inner)
}

// MarshalVaultBumpSeqAllocatorEpoch bumps allocator epoch and burns any
// outstanding active lease tail (failover safety).
func MarshalVaultBumpSeqAllocatorEpoch(vaultID glid.GLID) []byte {
	return marshalVaultScoped(vaultID, vaultctlfsm.NewBumpSeqAllocatorEpoch())
}

// MarshalVaultPublishFence durably records one fence cut (F_n) for vaultID.
func MarshalVaultPublishFence(vaultID glid.GLID, upperBoundSeq uint64, createdAt time.Time) []byte {
	return marshalVaultScoped(vaultID, vaultctlfsm.NewPublishFence(upperBoundSeq, createdAt))
}
