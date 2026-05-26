package vaultraft

import (
	"errors"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Vault control-plane FSM command opcodes (first byte of Apply payload).
const (
	// OpNoop is a no-op replicated command for tests and liveness checks.
	OpNoop byte = 1
	// OpVaultChunkFSM wraps a chunk-metadata command (vaultctlfsm wire format,
	// including its leading command byte) scoped to a vault-instance GLID.
	// See MarshalVaultChunkCommand.
	OpVaultChunkFSM byte = 2
)

// MarshalNoop returns a minimal replicated command that Apply accepts as a no-op.
func MarshalNoop() []byte { return []byte{OpNoop} }

// MarshalVaultChunkCommand builds a vault control-plane log entry that applies
// chunkWire to the vaultctlfsm sub-state for vaultID. chunkWire must be a full
// vaultctlfsm command (e.g. output of vaultctlfsm.MarshalCreateChunk).
func MarshalVaultChunkCommand(vaultID glid.GLID, chunkWire []byte) []byte {
	out := make([]byte, 0, 1+glid.Size+len(chunkWire))
	out = append(out, OpVaultChunkFSM)
	out = append(out, vaultID[:]...)
	out = append(out, chunkWire...)
	return out
}

func marshalVaultSubCommand(vaultID glid.GLID, wire []byte) ([]byte, error) {
	if len(wire) == 0 {
		return nil, errors.New("vaultraft: empty vault sub-command")
	}
	return MarshalVaultChunkCommand(vaultID, wire), nil
}

// MarshalVaultReserveSeqRange replicates a destination-vault sequence lease
// reservation for vaultID. holderEpoch must match the current allocator epoch.
func MarshalVaultReserveSeqRange(vaultID glid.GLID, holderID string, holderEpoch, count uint64) ([]byte, error) {
	wire, err := vaultctlfsm.MarshalReserveSeqRange(holderID, holderEpoch, count)
	if err != nil {
		return nil, err
	}
	return marshalVaultSubCommand(vaultID, wire)
}

// MarshalVaultBurnSeqLeaseTail records the consumed prefix of the holder's
// active lease and burns any remaining tail as an unassigned gap.
func MarshalVaultBurnSeqLeaseTail(vaultID glid.GLID, holderID string, holderEpoch, consumedEnd uint64) ([]byte, error) {
	wire, err := vaultctlfsm.MarshalBurnSeqLeaseTail(holderID, holderEpoch, consumedEnd)
	if err != nil {
		return nil, err
	}
	return marshalVaultSubCommand(vaultID, wire)
}

// MarshalVaultBumpSeqAllocatorEpoch bumps allocator epoch and burns any
// outstanding active lease tail (failover safety).
func MarshalVaultBumpSeqAllocatorEpoch(vaultID glid.GLID) []byte {
	return MarshalVaultChunkCommand(vaultID, vaultctlfsm.MarshalBumpSeqAllocatorEpoch())
}
