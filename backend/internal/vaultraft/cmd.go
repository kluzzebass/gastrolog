package vaultraft

import "gastrolog/internal/glid"

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
