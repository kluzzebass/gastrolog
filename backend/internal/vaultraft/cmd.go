package vaultraft

import "gastrolog/internal/glid"

// Vault control-plane FSM command opcodes (first byte of Apply payload).
const (
	// OpNoop is a no-op replicated command for tests and liveness checks.
	OpNoop byte = 1
	// OpVaultChunkFSM wraps a tier chunk-metadata command (tierfsm wire format, including
	// its leading command byte) scoped to a tier GLID. See MarshalTierCommand.
	OpVaultChunkFSM byte = 2
)

// MarshalNoop returns a minimal replicated command that Apply accepts as a no-op.
func MarshalNoop() []byte { return []byte{OpNoop} }

// MarshalTierCommand builds a vault control-plane log entry that applies
// tierWire to the tierfsm sub-state for tierID. tierWire must be a full
// tierfsm command (e.g. output of tierfsm.MarshalCreateChunk).
func MarshalTierCommand(tierID glid.GLID, tierWire []byte) []byte {
	out := make([]byte, 0, 1+glid.Size+len(tierWire))
	out = append(out, OpVaultChunkFSM)
	out = append(out, tierID[:]...)
	out = append(out, tierWire...)
	return out
}
