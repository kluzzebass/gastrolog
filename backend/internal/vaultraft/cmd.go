package vaultraft

// Vault control-plane FSM command opcodes (first byte of Apply payload).
const (
	// OpNoop is a no-op replicated command for tests and liveness checks.
	OpNoop byte = 1
)

// MarshalNoop returns a minimal replicated command that Apply accepts as a no-op.
func MarshalNoop() []byte { return []byte{OpNoop} }
