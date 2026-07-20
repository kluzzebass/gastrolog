package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
)

// TestChunkStateToProto pins the wire lifecycle for every internal state.
// The Unknown row matters most (gastrolog-5wh571): a local meta with no
// FSM-overlaid state comes from a two-state manager (memory mode, fresh
// head), so the producer resolves it to Active/Sealed here — consumers
// render UNSPECIFIED as "unknown" and never guess, so letting Unknown
// leak onto the wire would badge every memory-mode active chunk unknown.
func TestChunkStateToProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		state  chunk.ChunkState
		sealed bool
		expect apiv1.ChunkState
	}{
		{"active", chunk.ChunkStateActive, false, apiv1.ChunkState_CHUNK_STATE_ACTIVE},
		{"sealing", chunk.ChunkStateSealing, false, apiv1.ChunkState_CHUNK_STATE_SEALING},
		{"sealed", chunk.ChunkStateSealed, true, apiv1.ChunkState_CHUNK_STATE_SEALED},
		{"unknown unsealed resolves to active (two-state manager)", chunk.ChunkStateUnknown, false, apiv1.ChunkState_CHUNK_STATE_ACTIVE},
		{"unknown sealed resolves to sealed (legacy bool contract)", chunk.ChunkStateUnknown, true, apiv1.ChunkState_CHUNK_STATE_SEALED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := chunkStateToProto(tt.state, tt.sealed); got != tt.expect {
				t.Errorf("chunkStateToProto(%v, %v) = %v, want %v", tt.state, tt.sealed, got, tt.expect)
			}
		})
	}
}

// TestAdmissionCauseToProto pins the mapping the VaultInfo.AdmissionRefused
// field relies on: every orchestrator.VaultAdmissionCause the admission-
// causes collector can emit must map to its intended wire enum value, and an
// unrecognized value (defense in depth — the collector emits no such value
// today) maps to UNSPECIFIED rather than a wrong cause.
func TestAdmissionCauseToProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cause  orchestrator.VaultAdmissionCause
		expect apiv1.VaultAdmissionCause
	}{
		{"vault disk protect", orchestrator.VaultAdmissionCauseVaultDiskProtect, apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_VAULT_DISK_PROTECT},
		{"max-size bound", orchestrator.VaultAdmissionCauseMaxSizeBound, apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_MAX_SIZE_BOUND},
		{"backlog budget", orchestrator.VaultAdmissionCauseBacklogBudget, apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_BACKLOG_BUDGET},
		{"unrecognized value resolves to unspecified", orchestrator.VaultAdmissionCause(99), apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := admissionCauseToProto(tt.cause); got != tt.expect {
				t.Errorf("admissionCauseToProto(%v) = %v, want %v", tt.cause, got, tt.expect)
			}
		})
	}
}
