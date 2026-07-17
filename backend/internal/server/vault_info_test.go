package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
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
