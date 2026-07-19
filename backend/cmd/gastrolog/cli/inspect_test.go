package cli

import (
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func TestChunkBadges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		chunk  *v1.ChunkMeta
		expect string
	}{
		{
			name:   "active chunk",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_ACTIVE},
			expect: "active",
		},
		{
			// gastrolog-5wh571: SEALING is not "active" — a wedged seal
			// and a healthy open chunk are opposite diagnoses.
			name:   "sealing chunk",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALING},
			expect: "sealing",
		},
		{
			// gastrolog-24m1t step 7f dropped the "compressed" badge —
			// sealed chunks are GLCB and GLCB is zstd-compressed by
			// construction, so the flag carried no information.
			name:   "sealed (GLCB is implicitly compressed)",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALED, Sealed: true},
			expect: "sealed",
		},
		{
			// Unspecified state renders as "unknown", never a guess
			// (gastrolog-5wh571). The zero-value else-branch used to
			// claim "active" here.
			name:   "unspecified state",
			chunk:  &v1.ChunkMeta{},
			expect: "unknown",
		},
		{
			// The legacy bool never overrides the enum: state is the
			// authority, and the server derives state from the bool for
			// pre-Phase-3 entries before it reaches the wire.
			name:   "unspecified state with legacy sealed bool",
			chunk:  &v1.ChunkMeta{Sealed: true},
			expect: "unknown",
		},
		{
			name:   "sealed cloud",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALED, Sealed: true, CloudBacked: true},
			expect: "sealed cloud",
		},
		{
			name:   "full cloud archived",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALED, Sealed: true, CloudBacked: true, Archived: true},
			expect: "sealed cloud archived",
		},
		{
			name:   "retention pending",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALED, Sealed: true, RetentionPending: true},
			expect: "sealed retention-pending",
		},
		{
			// Modifiers ride along on non-sealed lifecycles too.
			name:   "sealing cloud",
			chunk:  &v1.ChunkMeta{State: v1.ChunkState_CHUNK_STATE_SEALING, CloudBacked: true},
			expect: "sealing cloud",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkBadges(tt.chunk)
			if got != tt.expect {
				t.Errorf("chunkBadges() = %q, want %q", got, tt.expect)
			}
		})
	}
}

// TestChunkSizeBytes pins the gastrolog-33ul6h fix: an evicted cloud-backed
// chunk (CloudBacked, DiskBytes==0) must report 0, never fall back to
// logical Bytes — the object still exists in the cloud store, at
// CloudBytes, a currency this local-disk stat never touches. The existing
// pipeline-chunk fallback (non-cloud, DiskBytes unset -> logical Bytes,
// gastrolog-45ywhx) must still hold.
func TestChunkSizeBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		chunk  *v1.ChunkMeta
		expect int64
	}{
		{
			name:   "local sealed chunk reports DiskBytes",
			chunk:  &v1.ChunkMeta{Bytes: 4000, DiskBytes: 900},
			expect: 900,
		},
		{
			name:   "pipeline GLCB chunk falls back to logical Bytes (gastrolog-45ywhx)",
			chunk:  &v1.ChunkMeta{Bytes: 4000, DiskBytes: 0},
			expect: 4000,
		},
		{
			name:   "cached cloud-backed chunk reports its cache size",
			chunk:  &v1.ChunkMeta{Bytes: 999999, CloudBacked: true, DiskBytes: 1200, CloudBytes: 300},
			expect: 1200,
		},
		{
			name:   "evicted cloud-backed chunk reports 0, not logical Bytes or CloudBytes",
			chunk:  &v1.ChunkMeta{Bytes: 999999, CloudBacked: true, DiskBytes: 0, CloudBytes: 300},
			expect: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkSizeBytes(tt.chunk)
			if got != tt.expect {
				t.Errorf("chunkSizeBytes() = %d, want %d", got, tt.expect)
			}
		})
	}
}
