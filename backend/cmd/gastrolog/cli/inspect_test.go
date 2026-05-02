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
			chunk:  &v1.ChunkMeta{},
			expect: "active",
		},
		{
			name:   "sealed only",
			chunk:  &v1.ChunkMeta{Sealed: true},
			expect: "sealed",
		},
		{
			// gastrolog-24m1t step 7f dropped the "compressed" badge —
			// sealed chunks are GLCB and GLCB is zstd-compressed by
			// construction, so the flag carried no information.
			name:   "sealed (GLCB is implicitly compressed)",
			chunk:  &v1.ChunkMeta{Sealed: true, Compressed: true},
			expect: "sealed",
		},
		{
			name:   "sealed cloud",
			chunk:  &v1.ChunkMeta{Sealed: true, Compressed: true, CloudBacked: true},
			expect: "sealed cloud",
		},
		{
			name:   "full cloud archived",
			chunk:  &v1.ChunkMeta{Sealed: true, Compressed: true, CloudBacked: true, Archived: true},
			expect: "sealed cloud archived",
		},
		{
			name:   "retention pending",
			chunk:  &v1.ChunkMeta{Sealed: true, RetentionPending: true},
			expect: "sealed retention-pending",
		},
		{
			name: "transition streamed",
			chunk: &v1.ChunkMeta{
				Sealed: true, RetentionPending: true, TransitionStreamed: true,
			},
			expect: "sealed retention-pending streamed-await-delete",
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
