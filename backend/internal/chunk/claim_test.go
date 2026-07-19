package chunk

import "testing"

func TestDiskClaim(t *testing.T) {
	id := NewChunkID()

	tests := []struct {
		name       string
		meta       ChunkMeta
		indexSizes IndexSizeLookup
		want       int64
	}{
		{
			name: "DiskBytes wins over logical Bytes (compressed chunk)",
			meta: ChunkMeta{ID: id, Bytes: 4000, DiskBytes: 1000},
			want: 1000,
		},
		{
			name: "cloud-backed with no local copy claims nothing",
			meta: ChunkMeta{ID: id, Bytes: 999999, CloudBacked: true, DiskBytes: 0},
			want: 0,
		},
		{
			name: "cloud-backed cached counts its cache bytes",
			meta: ChunkMeta{ID: id, Bytes: 999999, CloudBacked: true, DiskBytes: 500},
			want: 500,
		},
		{
			name:       "fallback: no DiskBytes, no lookup — logical Bytes only",
			meta:       ChunkMeta{ID: id, Bytes: 300, DiskBytes: 0},
			indexSizes: nil,
			want:       300,
		},
		{
			name: "fallback: no DiskBytes, indexes add on top of Bytes",
			meta: ChunkMeta{ID: id, Bytes: 300, DiskBytes: 0},
			indexSizes: func(ChunkID) (map[string]int64, error) {
				return map[string]int64{"token": 50, "attr": 25}, nil
			},
			want: 375,
		},
		{
			name: "fallback: index lookup error contributes nothing extra",
			meta: ChunkMeta{ID: id, Bytes: 300, DiskBytes: 0},
			indexSizes: func(ChunkID) (map[string]int64, error) {
				return nil, ErrChunkNotFound
			},
			want: 300,
		},
		{
			name: "zero everything is zero",
			meta: ChunkMeta{ID: id},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DiskClaim(tt.meta, tt.indexSizes)
			if got != tt.want {
				t.Errorf("DiskClaim() = %d, want %d", got, tt.want)
			}
		})
	}
}
