package glcb_test

import (
	"strings"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

// blobInfoFrom wraps an encoded object-metadata map as a BlobInfo the decoder
// consumes, mirroring what a blob store's List returns.
func blobInfoFrom(md map[string]string, size int64) blobstore.BlobInfo {
	return blobstore.BlobInfo{Key: "vault/chunk.glcb.zst", Size: size, Metadata: md}
}

// assertMetaEqual compares the decode-relevant fields of two ChunkMetas,
// using time.Time.Equal for the bounds so wall-clock vs monotonic and
// location pointers don't cause spurious mismatches.
func assertMetaEqual(t *testing.T, got, want chunk.ChunkMeta) {
	t.Helper()
	if got.RecordCount != want.RecordCount {
		t.Errorf("RecordCount = %d, want %d", got.RecordCount, want.RecordCount)
	}
	if got.Bytes != want.Bytes {
		t.Errorf("Bytes = %d, want %d", got.Bytes, want.Bytes)
	}
	for _, f := range []struct {
		name      string
		got, want time.Time
	}{
		{"WriteStart", got.WriteStart, want.WriteStart},
		{"WriteEnd", got.WriteEnd, want.WriteEnd},
		{"IngestStart", got.IngestStart, want.IngestStart},
		{"IngestEnd", got.IngestEnd, want.IngestEnd},
		{"SourceStart", got.SourceStart, want.SourceStart},
		{"SourceEnd", got.SourceEnd, want.SourceEnd},
	} {
		if !f.got.Equal(f.want) {
			t.Errorf("%s = %v, want %v", f.name, f.got, f.want)
		}
	}
}

// TestObjectMetadataRoundTrip is the codec property test: for representative
// BlobMeta values, DecodeObjectMetadata(EncodeObjectMetadata(x)) reproduces
// x's record count, byte size, and time bounds. Zero time bounds are legit-
// imately omitted by the encoder and decode to zero — asserted explicitly.
func TestObjectMetadataRoundTrip(t *testing.T) {
	base := time.Date(2026, 7, 14, 10, 30, 0, 123456789, time.UTC)

	cases := []struct {
		name string
		bm   glcb.BlobMeta
	}{
		{
			name: "all fields populated",
			bm: glcb.BlobMeta{
				ChunkID:     chunk.NewChunkID(),
				VaultID:     glid.New(),
				RecordCount: 42_000,
				RawBytes:    987_654,
				WriteStart:  base,
				WriteEnd:    base.Add(5 * time.Minute),
				IngestStart: base.Add(time.Second),
				IngestEnd:   base.Add(6 * time.Minute),
				SourceStart: base.Add(-time.Hour),
				SourceEnd:   base.Add(-time.Minute),
			},
		},
		{
			name: "no source timestamps (zero omitted)",
			bm: glcb.BlobMeta{
				ChunkID:     chunk.NewChunkID(),
				VaultID:     glid.New(),
				RecordCount: 1,
				RawBytes:    64,
				WriteStart:  base,
				WriteEnd:    base,
				IngestStart: base,
				IngestEnd:   base,
				// SourceStart / SourceEnd zero => omitted by encoder.
			},
		},
		{
			name: "empty chunk, all bounds zero",
			bm: glcb.BlobMeta{
				ChunkID:     chunk.NewChunkID(),
				VaultID:     glid.New(),
				RecordCount: 0,
				RawBytes:    128,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := glcb.EncodeObjectMetadata(tc.bm)
			// cloudBytes (the blob's object size) differs from RawBytes so
			// we can prove Bytes tracks raw_bytes, not the wire size, when
			// raw_bytes > 0.
			const cloudBytes = 5_000_000
			got, err := glcb.DecodeObjectMetadata(tc.bm.ChunkID, blobInfoFrom(md, cloudBytes))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			wantBytes := tc.bm.RawBytes
			if wantBytes <= 0 {
				wantBytes = cloudBytes // encoder writes raw_bytes=0 => Bytes falls back to CloudBytes
			}
			assertMetaEqual(t, got, chunk.ChunkMeta{
				RecordCount: int64(tc.bm.RecordCount),
				Bytes:       wantBytes,
				WriteStart:  tc.bm.WriteStart,
				WriteEnd:    tc.bm.WriteEnd,
				IngestStart: tc.bm.IngestStart,
				IngestEnd:   tc.bm.IngestEnd,
				SourceStart: tc.bm.SourceStart,
				SourceEnd:   tc.bm.SourceEnd,
			})

			if got.ID != tc.bm.ChunkID {
				t.Errorf("ID = %v, want %v", got.ID, tc.bm.ChunkID)
			}
			if !got.Sealed {
				t.Error("Sealed = false, want true")
			}
			if got.CloudBytes != cloudBytes {
				t.Errorf("CloudBytes = %d, want %d", got.CloudBytes, cloudBytes)
			}
			if got.DiskBytes != 0 {
				t.Errorf("DiskBytes = %d, want 0 (object-metadata decode carries no local disk fact)", got.DiskBytes)
			}
		})
	}
}

// TestDecodeObjectMetadata_ZeroBoundsAbsent proves the specific round-trip
// invariant called out in the task: a zero WriteStart encodes to an ABSENT
// key and decodes back to a zero time — never fabricated.
func TestDecodeObjectMetadata_ZeroBoundsAbsent(t *testing.T) {
	bm := glcb.BlobMeta{
		ChunkID:     chunk.NewChunkID(),
		VaultID:     glid.New(),
		RecordCount: 3,
		RawBytes:    10,
		// WriteStart intentionally zero.
	}
	md := glcb.EncodeObjectMetadata(bm)
	if _, present := md["write_start"]; present {
		t.Fatal("encoder emitted write_start for a zero time; expected omission")
	}
	got, err := glcb.DecodeObjectMetadata(bm.ChunkID, blobInfoFrom(md, 100))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.WriteStart.IsZero() {
		t.Errorf("WriteStart = %v, want zero", got.WriteStart)
	}
}

// TestDecodeObjectMetadata_Malformed asserts every malformed / mangled input
// surfaces as an error instead of silently producing a zero-record,
// zero-time ChunkMeta presented as authoritative (the gastrolog-5opw43 bug).
func TestDecodeObjectMetadata_Malformed(t *testing.T) {
	id := chunk.NewChunkID()

	// A valid baseline map we mutate per case.
	valid := func() map[string]string {
		return glcb.EncodeObjectMetadata(glcb.BlobMeta{
			ChunkID:     id,
			VaultID:     glid.New(),
			RecordCount: 5,
			RawBytes:    100,
			WriteStart:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			WriteEnd:    time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		})
	}

	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{
			name:   "bad record_count",
			mutate: func(m map[string]string) { m["record_count"] = "abc" },
		},
		{
			name:   "bad raw_bytes",
			mutate: func(m map[string]string) { m["raw_bytes"] = "not-a-number" },
		},
		{
			name:   "truncated timestamp",
			mutate: func(m map[string]string) { m["write_start"] = "2026-01-01T00:00" },
		},
		{
			name:   "garbage timestamp",
			mutate: func(m map[string]string) { m["write_end"] = "yesterday" },
		},
		{
			name:   "missing required record_count",
			mutate: func(m map[string]string) { delete(m, "record_count") },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := valid()
			tc.mutate(md)
			got, err := glcb.DecodeObjectMetadata(id, blobInfoFrom(md, 100))
			if err == nil {
				t.Fatalf("expected error for %s, got ChunkMeta %+v", tc.name, got)
			}
			// The failed decode must not leak a partially-populated meta.
			if got.RecordCount != 0 || !got.WriteStart.IsZero() {
				t.Errorf("error path returned non-zero ChunkMeta: %+v", got)
			}
		})
	}
}

// TestDecodeObjectMetadata_S3LowercasedKeys proves the decoder survives a
// provider (S3) that lowercases user-metadata keys on write. Since the
// canonical keys are already lowercase this is trivially satisfied for them;
// the case-insensitive lookup additionally survives upper/title-casing.
func TestDecodeObjectMetadata_CaseInsensitiveKeys(t *testing.T) {
	id := chunk.NewChunkID()
	ws := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	// Simulate a provider that TitleCased the keys (worse than S3's
	// lowercase, which the constants already match).
	md := map[string]string{
		"Chunk_Id":     id.String(),
		"Vault_Id":     glid.New().String(),
		"Record_Count": "17",
		"Raw_Bytes":    "256",
		"Write_Start":  ws.Format(time.RFC3339Nano),
	}
	got, err := glcb.DecodeObjectMetadata(id, blobInfoFrom(md, 999))
	if err != nil {
		t.Fatalf("decode with title-cased keys: %v", err)
	}
	if got.RecordCount != 17 {
		t.Errorf("RecordCount = %d, want 17", got.RecordCount)
	}
	if got.Bytes != 256 {
		t.Errorf("Bytes = %d, want 256", got.Bytes)
	}
	if !got.WriteStart.Equal(ws) {
		t.Errorf("WriteStart = %v, want %v", got.WriteStart, ws)
	}

	// Also confirm the plain S3 lowercase form (identical to canonical).
	lower := make(map[string]string, len(md))
	for k, v := range md {
		lower[strings.ToLower(k)] = v
	}
	got2, err := glcb.DecodeObjectMetadata(id, blobInfoFrom(lower, 999))
	if err != nil {
		t.Fatalf("decode with lowercased keys: %v", err)
	}
	if got2.RecordCount != 17 {
		t.Errorf("lowercase RecordCount = %d, want 17", got2.RecordCount)
	}
}

// TestBlobMetaToChunkMeta covers the authoritative-footer fallback converter:
// it maps a GLCB's own layout meta to ChunkMeta, and falls back to diskBytes
// for Bytes only when RawBytes is unknown.
func TestBlobMetaToChunkMeta(t *testing.T) {
	base := time.Date(2026, 5, 5, 5, 5, 5, 0, time.UTC)
	bm := glcb.BlobMeta{
		ChunkID:           chunk.NewChunkID(),
		VaultID:           glid.New(),
		RecordCount:       9,
		RawBytes:          321,
		WriteStart:        base,
		WriteEnd:          base.Add(time.Minute),
		IngestStart:       base,
		IngestEnd:         base.Add(time.Minute),
		IngestTSMonotonic: true,
	}
	got := glcb.BlobMetaToChunkMeta(bm, 111)
	if got.RecordCount != 9 || got.Bytes != 321 || got.CloudBytes != 111 {
		t.Errorf("unexpected meta: %+v", got)
	}
	if got.DiskBytes != 0 {
		t.Errorf("DiskBytes = %d, want 0 (footer read carries no local disk fact)", got.DiskBytes)
	}
	if !got.Sealed || !got.IngestTSMonotonic {
		t.Errorf("expected Sealed and IngestTSMonotonic true: %+v", got)
	}
	if !got.WriteStart.Equal(base) {
		t.Errorf("WriteStart = %v, want %v", got.WriteStart, base)
	}

	// RawBytes unknown => Bytes falls back to cloudBytes.
	bm.RawBytes = 0
	got = glcb.BlobMetaToChunkMeta(bm, 111)
	if got.Bytes != 111 {
		t.Errorf("Bytes fallback = %d, want 111", got.Bytes)
	}
}
