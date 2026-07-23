package glcb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// Object-metadata keys.
//
// When a sealed GLCB is uploaded to a blob store, a small map of user
// metadata is attached to the object so a later List scan can reconstruct a
// chunk.ChunkMeta (record count, byte size, time bounds) WITHOUT downloading
// and parsing the blob footer. These keys are that map's schema.
//
// The encoder (EncodeObjectMetadata) and decoder (DecodeObjectMetadata) MUST
// agree on the exact spellings, so they are declared once here rather than
// as independent string literals in each function. Previously each side spelled
// all ten keys by hand and any drift silently produced wrong metadata
// (gastrolog-5opw43).
const (
	metaKeyChunkID     = "chunk_id"
	metaKeyVaultID     = "vault_id"
	metaKeyRecordCount = "record_count"
	metaKeyRawBytes    = "raw_bytes"
	metaKeyWriteStart  = "write_start"
	metaKeyWriteEnd    = "write_end"
	metaKeyIngestStart = "ingest_start"
	metaKeyIngestEnd   = "ingest_end"
	metaKeySourceStart = "source_start"
	metaKeySourceEnd   = "source_end"
)

// EncodeObjectMetadata builds the blob object-metadata map from a BlobMeta for
// upload. This is a CACHE of the authoritative state that also lives in the
// sealed GLCB footer; DecodeObjectMetadata is its inverse.
//
// Zero time bounds are omitted (their key is absent) rather than encoded as an
// epoch string, so a chunk with, say, no source timestamps carries no
// source_start / source_end keys. DecodeObjectMetadata treats an absent time
// key as a zero time, keeping the round-trip consistent.
func EncodeObjectMetadata(bm BlobMeta) map[string]string {
	md := map[string]string{
		metaKeyChunkID:     bm.ChunkID.String(),
		metaKeyVaultID:     bm.VaultID.String(),
		metaKeyRecordCount: strconv.FormatUint(uint64(bm.RecordCount), 10),
		metaKeyRawBytes:    strconv.FormatInt(bm.RawBytes, 10),
	}
	if !bm.WriteStart.IsZero() {
		md[metaKeyWriteStart] = bm.WriteStart.Format(time.RFC3339Nano)
	}
	if !bm.WriteEnd.IsZero() {
		md[metaKeyWriteEnd] = bm.WriteEnd.Format(time.RFC3339Nano)
	}
	if !bm.IngestStart.IsZero() {
		md[metaKeyIngestStart] = bm.IngestStart.Format(time.RFC3339Nano)
	}
	if !bm.IngestEnd.IsZero() {
		md[metaKeyIngestEnd] = bm.IngestEnd.Format(time.RFC3339Nano)
	}
	if !bm.SourceStart.IsZero() {
		md[metaKeySourceStart] = bm.SourceStart.Format(time.RFC3339Nano)
	}
	if !bm.SourceEnd.IsZero() {
		md[metaKeySourceEnd] = bm.SourceEnd.Format(time.RFC3339Nano)
	}
	return md
}

// DecodeObjectMetadata reconstructs a ChunkMeta from a blob's object metadata
// (the inverse of EncodeObjectMetadata). It is used by the file vault's cloud
// index scan to learn a cloud-backed chunk's record count and bounds without
// downloading the blob.
//
// Contract: any malformed field (an unparseable integer or timestamp, or a
// missing record_count — which the encoder always emits) is a REAL error and
// is returned as such. It is NEVER swallowed into a zero RecordCount / zero
// time bounds. Because this metadata feeds retention sweeps and query pruning,
// a fabricated zero presented as authoritative could drop or hide a chunk; the
// object metadata is only a cache, so on error the caller must fall back to the
// authoritative blob footer (see BlobMetaToChunkMeta) rather than trust zeros.
//
// Keys are matched case-insensitively: S3 (and some other providers) lowercase
// user-metadata keys on write, so a blob round-tripped through such a provider
// comes back with lowercased keys. The canonical constants are already
// lowercase, but matching case-insensitively guards against providers that
// upper- or title-case keys, and keeps the whole cloud vault from failing to
// index (and thus needlessly re-downloading every blob) on a key-casing quirk.
func DecodeObjectMetadata(id chunk.ChunkID, info blobstore.BlobInfo) (chunk.ChunkMeta, error) {
	md := newMetaLookup(info.Metadata)

	// info.Size is the cloud object's own size — CloudBytes, not DiskBytes.
	// This ChunkMeta is built straight from the cloud store's blob listing;
	// the caller (loadCloudBackedChunksFromStore) fills in the LOCAL
	// DiskBytes separately, from whatever this node actually has cached on
	// disk (0 if nothing). See gastrolog-33ul6h.
	meta := chunk.ChunkMeta{
		ID:         id,
		Sealed:     true,
		CloudBytes: info.Size,
		Bytes:      info.Size, // overwritten below when raw_bytes is known
	}

	if v, ok := md.get(metaKeyRawBytes); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return chunk.ChunkMeta{}, fmt.Errorf("decode object metadata for %s: %s=%q: %w", id, metaKeyRawBytes, v, err)
		}
		if n > 0 {
			meta.Bytes = n
		}
	}

	// record_count is always emitted by the encoder; its absence signals
	// truncated or mangled metadata, not a legitimately empty chunk.
	v, ok := md.get(metaKeyRecordCount)
	if !ok {
		return chunk.ChunkMeta{}, fmt.Errorf("decode object metadata for %s: missing required key %q", id, metaKeyRecordCount)
	}
	rc, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("decode object metadata for %s: %s=%q: %w", id, metaKeyRecordCount, v, err)
	}
	meta.RecordCount = rc

	for _, f := range []struct {
		key  string
		dest *time.Time
	}{
		{metaKeyWriteStart, &meta.WriteStart},
		{metaKeyWriteEnd, &meta.WriteEnd},
		{metaKeyIngestStart, &meta.IngestStart},
		{metaKeyIngestEnd, &meta.IngestEnd},
		{metaKeySourceStart, &meta.SourceStart},
		{metaKeySourceEnd, &meta.SourceEnd},
	} {
		v, ok := md.get(f.key)
		if !ok {
			// Absent time key = zero bound (the encoder omits zero times).
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return chunk.ChunkMeta{}, fmt.Errorf("decode object metadata for %s: %s=%q: %w", id, f.key, v, err)
		}
		*f.dest = t
	}

	return meta, nil
}

// BlobMetaToChunkMeta builds a ChunkMeta from the authoritative BlobMeta
// decoded from a sealed GLCB's own layout/footer — the source of truth for
// record count and bounds. Callers use this as the fallback when a blob's
// object metadata is unreadable (DecodeObjectMetadata returned an error): the
// footer is trusted, the object-metadata cache is not.
//
// cloudBytes is the on-wire size of the cloud object (BlobMeta does not
// carry it — the only caller is the cloud-blob-footer fallback, so this is
// always the compressed blob's size, never a local on-disk fact). Bytes
// falls back to it only when the footer's RawBytes is unknown. The caller
// fills in the LOCAL DiskBytes separately. See gastrolog-33ul6h.
func BlobMetaToChunkMeta(bm BlobMeta, cloudBytes int64) chunk.ChunkMeta {
	bytes := bm.RawBytes
	if bytes <= 0 {
		bytes = cloudBytes
	}
	return chunk.ChunkMeta{
		ID:                bm.ChunkID,
		Sealed:            true,
		CloudBytes:        cloudBytes,
		Bytes:             bytes,
		RecordCount:       int64(bm.RecordCount),
		WriteStart:        bm.WriteStart,
		WriteEnd:          bm.WriteEnd,
		IngestStart:       bm.IngestStart,
		IngestEnd:         bm.IngestEnd,
		SourceStart:       bm.SourceStart,
		SourceEnd:         bm.SourceEnd,
		IngestTSMonotonic: bm.IngestTSMonotonic,
	}
}

// metaLookup wraps an object-metadata map for case-insensitive key access.
// Providers normalize user-metadata key casing inconsistently (S3 lowercases),
// so lookups fold keys to lower case; the canonical constants are lowercase.
type metaLookup struct {
	folded map[string]string
}

func newMetaLookup(md map[string]string) metaLookup {
	folded := make(map[string]string, len(md))
	for k, v := range md {
		folded[strings.ToLower(k)] = v
	}
	return metaLookup{folded: folded}
}

func (l metaLookup) get(key string) (string, bool) {
	v, ok := l.folded[strings.ToLower(key)]
	return v, ok
}
