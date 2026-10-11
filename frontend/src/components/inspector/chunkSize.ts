// eslint-disable-next-line no-restricted-imports -- no Chunk model yet
import type { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";

// chunkDiskClaimBytes is this chunk's LOCAL on-disk claim — what deleting
// it would actually free on the responding node. Mirrors the backend's
// chunk.DiskClaim formula (backend/internal/chunk/claim.go): a cloud-backed
// chunk with no local cache (diskBytes == 0) claims nothing — the object
// still lives in the cloud store, at cloudBytes, a currency this never
// substitutes in. Otherwise diskBytes wins when recorded, falling back to
// logical bytes only for chunks with no disk-bytes tracking at all
// (pipeline GLCB chunks).
export function chunkDiskClaimBytes(chunk: ChunkMeta): number {
  const diskBytes = Number(chunk.diskBytes);
  if (chunk.cloudBacked && diskBytes === 0) return 0;
  if (diskBytes > 0) return diskBytes;
  return Number(chunk.bytes);
}

/** Which quantity the chunk list's Size column shows for one row. */
export type ChunkSizeKind = "disk" | "cloud";

// chunkSizeCell shows the chunk's disk claim — the figure the header sums and
// retention's size bound counts — except for a cloud-backed chunk with no
// local copy, whose claim is zero; that row shows the cloud object's size and
// is labeled cloud so the two currencies never share one unmarked number.
export function chunkSizeCell(chunk: ChunkMeta): { bytes: number; kind: ChunkSizeKind } {
  if (chunk.cloudBacked && Number(chunk.diskBytes) === 0) {
    return { bytes: Number(chunk.cloudBytes), kind: "cloud" };
  }
  return { bytes: chunkDiskClaimBytes(chunk), kind: "disk" };
}
