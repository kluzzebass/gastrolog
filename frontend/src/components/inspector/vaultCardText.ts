// eslint-disable-next-line no-restricted-imports -- no Chunk model yet
import type { ContributionReport, GetIndexesResponse } from "../../api/gen/gastrolog/v1/vault_pb";
import { VaultAdmissionCause, type VaultAdmissionRefusal } from "../../api/model/vault";

// vaultAdmissionCauseLabels maps the backend's admission-refusal cause enum
// (VaultInfo.admissionRefused, populated from the responding node's own
// admission-causes collector — backend/internal/orchestrator/disk_guard.go's
// VaultAdmissionCauses) to a terse tooltip phrase. A first-class backend
// signal, not a UI-side derivation from alarm state. The disk-free
// thresholds belong to the storage a vault is placed on, not the vault,
// hence STORAGE_DISK_PROTECT.
const vaultAdmissionCauseLabels: Partial<Record<VaultAdmissionCause, string>> = {
  [VaultAdmissionCause.MAX_SIZE_BOUND]: "at max-size bound",
  [VaultAdmissionCause.STORAGE_DISK_PROTECT]: "storage below floor",
  [VaultAdmissionCause.BACKLOG_BUDGET]: "backlog at budget",
  [VaultAdmissionCause.AGE_BOUND]: "past age bound",
  [VaultAdmissionCause.CHUNK_COUNT_BOUND]: "over chunk-count bound",
};

// vaultRefusingCauseLabels maps the vault's admission-refused causes to their
// terse tooltip phrases, dropping UNSPECIFIED (never emitted by the backend
// today, but the label map has no entry for it — defense in depth).
export function vaultRefusingCauseLabels(refusals: readonly VaultAdmissionRefusal[]): string[] {
  const labels: string[] = [];
  for (const r of refusals) {
    const label = vaultAdmissionCauseLabels[r.cause];
    if (label) labels.push(label);
  }
  return labels;
}

// vaultRefusalDetails pairs each active cause's label with the backend's own
// detail string, verbatim — the expanded card's refusal section renders
// exactly this, never reconstructing the specifics client-side
// (VaultAdmissionRefusal carries {cause, detail} on the wire). Same
// UNSPECIFIED-drop discipline as vaultRefusingCauseLabels.
export interface VaultRefusalDetail {
  label: string;
  detail: string;
}

export function vaultRefusalDetails(refusals: readonly VaultAdmissionRefusal[]): VaultRefusalDetail[] {
  const details: VaultRefusalDetail[] = [];
  for (const r of refusals) {
    const label = vaultAdmissionCauseLabels[r.cause];
    if (label) details.push({ label, detail: r.detail });
  }
  return details;
}

/** The header badge for a vault refusing new records, naming every active cause. */
export function refusingBadgeText(causes: readonly string[]): string {
  return causes.length === 0 ? "refusing" : `refusing: ${causes.join(", ")}`;
}

function nodesUnreached(n: number): string {
  return `${n} ${n === 1 ? "node" : "nodes"} unreached`;
}

/** The chunk table's empty state: ListChunks fans out to every node hosting the vault, so an empty list is a cluster-wide fact unless some host dropped out of the merge. */
export function chunksEmptyText(report: ContributionReport | null | undefined): string {
  const n = report?.degraded.length ?? 0;
  return n > 0 ? `No chunks reported — ${nodesUnreached(n)}.` : "No chunks in this vault yet.";
}

/** The Indexes section's empty state for a sealed chunk that produced no index list. */
export function indexesEmptyText(data?: GetIndexesResponse): string {
  const n = data?.contributionReport?.degraded.length ?? 0;
  return n > 0
    ? `Indexes unknown — the chunk was not found on the nodes that answered, ${nodesUnreached(n)}.`
    : "Indexes unavailable — no node answered for this chunk.";
}

export const INDEX_OK_TITLE = "opened on the node that answered — queries on that node use it";

export const INDEX_MISSING_TITLE =
  "could not be opened on the node that answered — its file is absent or unreadable there; reindex rebuilds it";

/** The archived badge's hover text, naming the cloud storage class the chunk sits in when the backend reports one. */
export function archivedBadgeTitle(cloudStorageClass: string): string {
  return `archived to ${cloudStorageClass || "offline storage"} — restore it before it can be queried`;
}

export const CLOUD_BACKED_VAULT_BADGE = {
  text: "cloud-backed",
  title: "cloud-backed vault — sealed chunks are uploaded to its cloud service",
  helpRef: "storage-cloud",
};
