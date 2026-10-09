import { encode } from "../../api/glid";
import { VaultType, type VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { EligibilityOption } from "./EligibilitySelect";
import type { VaultTypeLabel } from "./VaultsSettings";

// Every reason here names a rejection PutVault makes for
// retention_disposition = "transfer"; the UI must neither offer a pairing the
// server rejects nor withhold one it accepts.

export const TRANSFER_HELP_REF = "vaults-config#transfer-target";

type TransferCandidate = Pick<
  VaultConfig,
  "id" | "name" | "type" | "cloudServiceId" | "retentionDisposition" | "retentionTransferTargetVaultId"
>;

function nonFileTypeReason(t: VaultType): string {
  switch (t) {
    case VaultType.MEMORY:
      return "memory vault";
    case VaultType.JSONL:
      return "JSONL sink";
    default:
      return "not a file vault";
  }
}

// Mirrors PutVault's transfer-graph walk: following transfer edges from the
// candidate target must never revisit a vault, the source included.
function closesTransferCycle(
  targetId: string,
  sourceId: string | undefined,
  byId: Map<string, TransferCandidate>,
): boolean {
  const seen = new Set<string>(sourceId === undefined ? [] : [sourceId]);
  let cur = targetId;
  for (;;) {
    if (seen.has(cur)) return true;
    seen.add(cur);
    const v = byId.get(cur);
    if (v?.retentionDisposition !== "transfer" || v.retentionTransferTargetVaultId.length === 0) return false;
    cur = encode(v.retentionTransferTargetVaultId);
  }
}

/**
 * Every vault as a candidate transfer target, eligible ones first. Each
 * ineligible vault carries the reason PutVault would reject it: the source
 * itself, a non-file vault, a cloud-backed vault, or a target whose
 * transfer chain cycles.
 */
export function transferTargetOptions(
  vaults: TransferCandidate[],
  sourceId?: string,
): EligibilityOption[] {
  const byId = new Map(vaults.map((v) => [encode(v.id), v]));
  const options = vaults.map((v): EligibilityOption => {
    const value = encode(v.id);
    let ineligibleReason: string | undefined;
    if (value === sourceId) ineligibleReason = "this vault";
    else if (v.type !== VaultType.FILE) ineligibleReason = nonFileTypeReason(v.type);
    else if (v.cloudServiceId.length > 0) ineligibleReason = "cloud-backed";
    else if (closesTransferCycle(value, sourceId, byId)) ineligibleReason = "would create a transfer cycle";
    return { value, label: v.name || value, ineligibleReason };
  });
  const eligibleFirst = (o: EligibilityOption) => (o.ineligibleReason === undefined ? 0 : 1);
  return options.sort((a, b) => eligibleFirst(a) - eligibleFirst(b) || a.label.localeCompare(b.label));
}

/**
 * Why a vault cannot itself transfer, or undefined when it can: PutVault
 * accepts transfer only from a non-cloud file vault.
 */
export function transferSourceIneligibility(s: { type: VaultTypeLabel; cloudServiceId: string }): string | undefined {
  switch (s.type) {
    case "memory":
      return "this is a memory vault";
    case "jsonl":
      return "this is a JSONL sink";
    case "file":
      return s.cloudServiceId === "" ? undefined : "this vault is cloud-backed";
  }
}
