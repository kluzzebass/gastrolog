import { encode } from "../../api/glid";
import { VaultType, type VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { EligibilityOption } from "./EligibilitySelect";
import type { VaultTypeLabel } from "./vaultStorageEntry";

// Every reason here names a rejection PutVault makes for
// retention_disposition = "transfer"; the UI must neither offer a pairing the
// server rejects nor withhold one it accepts.

export const TRANSFER_HELP_REF = "vaults-config#transfer-target";

type TransferCandidate = Pick<
  VaultConfig,
  "id" | "name" | "type" | "cloudServiceId" | "retentionDisposition" | "retentionTransferTargetVaultId"
>;

// File and memory vaults both keep sealed chunks transfer can move.
function chunkHolding(t: VaultType): boolean {
  return t === VaultType.FILE || t === VaultType.MEMORY;
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
 * itself, a vault of no buildable type, a cloud-backed vault, or a target
 * whose transfer chain cycles.
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
    else if (!chunkHolding(v.type)) ineligibleReason = "not a file or memory vault";
    else if (v.cloudServiceId.length > 0) ineligibleReason = "cloud-backed";
    else if (closesTransferCycle(value, sourceId, byId)) ineligibleReason = "would create a transfer cycle";
    return { value, label: v.name || value, ineligibleReason };
  });
  const eligibleFirst = (o: EligibilityOption) => (o.ineligibleReason === undefined ? 0 : 1);
  return options.sort((a, b) => eligibleFirst(a) - eligibleFirst(b) || a.label.localeCompare(b.label));
}

/**
 * Why a vault cannot itself transfer, or undefined when it can: PutVault
 * accepts transfer from a file or memory vault that is not cloud-backed.
 */
export function transferSourceIneligibility(s: { type: VaultTypeLabel; cloudServiceId: string }): string | undefined {
  return s.cloudServiceId === "" ? undefined : "this vault is cloud-backed";
}
