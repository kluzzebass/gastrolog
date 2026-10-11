import type { Storage } from "../../api/model/storage";
import { formatBytes } from "../../utils/units";

// storageVerdictLabel renders the storage's badge grammar as text — the
// same two server-computed, hysteresis-aware booleans the admission gate
// itself consults (StorageState.warn_verdict / protect_verdict), never
// re-derived from free/warn/floor here. Matches the CLI's storageVerdict
// (UI/CLI parity) and the vault card's "refusing" grammar:
// a badge appears only while the condition is active, nothing when healthy.
export function storageVerdictLabel(storage: Pick<Storage, "warnVerdict" | "protectVerdict">): "protected" | "warn" | null {
  if (storage.protectVerdict) return "protected";
  if (storage.warnVerdict) return "warn";
  return null;
}

// thresholdLabel renders an effective threshold with its provenance — the
// resolved bytes value always leads (placeholder-style: the effective
// value is what matters). expr is the EFFECTIVE expression from the wire,
// verbatim, never re-derived here (render the wire). Mirrors the CLI's
// thresholdLabel exactly.
//
// isDefault storages get "(expr, default)" — there is no configurable
// node-level override to inherit from, so an unset expression is
// DEFAULTED, never "inherited". An explicit percentage expression ("10%")
// still gets "(expr)": a percentage carries information the resolved byte
// count alone can't (it rescales with the volume). An explicit
// absolute-size expression ("20GiB") resolves to exactly the shown byte
// count, so appending it would just repeat the same number in a second
// spelling — the bytes alone are the complete, non-redundant answer.
export function thresholdLabel(expr: string, isDefault: boolean, effectiveBytes: bigint): string {
  const eff = formatBytes(effectiveBytes);
  // Empty expr from a build predating the effective-expression field:
  // "(default)" alone beats "(, default)".
  if (isDefault) return expr === "" ? `${eff} (default)` : `${eff} (${expr}, default)`;
  if (expr.includes("%")) return `${eff} (${expr})`;
  return eff;
}

/** The header's space figure: free bytes against the volume's total, both from the last disk-guard sample. */
export function freeOfTotalLabel(freeBytes: bigint, totalBytes: bigint): string {
  return `${formatBytes(freeBytes)} free of ${formatBytes(totalBytes)}`;
}
