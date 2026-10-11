import type { PipState } from "./sealPipState";

// Anomalous states glow in their own color so the problem NODE is the
// loudest thing in its row; sealed stays calm everywhere (quiet default).
const PIP_CLASSES: Record<PipState, string> = {
  active: "border-[1.5px] border-copper",
  sealing:
    "border-[1.5px] border-amber bg-linear-to-t from-amber from-50% to-transparent to-50% animate-pulse",
  lagging:
    "border-[1.5px] border-amber bg-linear-to-t from-amber from-50% to-transparent to-50% animate-pulse shadow-[0_0_6px_1px] shadow-amber/70",
  sealed: "bg-severity-info",
  uncached: "border-[1.5px] border-severity-info/40",
  unknown: "border-[1.5px] border-dashed border-text-muted/60",
  missing:
    "border-[1.5px] border-dashed border-severity-error shadow-[0_0_6px_1px] shadow-severity-error/60",
  holds: "bg-severity-error animate-pulse",
  acked: "border-[1.5px] border-severity-error/45",
  ghost: "bg-text-muted/55 shadow-[0_0_6px_1px] shadow-text-muted/50",
};

// A calm row dims its sealed pips by mixing the green into the muted text
// tone. The sealed pip is the one with no border, so an alpha fill over
// the ink surface leaves the whole Status column looking empty.
export const CALM_SEALED_CLASS =
  "bg-[color-mix(in_srgb,var(--color-severity-info)_45%,var(--color-text-muted))]";

export function pipClass(state: PipState, calm: boolean): string {
  return state === "sealed" && calm ? CALM_SEALED_CLASS : PIP_CLASSES[state];
}
