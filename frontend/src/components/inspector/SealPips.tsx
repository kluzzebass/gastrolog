import type { SealPip, PipState } from "./sealPipState";

// SealPips renders the per-node copy-seal lifecycle for one chunk row
// (design reference docs/mockups/seal-pips.html).
// One circle per placement node, identical order in every row. Fill
// degree carries the lifecycle (hollow → half → solid) so the grammar
// survives colorblindness and small sizes; color is secondary. Ghost
// pips (stale residency) append after a small gap. Cloud-backed chunks
// render a BlobMark instead.

// Anomalous states glow in their own color so the problem NODE is the
// loudest thing in its row; sealed stays calm everywhere (quiet default).
const PIP_CLASSES: Record<PipState, string> = {
  active: "border-[1.5px] border-copper",
  sealing:
    "border-[1.5px] border-amber bg-linear-to-t from-amber from-50% to-transparent to-50% animate-pulse",
  lagging:
    "border-[1.5px] border-amber bg-linear-to-t from-amber from-50% to-transparent to-50% animate-pulse shadow-[0_0_6px_1px] shadow-amber/70",
  sealed: "bg-severity-info/45",
  uncached: "border-[1.5px] border-severity-info/40",
  unknown: "border-[1.5px] border-dashed border-text-muted/60",
  missing:
    "border-[1.5px] border-dashed border-severity-error shadow-[0_0_6px_1px] shadow-severity-error/60",
  holds: "bg-severity-error animate-pulse",
  acked: "border-[1.5px] border-severity-error/45",
  ghost: "bg-text-muted/55 shadow-[0_0_6px_1px] shadow-text-muted/50",
};

function Pip({ pip, size }: Readonly<{ pip: SealPip; size: number }>) {
  return (
    <span
      className={`relative inline-block rounded-full ${PIP_CLASSES[pip.state]}`}
      style={{ width: size, height: size }}
      title={pip.title}
      aria-label={pip.title}
    >
      {pip.state === "missing" && (
        <span
          className="absolute left-0 right-0 top-1/2 h-[1.3px] -rotate-45 bg-severity-error"
          aria-hidden
        />
      )}
    </span>
  );
}

export function SealPips({
  pips,
  ghosts,
  size = 11,
}: Readonly<{
  pips: SealPip[];
  ghosts: SealPip[];
  size?: number;
}>) {
  if (pips.length === 0 && ghosts.length === 0) return null;
  return (
    <span className="inline-flex items-center gap-1.5">
      {pips.map((p) => (
        <Pip key={p.node} pip={p} size={size} />
      ))}
      {ghosts.length > 0 && <span aria-hidden style={{ width: 8 }} />}
      {ghosts.map((p) => (
        <Pip key={p.node} pip={p} size={size} />
      ))}
    </span>
  );
}

// BlobMark replaces the pip row for cloud-backed chunks: residency is the
// blob store, not homes. Dashed and pulsing while the upload runs, solid
// once the bytes are durable. The label is the operator-configured store
// name — vendor words appear only as operator-supplied labels.
export function BlobMark({
  label,
  uploading,
}: Readonly<{ label: string; uploading: boolean }>) {
  const state = uploading
    ? "border-dashed animate-pulse"
    : "border-solid";
  const title = uploading
    ? `uploading to ${label} — not yet durable in the blob store`
    : `cloud-backed — bytes durable in ${label}`;
  return (
    <span
      className={`inline-flex h-[13px] items-center justify-center rounded-[7px] border-[1.5px] border-copper px-[7px] text-[9px] font-bold tracking-[0.05em] text-copper ${state}`}
      title={title}
      aria-label={title}
    >
      {label}
    </span>
  );
}
