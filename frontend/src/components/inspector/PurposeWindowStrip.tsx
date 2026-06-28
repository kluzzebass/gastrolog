import { PEER_PURPOSE_SLOTS } from "./peerPurposeDisplay";

export function PurposeWindowStrip({
  active,
  muted,
}: Readonly<{
  active: ReadonlySet<string>;
  muted?: boolean;
}>) {
  return (
    <span
      className={`inline-flex items-center gap-0.5 tracking-tight ${muted ? "opacity-70" : ""}`}
      aria-label="Purpose activity this interval"
    >
      {PEER_PURPOSE_SLOTS.map((slot) => {
        const on = active.has(slot.id);
        return (
          <span
            key={slot.id}
            title={slot.label}
            className={`inline-block w-[1.1em] text-center select-none ${
              on
                ? ""
                : "opacity-20 grayscale"
            }`}
            aria-hidden={!on}
          >
            {slot.emoji}
          </span>
        );
      })}
    </span>
  );
}
