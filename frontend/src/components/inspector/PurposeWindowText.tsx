import { orderPeerPurposes, peerPurposeLabel } from "./peerPurposeDisplay";

/**
 * The purposes a lane has carried, as text: a purpose that used the lane
 * during the last stats interval reads normally, one that did not is dimmed.
 */
export function PurposeWindowText({
  carried,
  active,
}: Readonly<{
  carried: readonly string[];
  active: ReadonlySet<string>;
}>) {
  const purposes = orderPeerPurposes(carried);
  if (purposes.length === 0) {
    return <>—</>;
  }
  return (
    <span className="whitespace-nowrap" aria-label="Purposes carried by this lane; dimmed ones were idle this interval">
      {purposes.map((id, i) => {
        const on = active.has(id);
        const label = peerPurposeLabel(id);
        return (
          <span key={id}>
            {i > 0 ? ", " : ""}
            <span
              className={on ? undefined : "opacity-40"}
              title={on ? `${label} — active this interval` : `${label} — idle this interval`}
              data-active={on ? "true" : "false"}
            >
              {label}
            </span>
          </span>
        );
      })}
    </span>
  );
}
