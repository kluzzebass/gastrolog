/** The badge text for a merge that `unreached` peers dropped out of. */
export function partialBadgeText(unreached: number): string {
  return `partial — ${unreached} ${unreached === 1 ? "node" : "nodes"} unreached`;
}
