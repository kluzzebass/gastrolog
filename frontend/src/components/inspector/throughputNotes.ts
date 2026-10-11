/**
 * Status notes for a quiet throughput stage row: why that node shows no
 * activity on that stage. "warn" marks a stall rather than a healthy rest.
 */
export interface IdleNote {
  text: string;
  warn: boolean;
}

export function appendIdleNote(): IdleNote {
  return { text: "no ingest", warn: false };
}

/** A home's Collected row: its head count against the registry. */
export function collectIdleNote(headSegments: number | undefined, publishedSegments: number): IdleNote {
  if (headSegments === undefined) return { text: "idle", warn: false };
  if (publishedSegments === 0) return { text: "nothing published", warn: false };
  if (headSegments >= publishedSegments) return { text: "caught up", warn: false };
  return { text: `behind ${(publishedSegments - headSegments).toLocaleString()} segments`, warn: true };
}

/** A home's Sealed row: what the chunking planner still has in front of it. */
export function sealIdleNote(eligibleSegments: number, sealedManifestPending: boolean): IdleNote {
  if (eligibleSegments > 0) {
    return { text: `${eligibleSegments.toLocaleString()} segments eligible to chunk`, warn: true };
  }
  if (sealedManifestPending) return { text: "sealed manifest awaiting build", warn: true };
  return { text: "up to date", warn: false };
}
