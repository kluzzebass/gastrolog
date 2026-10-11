import type { Timestamp } from "@bufbuild/protobuf";
import { formatDuration } from "../../utils/units";

/** The text shown in place of a node's stats: never heard from, or silent since the cluster's last contact. */
export function nodeStatsEmptyText(lastSeen: Timestamp | undefined, nowMs: number): string {
  const seen = lastSeen?.seconds ?? 0n;
  if (seen <= 0n) return "No stats yet — waiting for this node's first broadcast.";
  const elapsed = BigInt(Math.floor(nowMs / 1000)) - seen;
  const ago = elapsed >= 1n ? formatDuration(elapsed) : "";
  return ago
    ? `No stats — the cluster last heard from this node ${ago} ago.`
    : "No stats — the cluster heard from this node just now; its broadcast is still pending.";
}

/** Raft's last-contact duration rounded to one decimal; an empty value reads as never. */
export function formatRaftContact(contact: string): string {
  if (!contact) return "never";
  const m = /^(\d+(?:\.\d+)?)(ns|µs|us|ms|s)$/.exec(contact);
  if (!m) return contact;
  const n = Number(m[1]);
  return `${Number.isInteger(n) ? String(n) : n.toFixed(1)}${m[2]}`;
}

/** The cluster's leader row: a name, or the no-quorum state spelled out. */
export function leaderLabel(leaderName: string): string {
  return leaderName || "none — no quorum";
}
