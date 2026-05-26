import { Record as ProtoRecord } from "./api/client";
import { encode } from "./api/glid";

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}


export type Theme = "dark" | "light" | "system";
export type Palette = "observatory" | "nord" | "solarized" | "dracula" | "catppuccin" | "gruvbox" | "tokyonight" | "rosepine" | "everforest" | "synthwave";

export const timeRangeMs: Record<string, number> = {
  "5m": 5 * 60 * 1000,
  "15m": 15 * 60 * 1000,
  "30m": 30 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "3h": 3 * 60 * 60 * 1000,
  "6h": 6 * 60 * 60 * 1000,
  "12h": 12 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
  "3d": 3 * 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
  "30d": 30 * 24 * 60 * 60 * 1000,
};

const RE_KV_PAIRS =
  /(?:^|[\s,;:()[\]{}])([a-zA-Z_][a-zA-Z0-9_.]*?)=(?:"([^"]*)"|'([^']*)'|([^\s,;)\]}"'=&{[]+))/g;

/** Extract key=value pairs from raw log text (simplified port of Go tokenizer.ExtractKeyValues). */
export function extractKVPairs(raw: string): { key: string; value: string }[] {
  const results: { key: string; value: string }[] = [];
  const seen = new Set<string>();
  RE_KV_PAIRS.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = RE_KV_PAIRS.exec(raw)) !== null) {
    const key = m[1]!.toLowerCase();
    const value = (m[2] ?? m[3] ?? m[4] ?? "").toLowerCase();
    if (key.length > 64 || value.length > 64 || value.length === 0) continue;
    const dedup = `${key}\0${value}`;
    if (seen.has(dedup)) continue;
    seen.add(dedup);
    results.push({ key, value });
  }
  return results;
}

export { relativeTime } from "./utils/temporal";

export { formatBytes } from "./utils/units";

export type FieldSummary = {
  key: string;
  count: number;
  values: { value: string; count: number }[];
};

export function formatChunkId(chunkId: string): string {
  return chunkId || "N/A";
}

/** Stable React key / dedup identity for materialized and spool record refs. */
export function recordRefKey(ref: ProtoRecord["ref"], fallback: string): string {
  if (!ref) return fallback;
  const vault = encode(ref.vaultId);
  if (ref.chunkId?.length) {
    return `${vault}:${encode(ref.chunkId)}:${ref.pos}`;
  }
  if (ref.vaultSeq !== 0n) {
    return `${vault}:seq:${ref.vaultSeq}`;
  }
  return `${vault}:${fallback}`;
}

export function sameRecord(
  a: ProtoRecord | null,
  b: ProtoRecord | null,
): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  const ar = a.ref,
    br = b.ref;
  if (!ar || !br) return false;
  if (!bytesEqual(ar.vaultId, br.vaultId)) return false;
  const aMaterialized = ar.chunkId?.length > 0;
  const bMaterialized = br.chunkId?.length > 0;
  if (aMaterialized && bMaterialized) {
    return bytesEqual(ar.chunkId, br.chunkId) && ar.pos === br.pos;
  }
  if (!aMaterialized && !bMaterialized) {
    return ar.vaultSeq === br.vaultSeq && ar.vaultSeq !== 0n;
  }
  return false;
}

/** Props to make a non-button element keyboard-activatable (Enter/Space). */
export function clickableProps(handler: (() => void) | undefined) {
  if (!handler) return {};
  return {
    role: "button" as const,
    tabIndex: 0,
    onKeyDown: (e: React.KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        handler();
      }
    },
  };
}

export {type Record as ProtoRecord} from "./api/client";