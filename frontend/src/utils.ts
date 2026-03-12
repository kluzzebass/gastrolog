import { Record as ProtoRecord } from "./api/client";


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

export function sameRecord(
  a: ProtoRecord | null,
  b: ProtoRecord | null,
): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  const ar = a.ref,
    br = b.ref;
  if (!ar || !br) return false;
  return (
    ar.chunkId === br.chunkId && ar.pos === br.pos && ar.vaultId === br.vaultId
  );
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