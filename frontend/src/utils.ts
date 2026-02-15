import { Record as ProtoRecord } from "./api/client";

export type { ProtoRecord };
export type Theme = "dark" | "light" | "system";

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

/** Format a relative time string (e.g., "3s ago", "2m ago"). */
export function relativeTime(date: Date): string {
  const now = Date.now();
  const diffMs = now - date.getTime();
  if (diffMs < 0) return "in the future";
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const days = Math.floor(hr / 24);
  return `${days}d ago`;
}

export { formatBytes } from "./utils/units";

export type FieldSummary = {
  key: string;
  count: number;
  values: { value: string; count: number }[];
};

export function aggregateFields(
  records: ProtoRecord[],
  source: "attrs" | "kv",
): FieldSummary[] {
  const keyMap = new Map<string, Map<string, number>>();
  const decoder = new TextDecoder();
  for (const record of records) {
    const pairs: [string, string][] =
      source === "attrs"
        ? Object.entries(record.attrs)
        : extractKVPairs(decoder.decode(record.raw)).map((p) => [
            p.key,
            p.value,
          ]);
    for (const [key, value] of pairs) {
      if (source === "kv" && key === "level") continue;
      let valMap = keyMap.get(key);
      if (!valMap) {
        valMap = new Map();
        keyMap.set(key, valMap);
      }
      valMap.set(value, (valMap.get(value) ?? 0) + 1);
    }
  }
  return Array.from(keyMap.entries())
    .map(([key, valMap]) => ({
      key,
      count: Array.from(valMap.values()).reduce((a, b) => a + b, 0),
      values: Array.from(valMap.entries())
        .map(([value, count]) => ({ value, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 10),
    }))
    .sort((a, b) => b.count - a.count);
}

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
    ar.chunkId === br.chunkId && ar.pos === br.pos && ar.storeId === br.storeId
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
