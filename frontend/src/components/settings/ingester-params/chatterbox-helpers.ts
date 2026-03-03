export const ALL_FORMATS = [
  { id: "plain", label: "Plain Text", description: "Unstructured log lines" },
  { id: "json", label: "JSON", description: "Structured JSON objects" },
  { id: "kv", label: "Key-Value", description: "key=value pairs" },
  { id: "access", label: "Access Log", description: "HTTP access log format" },
  { id: "syslog", label: "Syslog", description: "RFC 5424 syslog messages" },
  { id: "weird", label: "Weird", description: "Unusual / malformed entries" },
  {
    id: "multirecord",
    label: "Multi-Record",
    description: "Stack dumps, help output — each line as separate record",
  },
] as const;

export function parseFormats(raw: string): Set<string> {
  if (!raw.trim()) return new Set(ALL_FORMATS.map((f) => f.id));
  return new Set(
    raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );
}

export function parseWeights(raw: string): Record<string, number> {
  const weights: Record<string, number> = {};
  if (!raw.trim()) return weights;
  for (const pair of raw.split(",")) {
    const eq = pair.indexOf("=");
    if (eq === -1) continue;
    const name = pair.slice(0, eq).trim();
    const val = parseInt(pair.slice(eq + 1).trim(), 10);
    if (name && !isNaN(val) && val > 0) weights[name] = val;
  }
  return weights;
}

export function serializeFormats(enabled: Set<string>): string {
  // If all enabled, return empty (backend default is all)
  if (
    enabled.size === ALL_FORMATS.length &&
    ALL_FORMATS.every((f) => enabled.has(f.id))
  )
    return "";
  return ALL_FORMATS.filter((f) => enabled.has(f.id))
    .map((f) => f.id)
    .join(",");
}

export function serializeWeights(
  weights: Record<string, number>,
  enabled: Set<string>,
): string {
  const parts: string[] = [];
  for (const f of ALL_FORMATS) {
    if (!enabled.has(f.id)) continue;
    const w = weights[f.id];
    if (w !== undefined && w !== 1) parts.push(`${f.id}=${w}`);
  }
  return parts.join(",");
}
