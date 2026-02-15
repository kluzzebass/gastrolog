// Syntax highlighting for log messages.
// Two-stage pipeline: syntaxHighlight (foreground colors) → composeWithSearch (background overlay).

interface SyntaxSpan {
  text: string;
  color?: string; // CSS color value, undefined = inherit
  url?: string; // If set, render as a clickable link
}

interface HighlightedSpan {
  text: string;
  color?: string;
  searchHit: boolean;
  url?: string;
}

// --- Colors (CSS variable references) ---

const C_KEY = "var(--color-copper)";
const C_STRING = "var(--color-severity-info)";
const C_NUMBER = "var(--color-severity-debug)";
const C_KEYWORD = "var(--color-severity-warn)";
const C_PUNCT = "var(--color-text-ghost)";
const C_DIM = "var(--color-text-ghost)";
const C_SEV_ERROR = "var(--color-severity-error)";
const C_SEV_WARN = "var(--color-severity-warn)";
const C_SEV_INFO = "var(--color-severity-info)";
const C_SEV_DEBUG = "var(--color-severity-debug)";
const C_SEV_TRACE = "var(--color-severity-trace)";

// --- Public API ---

/** Syntax-highlight a log message. Detects JSON vs KV/plain automatically. */
export function syntaxHighlight(text: string): SyntaxSpan[] {
  const trimmed = text.trimStart();
  if (trimmed.startsWith("{")) {
    return highlightJSON(text);
  }
  return highlightKVPlain(text);
}

/** Compose syntax spans with search token highlighting. */
export function composeWithSearch(
  spans: SyntaxSpan[],
  tokens: string[],
): HighlightedSpan[] {
  if (tokens.length === 0) {
    return spans.map((s) => ({
      text: s.text,
      color: s.color,
      url: s.url,
      searchHit: false,
    }));
  }

  const pattern = new RegExp(
    `(${tokens.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})`,
    "gi",
  );

  const result: HighlightedSpan[] = [];

  for (const span of spans) {
    let lastIndex = 0;
    let match: RegExpExecArray | null;
    pattern.lastIndex = 0;

    while ((match = pattern.exec(span.text)) !== null) {
      if (match.index > lastIndex) {
        result.push({
          text: span.text.slice(lastIndex, match.index),
          color: span.color,
          url: span.url,
          searchHit: false,
        });
      }
      result.push({
        text: match[0],
        color: span.color,
        url: span.url,
        searchHit: true,
      });
      lastIndex = pattern.lastIndex;
    }

    if (lastIndex < span.text.length) {
      result.push({
        text: span.text.slice(lastIndex),
        color: span.color,
        url: span.url,
        searchHit: false,
      });
    }
  }

  return result;
}

// --- JSON highlighter ---

function highlightJSON(text: string): SyntaxSpan[] {
  const spans: SyntaxSpan[] = [];
  let i = 0;
  const len = text.length;

  // Track whether next string is a key or value.
  // After { or , (ignoring whitespace) → next string is key.
  // After : → next string/number/keyword is value.
  let expectKey = false;
  let afterColon = false;

  const push = (start: number, end: number, color?: string) => {
    if (end > start) {
      spans.push({ text: text.slice(start, end), color });
    }
  };

  while (i < len) {
    const ch = text[i]!;

    if (ch === "{" || ch === "[") {
      push(i, i + 1, C_PUNCT);
      expectKey = ch === "{";
      afterColon = false;
      i++;
    } else if (ch === "}" || ch === "]") {
      push(i, i + 1, C_PUNCT);
      expectKey = false;
      afterColon = false;
      i++;
    } else if (ch === ",") {
      push(i, i + 1, C_PUNCT);
      expectKey = true;
      afterColon = false;
      i++;
    } else if (ch === ":") {
      push(i, i + 1, C_PUNCT);
      afterColon = true;
      expectKey = false;
      i++;
    } else if (ch === '"') {
      // String — scan to closing quote (handle escapes).
      const start = i;
      i++; // skip opening quote
      while (i < len && text[i] !== '"') {
        if (text[i] === "\\") i++; // skip escaped char
        i++;
      }
      if (i < len) i++; // skip closing quote
      const color = expectKey ? C_KEY : C_STRING;
      push(start, i, color);
      if (expectKey) {
        expectKey = false;
      }
      afterColon = false;
    } else if (afterColon && (ch === "-" || (ch >= "0" && ch <= "9"))) {
      // Number
      const start = i;
      if (ch === "-") i++;
      while (i < len) {
        const d = text[i]!;
        if (
          (d >= "0" && d <= "9") ||
          d === "." ||
          d === "e" ||
          d === "E" ||
          d === "+" ||
          d === "-"
        ) {
          if (
            (d === "+" || d === "-") &&
            text[i - 1] !== "e" &&
            text[i - 1] !== "E"
          )
            break;
          i++;
        } else {
          break;
        }
      }
      push(start, i, C_NUMBER);
      afterColon = false;
    } else if (
      afterColon &&
      (text.startsWith("true", i) ||
        text.startsWith("false", i) ||
        text.startsWith("null", i))
    ) {
      // Boolean/null keyword
      const kw = text.startsWith("true", i)
        ? "true"
        : text.startsWith("false", i)
          ? "false"
          : "null";
      push(i, i + kw.length, C_KEYWORD);
      i += kw.length;
      afterColon = false;
    } else if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      // Whitespace — batch it.
      const start = i;
      while (
        i < len &&
        (text[i] === " " ||
          text[i] === "\t" ||
          text[i] === "\n" ||
          text[i] === "\r")
      ) {
        i++;
      }
      push(start, i);
    } else {
      // Unknown character — emit as-is.
      push(i, i + 1);
      i++;
    }
  }

  return mergeAdjacentSpans(spans);
}

// --- KV/Plain highlighter ---

interface ColorInterval {
  start: number;
  end: number;
  color: string;
  url?: string;
}

// Compiled once at module load; reset lastIndex before each use.
const RE_SEV =
  /\b(ERROR|ERR|WARN(?:ING)?|INFO|DEBUG|TRACE|FATAL|CRITICAL|NOTICE)\b/gi;
const RE_KV =
  /(?:^|[\s,;:()[\]{}])([a-zA-Z_][a-zA-Z0-9_.]*?)=(?:"[^"]*"|'[^']*'|[^\s,;)\]}"'=&{[]+)/g;
const RE_TS =
  /\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?|\[\d{2}\/[A-Z][a-z]{2}\/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\]|\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) [A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}(?:\.\d+)?(?:\s\d{4})?|[A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}/g;
const RE_URL = /\bhttps?:\/\/[^\s"'<>]+/g;
const RE_PATH = /(?:\/[\w.@-]+){2,}(?:\/[\w.@-]*)?|\b[a-zA-Z]:\\[\w.\\-]+/g;
const RE_UUID =
  /\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b/g;
const RE_IPV6 =
  /(?:(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}|::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}|::)(?:\/\d{1,3})?/g;
const RE_IPV4 =
  /\b(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?:\/\d{1,2})?(?::\d{1,5})?\b/g;
const RE_MAC = /\b[0-9a-fA-F]{2}(?:[:-][0-9a-fA-F]{2}){5}\b/g;
const RE_EMAIL = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
const RE_HOST =
  /\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\b/g;
const RE_QUOTED = /"[^"]*"|'[^']*'/g;

// Access log: CLF/Combined format.
// {ip} {ident} {user} [{timestamp}] "{method} {path} {protocol}" {status} {size}
const RE_ACCESS_LOG =
  /^(\S+) \S+ \S+ (\[[^\]]+\]) "(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|TRACE|CONNECT) ([^"]*?) (HTTP\/[\d.]+)" (\d{3}) (\d+|-)/;

// Syslog: RFC 3164 format.
// <priority>timestamp hostname program[pid]: message
const RE_SYSLOG =
  /^(<\d{1,3}>)([A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}) (\S+) (\S+?)(?:\[(\d+)\])?: /;

/** Reset lastIndex on a global regex so it scans from the start. */
function reset(re: RegExp): RegExp {
  re.lastIndex = 0;
  return re;
}

function httpMethodColor(method: string): string {
  switch (method) {
    case "GET":
    case "HEAD":
    case "OPTIONS":
      return C_SEV_INFO;
    case "DELETE":
      return C_SEV_ERROR;
    default: // POST, PUT, PATCH, TRACE, CONNECT
      return C_SEV_WARN;
  }
}

function httpStatusColor(status: string): string {
  switch (status[0]) {
    case "2":
      return C_SEV_INFO;
    case "3":
      return C_DIM;
    case "4":
      return C_SEV_WARN;
    case "5":
      return C_SEV_ERROR;
    default:
      return C_DIM;
  }
}

function highlightKVPlain(text: string): SyntaxSpan[] {
  const intervals: ColorInterval[] = [];
  let m: RegExpExecArray | null;

  // 0. Access log (CLF/Combined) — highest priority, claims key positions.
  const alm = RE_ACCESS_LOG.exec(text);
  if (alm) {
    // Timestamp [...]
    const tsStart = text.indexOf(alm[2]!, alm[1]!.length);
    intervals.push({
      start: tsStart,
      end: tsStart + alm[2]!.length,
      color: C_DIM,
    });

    // Find the quoted request string to locate method/path/protocol within it.
    const qStart = text.indexOf('"' + alm[3]!, tsStart + alm[2]!.length) + 1;

    // Method
    intervals.push({
      start: qStart,
      end: qStart + alm[3]!.length,
      color: httpMethodColor(alm[3]!),
    });

    // Path
    const pathStart = qStart + alm[3]!.length + 1; // +1 for space
    intervals.push({
      start: pathStart,
      end: pathStart + alm[4]!.length,
      color: C_NUMBER,
    });

    // Protocol
    const protoStart = pathStart + alm[4]!.length + 1; // +1 for space
    intervals.push({
      start: protoStart,
      end: protoStart + alm[5]!.length,
      color: C_DIM,
    });

    // Status code — find it after the closing quote + space
    const afterQuote = protoStart + alm[5]!.length + 2; // +2 for '" '
    intervals.push({
      start: afterQuote,
      end: afterQuote + alm[6]!.length,
      color: httpStatusColor(alm[6]!),
    });

    // Quotes and surrounding punctuation — dim them
    const openQuote = qStart - 1;
    intervals.push({ start: openQuote, end: openQuote + 1, color: C_DIM });
    const closeQuote = protoStart + alm[5]!.length;
    intervals.push({ start: closeQuote, end: closeQuote + 1, color: C_DIM });
  }

  // 0b. Syslog (RFC 3164) — <priority>timestamp hostname program[pid]: message
  const slm = RE_SYSLOG.exec(text);
  if (slm) {
    // Priority <NNN> → dim
    intervals.push({ start: 0, end: slm[1]!.length, color: C_DIM });

    // Timestamp → dim
    const tsOff = slm[1]!.length;
    intervals.push({
      start: tsOff,
      end: tsOff + slm[2]!.length,
      color: C_DIM,
    });

    // Hostname → number color (same as IPs/hostnames)
    const hostOff = tsOff + slm[2]!.length + 1; // +1 for space
    intervals.push({
      start: hostOff,
      end: hostOff + slm[3]!.length,
      color: C_NUMBER,
    });

    // Program name → key color (copper)
    const progOff = hostOff + slm[3]!.length + 1; // +1 for space
    intervals.push({
      start: progOff,
      end: progOff + slm[4]!.length,
      color: C_KEY,
    });

    // PID if present → number color
    if (slm[5]) {
      const pidOff = progOff + slm[4]!.length + 1; // +1 for [
      intervals.push({
        start: pidOff,
        end: pidOff + slm[5].length,
        color: C_NUMBER,
      });
      // Dim the brackets
      intervals.push({
        start: pidOff - 1,
        end: pidOff,
        color: C_DIM,
      });
      intervals.push({
        start: pidOff + slm[5].length,
        end: pidOff + slm[5].length + 1,
        color: C_DIM,
      });
    }
  }

  // 1. Severity keywords.
  reset(RE_SEV);
  while ((m = RE_SEV.exec(text)) !== null) {
    const word = m[1]!.toUpperCase();
    let color: string;
    if (
      word === "ERROR" ||
      word === "ERR" ||
      word === "FATAL" ||
      word === "CRITICAL"
    ) {
      color = C_SEV_ERROR;
    } else if (word === "WARN" || word === "WARNING") {
      color = C_SEV_WARN;
    } else if (word === "INFO" || word === "NOTICE") {
      color = C_SEV_INFO;
    } else if (word === "DEBUG") {
      color = C_SEV_DEBUG;
    } else {
      color = C_SEV_TRACE;
    }
    intervals.push({ start: m.index, end: m.index + m[0].length, color });
  }

  // 2. Key=value pairs — color the key and = sign.
  reset(RE_KV);
  while ((m = RE_KV.exec(text)) !== null) {
    const fullMatch = m[0];
    const keyStart = m.index + fullMatch.indexOf(m[1]!);
    const keyEnd = keyStart + m[1]!.length + 1; // +1 for the = sign
    intervals.push({ start: keyStart, end: keyEnd, color: C_DIM });
  }

  // 3. Timestamps: ISO, CLF, Go/Ruby, ctime, syslog BSD.
  reset(RE_TS);
  while ((m = RE_TS.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_DIM,
    });
  }

  // 4. URLs (consumes the full URL so hostname/IP don't split it).
  reset(RE_URL);
  while ((m = RE_URL.exec(text)) !== null) {
    let end = m.index + m[0].length;
    while (end > m.index && /[).,;:!?\]}]/.test(text[end - 1]!)) end--;
    const href = text.slice(m.index, end);
    intervals.push({ start: m.index, end, color: C_NUMBER, url: href });
  }

  // 5. File paths (absolute unix/windows paths).
  reset(RE_PATH);
  while ((m = RE_PATH.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 6. UUIDs (8-4-4-4-12 hex).
  reset(RE_UUID);
  while ((m = RE_UUID.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 7. IPv6 addresses.
  reset(RE_IPV6);
  while ((m = RE_IPV6.exec(text)) !== null) {
    if (m[0].length >= 3) {
      intervals.push({
        start: m.index,
        end: m.index + m[0].length,
        color: C_NUMBER,
      });
    }
  }

  // 8. IPv4 addresses (with optional /CIDR or :port).
  reset(RE_IPV4);
  while ((m = RE_IPV4.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 9. MAC addresses.
  reset(RE_MAC);
  while ((m = RE_MAC.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 10. Email addresses.
  reset(RE_EMAIL);
  while ((m = RE_EMAIL.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 11. Hostnames (at least one dot, TLD 2-6 alpha chars).
  reset(RE_HOST);
  while ((m = RE_HOST.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_NUMBER,
    });
  }

  // 12. Quoted strings.
  reset(RE_QUOTED);
  while ((m = RE_QUOTED.exec(text)) !== null) {
    intervals.push({
      start: m.index,
      end: m.index + m[0].length,
      color: C_STRING,
    });
  }

  return intervalsToSpans(text, intervals);
}

/** Convert a set of (possibly overlapping) color intervals to non-overlapping spans.
 *  First interval wins at each character position (priority by insertion order). */
export function intervalsToSpans(
  text: string,
  intervals: ColorInterval[],
): SyntaxSpan[] {
  if (intervals.length === 0) return [{ text }];

  // Build per-character color+url map. First match wins.
  const colors = new Array<string | undefined>(text.length);
  const urls = new Array<string | undefined>(text.length);
  for (const iv of intervals) {
    for (let i = iv.start; i < iv.end && i < text.length; i++) {
      if (colors[i] === undefined) {
        colors[i] = iv.color;
        urls[i] = iv.url;
      }
    }
  }

  // Merge runs of same color+url into spans.
  const spans: SyntaxSpan[] = [];
  let runStart = 0;
  let runColor = colors[0];
  let runUrl = urls[0];

  for (let i = 1; i <= text.length; i++) {
    const c = i < text.length ? colors[i] : null; // sentinel
    const u = i < text.length ? urls[i] : null;
    if (c !== runColor || u !== runUrl) {
      spans.push({
        text: text.slice(runStart, i),
        color: runColor,
        url: runUrl,
      });
      runStart = i;
      runColor = c ?? undefined;
      runUrl = u ?? undefined;
    }
  }

  return spans;
}

/** Merge adjacent spans with the same color to reduce span count. */
export function mergeAdjacentSpans(spans: SyntaxSpan[]): SyntaxSpan[] {
  if (spans.length <= 1) return spans;
  const merged: SyntaxSpan[] = [spans[0]!];
  for (let i = 1; i < spans.length; i++) {
    const prev = merged[merged.length - 1]!;
    if (prev.color === spans[i]!.color) {
      prev.text += spans[i]!.text;
    } else {
      merged.push(spans[i]!);
    }
  }
  return merged;
}
