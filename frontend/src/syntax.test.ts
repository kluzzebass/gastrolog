import { describe, expect, test } from "bun:test";
import {
  syntaxHighlight,
  composeWithSearch,
  intervalsToSpans,
  mergeAdjacentSpans,
} from "./syntax";

// ── syntaxHighlight ──

describe("syntaxHighlight", () => {
  test("detects JSON when trimmed text starts with {", () => {
    const spans = syntaxHighlight('  {"key": "value"}');
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe('  {"key": "value"}');
    // Should have colored spans (at minimum the key and value)
    expect(spans.some((s) => s.color !== undefined)).toBe(true);
  });

  test("detects KV/plain for non-JSON text", () => {
    const spans = syntaxHighlight("level=ERROR something happened");
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe("level=ERROR something happened");
  });

  test("returns single span for empty string", () => {
    const spans = syntaxHighlight("");
    expect(spans).toEqual([{ text: "" }]);
  });
});

// ── highlightJSON (via syntaxHighlight) ──

describe("highlightJSON", () => {
  test("colors keys and string values differently", () => {
    const spans = syntaxHighlight('{"name": "alice"}');
    const keySpan = spans.find((s) => s.text.includes("name"));
    const valSpan = spans.find((s) => s.text.includes("alice"));
    expect(keySpan).toBeDefined();
    expect(valSpan).toBeDefined();
    expect(keySpan!.color).not.toBe(valSpan!.color);
  });

  test("colors numbers", () => {
    const spans = syntaxHighlight('{"count": 42}');
    const numSpan = spans.find((s) => s.text === "42");
    expect(numSpan).toBeDefined();
    expect(numSpan!.color).toBeDefined();
  });

  test("colors booleans", () => {
    const spans = syntaxHighlight('{"ok": true}');
    const boolSpan = spans.find((s) => s.text === "true");
    expect(boolSpan).toBeDefined();
    expect(boolSpan!.color).toBeDefined();
  });

  test("colors null keyword", () => {
    const spans = syntaxHighlight('{"val": null}');
    const nullSpan = spans.find((s) => s.text === "null");
    expect(nullSpan).toBeDefined();
    expect(nullSpan!.color).toBeDefined();
  });

  test("colors punctuation ({, }, :, ,)", () => {
    const spans = syntaxHighlight('{"a": 1, "b": 2}');
    const punctSpans = spans.filter(
      (s) => s.text === "{" || s.text === "}" || s.text === ":" || s.text === ",",
    );
    expect(punctSpans.length).toBeGreaterThan(0);
    for (const p of punctSpans) {
      expect(p.color).toBeDefined();
    }
  });

  test("handles nested objects", () => {
    const input = '{"outer": {"inner": "val"}}';
    const spans = syntaxHighlight(input);
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe(input);
  });

  test("handles escaped quotes in strings", () => {
    const input = '{"msg": "say \\"hello\\""}';
    const spans = syntaxHighlight(input);
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe(input);
  });

  test("handles negative numbers", () => {
    const spans = syntaxHighlight('{"temp": -5}');
    const numSpan = spans.find((s) => s.text === "-5");
    expect(numSpan).toBeDefined();
    expect(numSpan!.color).toBeDefined();
  });
});

// ── highlightKVPlain (via syntaxHighlight) ──

describe("highlightKVPlain", () => {
  test("highlights severity keywords", () => {
    const spans = syntaxHighlight("ERROR: something failed");
    const errSpan = spans.find((s) => s.text === "ERROR");
    expect(errSpan).toBeDefined();
    expect(errSpan!.color).toBeDefined();
  });

  test("highlights multiple severity levels", () => {
    for (const level of ["WARN", "WARNING", "INFO", "DEBUG", "TRACE", "FATAL", "CRITICAL"]) {
      const spans = syntaxHighlight(`${level} message`);
      const span = spans.find((s) => s.text === level);
      expect(span).toBeDefined();
      expect(span!.color).toBeDefined();
    }
  });

  test("highlights KV pairs", () => {
    const spans = syntaxHighlight("user=alice status=200");
    // Key=value pairs have the key colored
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe("user=alice status=200");
  });

  test("highlights ISO timestamps", () => {
    const spans = syntaxHighlight("2024-03-15T10:30:00Z request started");
    const tsSpan = spans.find((s) => s.text.includes("2024-03-15T10:30:00Z"));
    expect(tsSpan).toBeDefined();
    expect(tsSpan!.color).toBeDefined();
  });

  test("highlights URLs with clickable link", () => {
    const spans = syntaxHighlight("visit https://example.com/path for info");
    const urlSpan = spans.find((s) => s.url !== undefined);
    expect(urlSpan).toBeDefined();
    expect(urlSpan!.url).toBe("https://example.com/path");
  });

  test("highlights IPv4 addresses", () => {
    const spans = syntaxHighlight("from 192.168.1.1 to 10.0.0.1");
    const ipSpans = spans.filter(
      (s) => s.color !== undefined && (s.text.includes("192.168") || s.text.includes("10.0")),
    );
    expect(ipSpans.length).toBeGreaterThan(0);
  });

  test("highlights file paths", () => {
    const spans = syntaxHighlight("reading /var/log/app.log");
    const pathSpan = spans.find((s) => s.text.includes("/var/log/app.log"));
    expect(pathSpan).toBeDefined();
    expect(pathSpan!.color).toBeDefined();
  });

  test("highlights UUIDs", () => {
    const spans = syntaxHighlight("id=550e8400-e29b-41d4-a716-446655440000");
    const uuidSpan = spans.find((s) =>
      s.text.includes("550e8400-e29b-41d4-a716-446655440000"),
    );
    expect(uuidSpan).toBeDefined();
  });

  test("highlights access logs", () => {
    const line =
      '192.168.1.1 - - [15/Mar/2024:10:30:00 +0000] "GET /api/health HTTP/1.1" 200 512';
    const spans = syntaxHighlight(line);
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe(line);
    // Method (GET) should be colored — may include trailing space from interval
    const getSpan = spans.find((s) => s.text.startsWith("GET"));
    expect(getSpan).toBeDefined();
    expect(getSpan!.color).toBeDefined();
  });

  test("highlights syslog format", () => {
    const line = "<13>Mar 15 10:30:00 myhost sshd[1234]: Accepted publickey";
    const spans = syntaxHighlight(line);
    const text = spans.map((s) => s.text).join("");
    expect(text).toBe(line);
    // Program name should be colored
    const progSpan = spans.find((s) => s.text === "sshd");
    expect(progSpan).toBeDefined();
    expect(progSpan!.color).toBeDefined();
  });
});

// ── composeWithSearch ──

describe("composeWithSearch", () => {
  test("passes through spans with no tokens", () => {
    const spans = [{ text: "hello world", color: "red" }];
    const result = composeWithSearch(spans, []);
    expect(result).toEqual([
      { text: "hello world", color: "red", url: undefined, searchHit: false },
    ]);
  });

  test("marks matching tokens as searchHit", () => {
    const spans = [{ text: "hello world" }];
    const result = composeWithSearch(spans, ["world"]);
    expect(result).toEqual([
      { text: "hello ", color: undefined, url: undefined, searchHit: false },
      { text: "world", color: undefined, url: undefined, searchHit: true },
    ]);
  });

  test("case-insensitive matching", () => {
    const spans = [{ text: "ERROR occurred" }];
    const result = composeWithSearch(spans, ["error"]);
    expect(result[0]!.searchHit).toBe(true);
    expect(result[0]!.text).toBe("ERROR");
  });

  test("preserves color from original spans", () => {
    const spans = [{ text: "hello world", color: "blue" }];
    const result = composeWithSearch(spans, ["world"]);
    const hit = result.find((r) => r.searchHit);
    expect(hit!.color).toBe("blue");
  });

  test("escapes special regex characters in tokens", () => {
    const spans = [{ text: "price is $100.00" }];
    const result = composeWithSearch(spans, ["$100.00"]);
    const hit = result.find((r) => r.searchHit);
    expect(hit).toBeDefined();
    expect(hit!.text).toBe("$100.00");
  });

  test("handles multiple tokens", () => {
    const spans = [{ text: "the quick brown fox" }];
    const result = composeWithSearch(spans, ["quick", "fox"]);
    const hits = result.filter((r) => r.searchHit);
    expect(hits.length).toBe(2);
    expect(hits.map((h) => h.text).sort()).toEqual(["fox", "quick"]);
  });
});

// ── intervalsToSpans ──

describe("intervalsToSpans", () => {
  test("returns single uncolored span with no intervals", () => {
    const result = intervalsToSpans("hello", []);
    expect(result).toEqual([{ text: "hello" }]);
  });

  test("colors a range in the middle", () => {
    const result = intervalsToSpans("hello world", [
      { start: 6, end: 11, color: "red" },
    ]);
    expect(result).toEqual([
      { text: "hello ", color: undefined, url: undefined },
      { text: "world", color: "red", url: undefined },
    ]);
  });

  test("first interval wins on overlap", () => {
    const result = intervalsToSpans("abcde", [
      { start: 0, end: 3, color: "red" },
      { start: 1, end: 4, color: "blue" },
    ]);
    // Characters 0-2 should be red (first wins), char 3 should be blue
    const redSpan = result.find((s) => s.color === "red");
    const blueSpan = result.find((s) => s.color === "blue");
    expect(redSpan).toBeDefined();
    expect(redSpan!.text).toBe("abc");
    expect(blueSpan).toBeDefined();
    expect(blueSpan!.text).toBe("d");
  });

  test("preserves url in interval", () => {
    const result = intervalsToSpans("visit link here", [
      { start: 6, end: 10, color: "blue", url: "https://example.com" },
    ]);
    const urlSpan = result.find((s) => s.url !== undefined);
    expect(urlSpan).toBeDefined();
    expect(urlSpan!.url).toBe("https://example.com");
  });
});

// ── mergeAdjacentSpans ──

describe("mergeAdjacentSpans", () => {
  test("returns empty for empty input", () => {
    expect(mergeAdjacentSpans([])).toEqual([]);
  });

  test("returns single span unchanged", () => {
    const input = [{ text: "hello", color: "red" }];
    expect(mergeAdjacentSpans(input)).toEqual(input);
  });

  test("merges adjacent spans with same color", () => {
    const result = mergeAdjacentSpans([
      { text: "hel", color: "red" },
      { text: "lo", color: "red" },
    ]);
    expect(result).toEqual([{ text: "hello", color: "red" }]);
  });

  test("does not merge spans with different colors", () => {
    const input = [
      { text: "hel", color: "red" },
      { text: "lo", color: "blue" },
    ];
    const result = mergeAdjacentSpans(input);
    expect(result.length).toBe(2);
  });

  test("merges multiple runs", () => {
    const result = mergeAdjacentSpans([
      { text: "a", color: "red" },
      { text: "b", color: "red" },
      { text: "c", color: "blue" },
      { text: "d", color: "blue" },
      { text: "e", color: "red" },
    ]);
    expect(result).toEqual([
      { text: "ab", color: "red" },
      { text: "cd", color: "blue" },
      { text: "e", color: "red" },
    ]);
  });
});
