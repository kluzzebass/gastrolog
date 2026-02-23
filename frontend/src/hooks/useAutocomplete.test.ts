import { describe, expect, test } from "bun:test";
import {
  isWordBreak,
  wordAtCursor,
  getValueContext,
  getPipeContext,
  bodyHasToken,
  prevWordBeforeCursor,
  resolveGrammar,
  PIPE_GRAMMARS,
} from "./useAutocomplete";

// ── isWordBreak ──

describe("isWordBreak", () => {
  test("space is a word break", () => {
    expect(isWordBreak(" ")).toBe(true);
  });

  test("tab is a word break", () => {
    expect(isWordBreak("\t")).toBe(true);
  });

  test("newline is a word break", () => {
    expect(isWordBreak("\n")).toBe(true);
  });

  test("parentheses are word breaks", () => {
    expect(isWordBreak("(")).toBe(true);
    expect(isWordBreak(")")).toBe(true);
  });

  test("equals sign is a word break", () => {
    expect(isWordBreak("=")).toBe(true);
  });

  test("asterisk is a word break", () => {
    expect(isWordBreak("*")).toBe(true);
  });

  test("quotes are word breaks", () => {
    expect(isWordBreak('"')).toBe(true);
    expect(isWordBreak("'")).toBe(true);
  });

  test("pipe is a word break", () => {
    expect(isWordBreak("|")).toBe(true);
  });

  test("comma is a word break", () => {
    expect(isWordBreak(",")).toBe(true);
  });

  test("letters are not word breaks", () => {
    expect(isWordBreak("a")).toBe(false);
    expect(isWordBreak("Z")).toBe(false);
  });

  test("digits are not word breaks", () => {
    expect(isWordBreak("0")).toBe(false);
    expect(isWordBreak("9")).toBe(false);
  });

  test("hyphens and underscores are not word breaks", () => {
    expect(isWordBreak("-")).toBe(false);
    expect(isWordBreak("_")).toBe(false);
  });
});

// ── wordAtCursor ──

describe("wordAtCursor", () => {
  test("extracts word at cursor in the middle", () => {
    const result = wordAtCursor("hello world", 3);
    expect(result).toEqual({ word: "hel", start: 0, end: 5 });
  });

  test("extracts word at end of text", () => {
    const result = wordAtCursor("hello", 5);
    expect(result).toEqual({ word: "hello", start: 0, end: 5 });
  });

  test("extracts second word", () => {
    const result = wordAtCursor("hello world", 8);
    expect(result).toEqual({ word: "wo", start: 6, end: 11 });
  });

  test("returns null when cursor is at position 0", () => {
    expect(wordAtCursor("hello", 0)).toBeNull();
  });

  test("returns null when cursor is beyond text length", () => {
    expect(wordAtCursor("hello", 10)).toBeNull();
  });

  test("returns null when cursor is at a word break", () => {
    expect(wordAtCursor("hello ", 6)).toBeNull();
  });

  test("returns null when cursor is right after = (word break)", () => {
    expect(wordAtCursor("key=value", 4)).toBeNull();
  });

  test("extracts value part when cursor is inside value", () => {
    const result = wordAtCursor("key=value", 7);
    expect(result).toEqual({ word: "val", start: 4, end: 9 });
  });

  test("word part is only text before cursor", () => {
    const result = wordAtCursor("longword", 4);
    expect(result).toEqual({ word: "long", start: 0, end: 8 });
  });

  test("extracts word after pipe", () => {
    const result = wordAtCursor("error |stats", 12);
    expect(result).toEqual({ word: "stats", start: 7, end: 12 });
  });
});

// ── getValueContext ──

describe("getValueContext", () => {
  test("returns key when cursor is after key=", () => {
    expect(getValueContext("level=err", 6)).toBe("level");
  });

  test("returns null when no = before word start", () => {
    expect(getValueContext("hello world", 6)).toBeNull();
  });

  test("returns null at start of text", () => {
    expect(getValueContext("hello", 0)).toBeNull();
  });

  test("returns null when = is not immediately before word start", () => {
    expect(getValueContext("hello", 3)).toBeNull();
  });

  test("returns key for complex key names", () => {
    expect(getValueContext("my_key=val", 7)).toBe("my_key");
  });

  test("returns null for empty key before =", () => {
    expect(getValueContext("=val", 1)).toBeNull();
  });
});

// ── getPipeContext ──

describe("getPipeContext", () => {
  test("returns null when no pipe in text", () => {
    expect(getPipeContext("error level=info", 16)).toBeNull();
  });

  test("returns keyword kind when cursor is right after pipe", () => {
    expect(getPipeContext("error | ", 8)).toEqual({ kind: "keyword" });
  });

  test("returns keyword kind when typing keyword", () => {
    expect(getPipeContext("error |sta", 10)).toEqual({ kind: "keyword" });
  });

  test("returns keyword kind with no text after pipe", () => {
    expect(getPipeContext("error |", 7)).toEqual({ kind: "keyword" });
  });

  test("returns body kind when past the keyword", () => {
    expect(getPipeContext("error |stats ", 13)).toEqual({
      kind: "body",
      keyword: "stats",
      bodyStart: 12,
    });
  });

  test("returns body kind when typing in body", () => {
    expect(getPipeContext("error |stats co", 15)).toEqual({
      kind: "body",
      keyword: "stats",
      bodyStart: 12,
    });
  });

  test("ignores pipe inside double quotes", () => {
    expect(getPipeContext('"a|b" foo', 9)).toBeNull();
  });

  test("ignores pipe inside single quotes", () => {
    expect(getPipeContext("'a|b' foo", 9)).toBeNull();
  });

  test("detects pipe after quoted segment", () => {
    const result = getPipeContext('"a|b" |stats ', 13);
    expect(result).toEqual({ kind: "body", keyword: "stats", bodyStart: 12 });
  });

  test("uses last unquoted pipe", () => {
    const result = getPipeContext("a |stats count |where ", 22);
    expect(result).toEqual({ kind: "body", keyword: "where", bodyStart: 21 });
  });
});

// ── bodyHasToken ──

describe("bodyHasToken", () => {
  test("returns false for empty body", () => {
    expect(bodyHasToken("", "by")).toBe(false);
  });

  test("returns false when token absent", () => {
    expect(bodyHasToken(" avg latency ", "by")).toBe(false);
  });

  test("returns true when token present", () => {
    expect(bodyHasToken(" avg latency by source ", "by")).toBe(true);
  });

  test("case-insensitive match", () => {
    expect(bodyHasToken(" count BY level ", "by")).toBe(true);
  });
});

// ── prevWordBeforeCursor ──

describe("prevWordBeforeCursor", () => {
  test("finds previous word", () => {
    expect(prevWordBeforeCursor("| stats count by le", 17, 0)).toBe("by");
  });

  test("returns null when no previous word", () => {
    expect(prevWordBeforeCursor("| stats", 2, 0)).toBeNull();
  });

  test("skips whitespace to find previous word", () => {
    expect(prevWordBeforeCursor("| stats   count", 10, 0)).toBe("stats");
  });
});

// ── resolveGrammar ──

describe("resolveGrammar", () => {
  const stats = PIPE_GRAMMARS["stats"]!;
  const timechart = PIPE_GRAMMARS["timechart"]!;
  const rename = PIPE_GRAMMARS["rename"]!;
  const head = PIPE_GRAMMARS["head"]!;
  const where_ = PIPE_GRAMMARS["where"]!;
  const eval_ = PIPE_GRAMMARS["eval"]!;

  // stats
  test("stats: empty body → aggs only", () => {
    expect(resolveGrammar(stats, null, "")).toEqual({ aggs: true });
  });

  test("stats: after as → none", () => {
    expect(resolveGrammar(stats, "as", " count as ")).toBe("none");
  });

  test("stats: after by → fields + bin", () => {
    expect(resolveGrammar(stats, "by", " count by ")).toEqual({
      fields: true,
      literals: ["bin"],
    });
  });

  test("stats: past by clause → fields + bin", () => {
    expect(resolveGrammar(stats, "source", " count by source ")).toEqual({
      fields: true,
      literals: ["bin"],
    });
  });

  test("stats: general body → funcs + fields + as + by", () => {
    expect(resolveGrammar(stats, "latency", " avg latency ")).toEqual({
      funcs: true,
      fields: true,
      literals: ["as", "by"],
    });
  });

  // timechart
  test("timechart: empty body → none (expects number)", () => {
    expect(resolveGrammar(timechart, null, "")).toBe("none");
  });

  test("timechart: after number → by", () => {
    expect(resolveGrammar(timechart, "50", " 50 ")).toEqual({
      literals: ["by"],
    });
  });

  test("timechart: after by → fields", () => {
    expect(resolveGrammar(timechart, "by", " 50 by ")).toEqual({
      fields: true,
    });
  });

  test("timechart: past by → fields", () => {
    expect(resolveGrammar(timechart, "status", " 50 by status ")).toEqual({
      fields: true,
    });
  });

  test("timechart: random word → none", () => {
    expect(resolveGrammar(timechart, "abc", " abc ")).toBe("none");
  });

  // rename
  test("rename: empty body → fields", () => {
    expect(resolveGrammar(rename, null, "")).toEqual({ fields: true });
  });

  test("rename: after as → none (user types new name)", () => {
    expect(resolveGrammar(rename, "as", " old as ")).toBe("none");
  });

  test("rename: general body → fields + as", () => {
    expect(resolveGrammar(rename, "old", " old ")).toEqual({
      fields: true,
      literals: ["as"],
    });
  });

  // head/tail/slice — always none
  test("head: empty → none", () => {
    expect(resolveGrammar(head, null, "")).toBe("none");
  });

  test("head: with content → none", () => {
    expect(resolveGrammar(head, "10", " 10 ")).toBe("none");
  });

  // where
  test("where: empty body → funcs + fields", () => {
    expect(resolveGrammar(where_, null, "")).toEqual({
      funcs: true,
      fields: true,
    });
  });

  test("where: general body → funcs + fields", () => {
    expect(resolveGrammar(where_, "level", " level ")).toEqual({
      funcs: true,
      fields: true,
    });
  });

  // eval
  test("eval: empty body → fields (assignment target)", () => {
    expect(resolveGrammar(eval_, null, "")).toEqual({ fields: true });
  });

  test("eval: general body → funcs + fields", () => {
    expect(resolveGrammar(eval_, "x", " x ")).toEqual({
      funcs: true,
      fields: true,
    });
  });
});
