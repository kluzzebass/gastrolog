import { describe, expect, test } from "bun:test";
import { isWordBreak, wordAtCursor, getValueContext } from "./useAutocomplete";

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
    // Cursor at position 6 is right after the space, no word typed yet
    expect(wordAtCursor("hello ", 6)).toBeNull();
  });

  test("returns null when cursor is right after = (word break)", () => {
    // Position 4 in "key=value" is right after "=", which is a break.
    // Scanning backward from 4: char 3 is "=" (break), so start=4.
    // word = text.slice(4, 4) = "" → returns null.
    expect(wordAtCursor("key=value", 4)).toBeNull();
  });

  test("extracts value part when cursor is inside value", () => {
    // Position 7 in "key=value": scans back to 4 (after =), word="val"
    const result = wordAtCursor("key=value", 7);
    expect(result).toEqual({ word: "val", start: 4, end: 9 });
  });

  test("word part is only text before cursor", () => {
    const result = wordAtCursor("longword", 4);
    expect(result).toEqual({ word: "long", start: 0, end: 8 });
  });
});

// ── getValueContext ──

describe("getValueContext", () => {
  test("returns key when cursor is after key=", () => {
    // In "level=err", if word starts at index 6 (after =)
    expect(getValueContext("level=err", 6)).toBe("level");
  });

  test("returns null when no = before word start", () => {
    expect(getValueContext("hello world", 6)).toBeNull();
  });

  test("returns null at start of text", () => {
    expect(getValueContext("hello", 0)).toBeNull();
  });

  test("returns null when = is not immediately before word start", () => {
    // wordStart=3, char at 2 is 'l', not '='
    expect(getValueContext("hello", 3)).toBeNull();
  });

  test("returns key for complex key names", () => {
    expect(getValueContext("my_key=val", 7)).toBe("my_key");
  });

  test("returns null for empty key before =", () => {
    // "=val" — key is empty
    expect(getValueContext("=val", 1)).toBeNull();
  });
});
