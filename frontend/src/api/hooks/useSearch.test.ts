import { describe, expect, test } from "bun:test";
import { extractTokens } from "./useSearch";

describe("extractTokens", () => {
  test("extracts bare words", () => {
    expect(extractTokens("error timeout")).toEqual(["error", "timeout"]);
  });

  test("lowercases tokens", () => {
    expect(extractTokens("ERROR Timeout")).toEqual(["error", "timeout"]);
  });

  test("filters out AND operator", () => {
    expect(extractTokens("error AND timeout")).toEqual(["error", "timeout"]);
  });

  test("filters out OR operator", () => {
    expect(extractTokens("error OR warning")).toEqual(["error", "warning"]);
  });

  test("filters out NOT operator", () => {
    expect(extractTokens("NOT debug")).toEqual(["debug"]);
  });

  test("operators are case-insensitive", () => {
    expect(extractTokens("error and timeout")).toEqual(["error", "timeout"]);
    expect(extractTokens("error or timeout")).toEqual(["error", "timeout"]);
    expect(extractTokens("not debug")).toEqual(["debug"]);
  });

  test("filters out reverse= directive", () => {
    expect(extractTokens("reverse=true error")).toEqual(["error"]);
  });

  test("filters out start= directive", () => {
    expect(extractTokens("start=2024-01-01 error")).toEqual(["error"]);
  });

  test("filters out end= directive", () => {
    expect(extractTokens("end=2024-12-31 error")).toEqual(["error"]);
  });

  test("filters out last= directive", () => {
    expect(extractTokens("last=5m error")).toEqual(["error"]);
  });

  test("filters out store= directive", () => {
    expect(extractTokens("store=mystore error")).toEqual(["error"]);
  });

  test("filters out limit= directive", () => {
    expect(extractTokens("limit=100 error")).toEqual(["error"]);
  });

  test("extracts values from key=value pairs", () => {
    expect(extractTokens("level=error")).toEqual(["error"]);
    expect(extractTokens("host=web-01 status=500")).toEqual(["web-01", "500"]);
  });

  test("excludes wildcard values (key=*)", () => {
    expect(extractTokens("level=*")).toEqual([]);
  });

  test("handles empty string", () => {
    expect(extractTokens("")).toEqual([]);
  });

  test("handles whitespace-only string", () => {
    expect(extractTokens("   ")).toEqual([]);
  });

  test("strips parentheses", () => {
    expect(extractTokens("(error OR warning)")).toEqual(["error", "warning"]);
  });

  test("handles multiple spaces between tokens", () => {
    expect(extractTokens("error   timeout")).toEqual(["error", "timeout"]);
  });

  test("handles mixed bare words and kv pairs", () => {
    expect(extractTokens("last=1h level=error timeout")).toEqual([
      "error",
      "timeout",
    ]);
  });
});
