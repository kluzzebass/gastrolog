import { describe, expect, test } from "bun:test";
import {
  stripTimeRange,
  stripAllDirectives,
  stripStore,
  stripChunk,
  stripPos,
  stripSeverity,
  buildTimeTokens,
  injectTimeRange,
  injectStore,
  buildSeverityExpr,
  extractDirectives,
  replaceExpression,
  appendOrExpression,
} from "./queryHelpers";

describe("stripTimeRange", () => {
  test("strips last=", () =>
    expect(stripTimeRange("last=5m foo")).toBe("foo"));
  test("strips start=", () =>
    expect(stripTimeRange("start=2024-01-01 foo")).toBe("foo"));
  test("strips end=", () =>
    expect(stripTimeRange("end=2024-01-01 foo")).toBe("foo"));
  test("strips reverse=", () =>
    expect(stripTimeRange("reverse=true foo")).toBe("foo"));
  test("strips all time tokens", () =>
    expect(
      stripTimeRange("last=5m start=x end=y reverse=true foo"),
    ).toBe("foo"));
  test("preserves non-time tokens", () =>
    expect(stripTimeRange("level=error foo")).toBe("level=error foo"));
  test("empty string", () => expect(stripTimeRange("")).toBe(""));
  test("only time tokens", () =>
    expect(stripTimeRange("last=5m reverse=false")).toBe(""));
  test("collapses whitespace", () =>
    expect(stripTimeRange("last=5m   foo   bar")).toBe("foo bar"));
});

describe("stripStore", () => {
  test("strips store=", () =>
    expect(stripStore("store=main foo")).toBe("foo"));
  test("preserves other tokens", () =>
    expect(stripStore("level=error foo")).toBe("level=error foo"));
  test("empty string", () => expect(stripStore("")).toBe(""));
  test("only store token", () => expect(stripStore("store=main")).toBe(""));
});

describe("stripChunk", () => {
  test("strips chunk=", () =>
    expect(stripChunk("chunk=abc123 foo")).toBe("foo"));
  test("empty string", () => expect(stripChunk("")).toBe(""));
});

describe("stripPos", () => {
  test("strips pos=", () => expect(stripPos("pos=42 foo")).toBe("foo"));
  test("empty string", () => expect(stripPos("")).toBe(""));
});

describe("stripSeverity", () => {
  test("strips single level=", () =>
    expect(stripSeverity("level=error foo")).toBe("foo"));
  test("strips OR group", () =>
    expect(stripSeverity("(level=error OR level=warn) foo")).toBe("foo"));
  test("not level=* kept (trailing \\b does not match after *)", () =>
    expect(stripSeverity("not level=* foo")).toBe("not level=* foo"));
  test("preserves non-severity tokens", () =>
    expect(stripSeverity("store=main foo")).toBe("store=main foo"));
  test("empty string", () => expect(stripSeverity("")).toBe(""));
  test("strips all known levels", () => {
    for (const level of ["error", "warn", "info", "debug", "trace"]) {
      expect(stripSeverity(`level=${level} foo`)).toBe("foo");
    }
  });
});

describe("stripAllDirectives", () => {
  test("strips all directive types", () =>
    expect(stripAllDirectives("last=5m start=x end=y reverse=true store=main limit=100 chunk=abc pos=42 foo")).toBe("foo"));
  test("only directives returns empty", () =>
    expect(stripAllDirectives("last=15m reverse=true")).toBe(""));
  test("preserves search expression", () =>
    expect(stripAllDirectives("last=5m reverse=true level=error foo")).toBe("level=error foo"));
  test("empty string", () => expect(stripAllDirectives("")).toBe(""));
});

describe("buildTimeTokens", () => {
  test("All range", () =>
    expect(buildTimeTokens("All", false)).toBe("reverse=false"));
  test("All range reversed", () =>
    expect(buildTimeTokens("All", true)).toBe("reverse=true"));
  test("known range", () =>
    expect(buildTimeTokens("5m", false)).toBe("last=5m reverse=false"));
  test("known range reversed", () =>
    expect(buildTimeTokens("5m", true)).toBe("last=5m reverse=true"));
  test("unknown range falls through", () =>
    expect(buildTimeTokens("custom", false)).toBe("reverse=false"));
});

describe("injectTimeRange", () => {
  test("into empty query", () =>
    expect(injectTimeRange("", "5m", false)).toBe("last=5m reverse=false"));
  test("into existing query", () =>
    expect(injectTimeRange("foo", "5m", false)).toBe(
      "last=5m reverse=false foo",
    ));
  test("replaces existing time range", () =>
    expect(injectTimeRange("last=1h reverse=true foo", "5m", false)).toBe(
      "last=5m reverse=false foo",
    ));
  test("All range into query", () =>
    expect(injectTimeRange("foo", "All", true)).toBe("reverse=true foo"));
});

describe("injectStore", () => {
  test("into empty query", () =>
    expect(injectStore("", "main")).toBe("store=main"));
  test("into existing query", () =>
    expect(injectStore("foo", "main")).toBe("store=main foo"));
  test("replaces existing store", () =>
    expect(injectStore("store=old foo", "main")).toBe("store=main foo"));
  test("all store strips", () =>
    expect(injectStore("store=old foo", "all")).toBe("foo"));
  test("all store on empty", () => expect(injectStore("", "all")).toBe(""));
});

describe("extractDirectives", () => {
  test("extracts all directive types", () =>
    expect(extractDirectives("last=5m reverse=true store=main foo")).toBe(
      "last=5m reverse=true store=main",
    ));
  test("no directives", () => expect(extractDirectives("foo bar")).toBe(""));
  test("empty string", () => expect(extractDirectives("")).toBe(""));
});

describe("replaceExpression", () => {
  test("replaces expression preserving directives", () =>
    expect(replaceExpression("last=5m reverse=true foo", "bar")).toBe(
      "last=5m reverse=true bar",
    ));
  test("no directives — just the value", () =>
    expect(replaceExpression("foo", "bar")).toBe("bar"));
  test("empty query — just the value", () =>
    expect(replaceExpression("", "bar")).toBe("bar"));
  test("only directives", () =>
    expect(replaceExpression("last=5m reverse=true", "bar")).toBe(
      "last=5m reverse=true bar",
    ));
});

describe("appendOrExpression", () => {
  test("empty expression — just the value", () =>
    expect(appendOrExpression("last=5m reverse=true", "192.168.1.1")).toBe(
      "last=5m reverse=true 192.168.1.1",
    ));
  test("single term — wraps in parens with OR", () =>
    expect(
      appendOrExpression("last=5m reverse=true 10.0.0.1", "192.168.1.1"),
    ).toBe("last=5m reverse=true (10.0.0.1 OR 192.168.1.1)"));
  test("existing group — appends inside parens", () =>
    expect(
      appendOrExpression(
        "last=5m reverse=true (10.0.0.1 OR 10.0.0.2)",
        "192.168.1.1",
      ),
    ).toBe("last=5m reverse=true (10.0.0.1 OR 10.0.0.2 OR 192.168.1.1)"));
  test("no directives — single term", () =>
    expect(appendOrExpression("foo", "bar")).toBe("(foo OR bar)"));
  test("no directives — existing group", () =>
    expect(appendOrExpression("(foo OR bar)", "baz")).toBe(
      "(foo OR bar OR baz)",
    ));
  test("completely empty query", () =>
    expect(appendOrExpression("", "foo")).toBe("foo"));
});

describe("buildSeverityExpr", () => {
  test("empty array", () => expect(buildSeverityExpr([])).toBe(""));
  test("single severity", () =>
    expect(buildSeverityExpr(["error"])).toBe("level=error"));
  test("two severities", () =>
    expect(buildSeverityExpr(["error", "warn"])).toBe(
      "(level=error OR level=warn)",
    ));
  test("three severities", () =>
    expect(buildSeverityExpr(["error", "warn", "info"])).toBe(
      "(level=error OR level=warn OR level=info)",
    ));
});
