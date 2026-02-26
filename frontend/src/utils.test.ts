import { describe, expect, test } from "bun:test";
import {
  extractKVPairs,
  relativeTime,
  sameRecord,
  formatChunkId,
} from "./utils";
import { Record, RecordRef } from "./api/gen/gastrolog/v1/query_pb";

describe("extractKVPairs", () => {
  test("simple key=value", () => {
    const pairs = extractKVPairs("level=error msg=hello");
    expect(pairs).toEqual([
      { key: "level", value: "error" },
      { key: "msg", value: "hello" },
    ]);
  });

  test("double-quoted value", () => {
    const pairs = extractKVPairs('msg="hello world"');
    expect(pairs).toEqual([{ key: "msg", value: "hello world" }]);
  });

  test("single-quoted value", () => {
    const pairs = extractKVPairs("msg='hello world'");
    expect(pairs).toEqual([{ key: "msg", value: "hello world" }]);
  });

  test("dotted key", () => {
    const pairs = extractKVPairs("http.status=200");
    expect(pairs).toEqual([{ key: "http.status", value: "200" }]);
  });

  test("underscore in key", () => {
    const pairs = extractKVPairs("user_id=42");
    expect(pairs).toEqual([{ key: "user_id", value: "42" }]);
  });

  test("empty string", () => {
    expect(extractKVPairs("")).toEqual([]);
  });

  test("no kv pairs", () => {
    expect(extractKVPairs("just some text")).toEqual([]);
  });

  test("deduplicates", () => {
    const pairs = extractKVPairs("level=error level=error");
    expect(pairs).toEqual([{ key: "level", value: "error" }]);
  });

  test("skips empty values", () => {
    // key= with nothing after should not match (value length 0)
    const pairs = extractKVPairs("key= next=ok");
    expect(pairs).toEqual([{ key: "next", value: "ok" }]);
  });

  test("lowercases keys and values", () => {
    const pairs = extractKVPairs("Level=ERROR");
    expect(pairs).toEqual([{ key: "level", value: "error" }]);
  });

  test("kv after delimiters", () => {
    const pairs = extractKVPairs("[level=info] (source=app)");
    expect(pairs).toEqual([
      { key: "level", value: "info" },
      { key: "source", value: "app" },
    ]);
  });

  test("skips keys longer than 64 chars", () => {
    const longKey = "a".repeat(65);
    const pairs = extractKVPairs(`${longKey}=value`);
    expect(pairs).toEqual([]);
  });

  test("skips values longer than 64 chars", () => {
    const longValue = "a".repeat(65);
    const pairs = extractKVPairs(`key=${longValue}`);
    expect(pairs).toEqual([]);
  });
});

describe("relativeTime", () => {
  test("seconds ago", () => {
    const date = new Date(Date.now() - 30 * 1000);
    expect(relativeTime(date)).toBe("30s ago");
  });

  test("minutes ago", () => {
    const date = new Date(Date.now() - 5 * 60 * 1000);
    expect(relativeTime(date)).toBe("5m ago");
  });

  test("hours ago", () => {
    const date = new Date(Date.now() - 3 * 60 * 60 * 1000);
    expect(relativeTime(date)).toBe("3h ago");
  });

  test("days ago", () => {
    const date = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000);
    expect(relativeTime(date)).toBe("2d ago");
  });

  test("in the future", () => {
    const date = new Date(Date.now() + 60 * 1000);
    expect(relativeTime(date)).toBe("in the future");
  });

  test("just now (0s ago)", () => {
    const date = new Date(Date.now());
    expect(relativeTime(date)).toBe("0s ago");
  });
});

describe("sameRecord", () => {
  test("both null", () => expect(sameRecord(null, null)).toBe(true));
  test("same reference", () => {
    const r = new Record({ ref: new RecordRef({ chunkId: "a", pos: 0n, vaultId: "s" }) });
    expect(sameRecord(r, r)).toBe(true);
  });
  test("a null", () => {
    const r = new Record({ ref: new RecordRef({ chunkId: "a", pos: 0n, vaultId: "s" }) });
    expect(sameRecord(null, r)).toBe(false);
  });
  test("b null", () => {
    const r = new Record({ ref: new RecordRef({ chunkId: "a", pos: 0n, vaultId: "s" }) });
    expect(sameRecord(r, null)).toBe(false);
  });
  test("same ref values", () => {
    const a = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    const b = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    expect(sameRecord(a, b)).toBe(true);
  });
  test("different chunk", () => {
    const a = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    const b = new Record({ ref: new RecordRef({ chunkId: "c2", pos: 5n, vaultId: "s1" }) });
    expect(sameRecord(a, b)).toBe(false);
  });
  test("different pos", () => {
    const a = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    const b = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 6n, vaultId: "s1" }) });
    expect(sameRecord(a, b)).toBe(false);
  });
  test("different vault", () => {
    const a = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    const b = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s2" }) });
    expect(sameRecord(a, b)).toBe(false);
  });
  test("missing ref on a", () => {
    const a = new Record({});
    const b = new Record({ ref: new RecordRef({ chunkId: "c1", pos: 5n, vaultId: "s1" }) });
    expect(sameRecord(a, b)).toBe(false);
  });
});

describe("formatChunkId", () => {
  test("non-empty returns as-is", () =>
    expect(formatChunkId("abc123")).toBe("abc123"));
  test("empty returns N/A", () => expect(formatChunkId("")).toBe("N/A"));
});
