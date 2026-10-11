import { describe, test, expect } from "bun:test";
import { appendIdleNote, collectIdleNote, sealIdleNote } from "./throughputNotes";

// Pins the STATUS column's wording per state, so the help table in
// inspector-vaults.md and the panel cannot drift apart silently.

describe("appendIdleNote", () => {
  test("a quiet origin says no ingest", () => {
    expect(appendIdleNote()).toEqual({ text: "no ingest", warn: false });
  });
});

describe("collectIdleNote", () => {
  test("a node without staging counts is idle", () => {
    expect(collectIdleNote(undefined, 10)).toEqual({ text: "idle", warn: false });
  });

  test("nothing published means nothing to collect, not a stall", () => {
    expect(collectIdleNote(0, 0)).toEqual({ text: "nothing published", warn: false });
  });

  test("head holding every published segment is caught up", () => {
    expect(collectIdleNote(212, 212)).toEqual({ text: "caught up", warn: false });
    expect(collectIdleNote(300, 212)).toEqual({ text: "caught up", warn: false });
  });

  test("head short of the registry is behind by the difference, as a warning", () => {
    expect(collectIdleNote(200, 212)).toEqual({ text: "behind 12 segments", warn: true });
    expect(collectIdleNote(0, 1500)).toEqual({ text: "behind 1,500 segments", warn: true });
  });
});

describe("sealIdleNote", () => {
  test("eligible segments the planner has not taken are a warning, counted", () => {
    expect(sealIdleNote(1, false)).toEqual({ text: "1 segments eligible to chunk", warn: true });
    expect(sealIdleNote(1200, true)).toEqual({ text: "1,200 segments eligible to chunk", warn: true });
  });

  test("a sealed manifest with nothing else eligible says what the homes owe", () => {
    expect(sealIdleNote(0, true)).toEqual({ text: "sealed manifest awaiting build", warn: true });
  });

  test("nothing eligible and nothing pending is up to date", () => {
    expect(sealIdleNote(0, false)).toEqual({ text: "up to date", warn: false });
  });
});
