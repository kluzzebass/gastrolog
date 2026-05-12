import { describe, test, expect } from "bun:test";
import { idFromBytes, idToBytes, asEntityID, eqID, isEmptyID, EMPTY_ID } from "./id";

describe("idFromBytes", () => {
  test("encodes 16 raw bytes to a 26-char string", () => {
    const bytes = new Uint8Array(16);
    for (let i = 0; i < 16; i++) bytes[i] = i;
    const id = idFromBytes(bytes);
    expect(id).toHaveLength(26);
  });

  test("returns EMPTY_ID for missing input", () => {
    // eslint-disable-next-line unicorn/no-useless-undefined -- exercising the explicit undefined branch
    expect(idFromBytes(undefined)).toBe(EMPTY_ID);
  });

  test("returns EMPTY_ID for empty Uint8Array", () => {
    expect(idFromBytes(new Uint8Array(0))).toBe(EMPTY_ID);
  });

  test("returns EMPTY_ID for all-zero 16-byte input", () => {
    expect(idFromBytes(new Uint8Array(16))).toBe(EMPTY_ID);
  });
});

describe("round-trip", () => {
  test("idFromBytes → idToBytes recovers the original bytes", () => {
    const original = new Uint8Array(16);
    for (let i = 0; i < 16; i++) original[i] = (i * 17) & 0xFF;
    const id = idFromBytes(original);
    const recovered = idToBytes(id);
    expect(recovered).toEqual(original);
  });

  test("EMPTY_ID round-trips through idToBytes to 16 zero bytes", () => {
    const bytes = idToBytes(EMPTY_ID);
    expect(bytes).toHaveLength(16);
    expect(Array.from(bytes).every((b) => b === 0)).toBe(true);
  });
});

describe("eqID", () => {
  test("returns true for identical IDs", () => {
    const bytes = new Uint8Array(16);
    bytes[0] = 42;
    const a = idFromBytes(bytes);
    const b = idFromBytes(bytes);
    expect(eqID(a, b)).toBe(true);
  });

  test("returns false for distinct IDs", () => {
    const a = idFromBytes(new Uint8Array([1, ...new Array<number>(15).fill(0)]));
    const b = idFromBytes(new Uint8Array([2, ...new Array<number>(15).fill(0)]));
    expect(eqID(a, b)).toBe(false);
  });

  test("EMPTY_ID equals EMPTY_ID", () => {
    expect(eqID(EMPTY_ID, EMPTY_ID)).toBe(true);
  });
});

describe("isEmptyID", () => {
  test("true for EMPTY_ID", () => {
    expect(isEmptyID(EMPTY_ID)).toBe(true);
  });

  test("false for a real ID", () => {
    const bytes = new Uint8Array(16);
    bytes[0] = 1;
    expect(isEmptyID(idFromBytes(bytes))).toBe(false);
  });
});

describe("asEntityID", () => {
  test("tags a plain string", () => {
    const id = asEntityID("06f1cjsv7tqth15mftnml5cs4o");
    expect(idToBytes(id)).toHaveLength(16);
  });
});
