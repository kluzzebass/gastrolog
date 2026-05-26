import { describe, test, expect } from "bun:test";
import { normalizeWriteModel, usesSequencedWriteModel } from "./writeModel";

describe("writeModel utils", () => {
  test("normalizeWriteModel defaults empty to chunk_append", () => {
    expect(normalizeWriteModel("")).toBe("chunk_append");
    expect(normalizeWriteModel(undefined)).toBe("chunk_append");
  });

  test("normalizeWriteModel accepts sequenced", () => {
    expect(normalizeWriteModel("sequenced")).toBe("sequenced");
  });

  test("normalizeWriteModel rejects unknown values", () => {
    expect(normalizeWriteModel("v2")).toBe("chunk_append");
  });

  test("usesSequencedWriteModel", () => {
    expect(usesSequencedWriteModel("sequenced")).toBe(true);
    expect(usesSequencedWriteModel("chunk_append")).toBe(false);
    expect(usesSequencedWriteModel("")).toBe(false);
  });
});
