import { describe, test, expect } from "bun:test";
import { storageVerdictLabel, thresholdLabel } from "./StorageCard";

// storageVerdictLabel drives the header badge grammar: nothing when
// healthy, "warn" below the warn threshold, "protected" below the floor
// (which supersedes warn — a storage can't be simultaneously warn-only and
// protected). Both inputs are the server-computed, hysteresis-aware
// booleans the admission gate itself consults (gastrolog-3cobq4);
// storageVerdictLabel does no derivation, only a two-flag → label mapping,
// mirroring the CLI's storageVerdict for UI/CLI parity.
describe("storageVerdictLabel", () => {
  test("healthy storage yields no label (badge hidden)", () => {
    expect(storageVerdictLabel({ warnVerdict: false, protectVerdict: false })).toBeNull();
  });

  test("warn-only storage yields \"warn\"", () => {
    expect(storageVerdictLabel({ warnVerdict: true, protectVerdict: false })).toBe("warn");
  });

  test("protected storage yields \"protected\"", () => {
    expect(storageVerdictLabel({ warnVerdict: false, protectVerdict: true })).toBe("protected");
  });

  test("protected supersedes warn when both are set", () => {
    expect(storageVerdictLabel({ warnVerdict: true, protectVerdict: true })).toBe("protected");
  });
});

// thresholdLabel renders the EFFECTIVE (resolved) byte value with its
// provenance — placeholder-style, matching the Settings form's "effective
// value is what matters" convention and the CLI's identical helper. expr
// is always the effective, non-empty expression from the wire.
describe("thresholdLabel", () => {
  test("defaulted threshold labels the source as \"default\", not \"inherited\" — there is no configurable node-level override to inherit from (gastrolog-2mrfdw)", () => {
    expect(thresholdLabel("10%", true, 3_000_000_000n)).toBe("2.8 GiB (10%, default)");
  });

  test("explicit percentage threshold echoes the configured expression verbatim", () => {
    expect(thresholdLabel("10%", false, 5_368_709_120n)).toBe("5.0 GiB (10%)");
  });

  test("explicit absolute-size expression omits the redundant expression — the bytes alone are the complete answer", () => {
    expect(thresholdLabel("3GB", false, 3_000_000_000n)).toBe("2.8 GiB");
  });

  test("zero resolved bytes (no sample yet) still renders honestly, not blank", () => {
    expect(thresholdLabel("10%", true, 0n)).toBe("0 B (10%, default)");
  });
});
