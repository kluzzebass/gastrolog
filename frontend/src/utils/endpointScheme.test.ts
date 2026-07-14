import { describe, test, expect } from "bun:test";
import { endpointSchemeError } from "./endpointScheme";

describe("endpointSchemeError", () => {
  // Happy path — explicit schemes are accepted.
  test("accepts https:// endpoint", () => {
    expect(endpointSchemeError("https://minio.local:9000")).toBeNull();
  });

  test("accepts http:// endpoint (plaintext local/dev)", () => {
    expect(endpointSchemeError("http://localhost:9000")).toBeNull();
  });

  test("accepts empty value (requiredness is the field's concern)", () => {
    expect(endpointSchemeError("")).toBeNull();
  });

  // Unhappy path — bare values are rejected.
  test("rejects bare host", () => {
    expect(endpointSchemeError("minio.local")).toContain("no scheme");
  });

  test("rejects bare host:port", () => {
    expect(endpointSchemeError("localhost:9000")).toContain("no scheme");
  });

  test("rejects bare AWS hostname", () => {
    expect(endpointSchemeError("s3.amazonaws.com")).toContain("no scheme");
  });

  test("rejects non-http scheme", () => {
    expect(endpointSchemeError("ftp://localhost")).toContain("no scheme");
  });

  test("suggests both accepted forms for the entered value", () => {
    const err = endpointSchemeError("minio.local:9000");
    expect(err).toContain('"https://minio.local:9000"');
    expect(err).toContain('"http://minio.local:9000"');
  });

  // Edge cases.
  test("rejects whitespace-only value (backend sees it as non-empty)", () => {
    expect(endpointSchemeError("   ")).toContain("no scheme");
  });

  test("accepts scheme with surrounding whitespace", () => {
    expect(endpointSchemeError("  https://minio.local:9000  ")).toBeNull();
  });

  test("accepts uppercase scheme", () => {
    expect(endpointSchemeError("HTTPS://minio.local:9000")).toBeNull();
    expect(endpointSchemeError("HTTP://localhost:9000")).toBeNull();
  });

  test("rejects scheme-like value without separator", () => {
    expect(endpointSchemeError("https:minio.local")).toContain("no scheme");
  });
});
