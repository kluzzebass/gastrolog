import { describe, expect, test } from "bun:test";
import { cloudServiceSaveRequest } from "./CloudServiceCard";

// The card never receives a service's credentials, so its edit state starts
// them empty. What matters on save is that an untouched credential goes out
// empty — the server reads that as "keep the stored one" — and that a typed
// credential goes out alone, without dragging a blank over its siblings.

function edit(patch: Partial<Parameters<typeof cloudServiceSaveRequest>[1]> = {}) {
  return {
    name: "archive",
    provider: "s3",
    bucket: "chunks",
    region: "us-east-1",
    endpoint: "",
    accessKey: "",
    secretKey: "",
    credentialsConfigured: true,
    container: "",
    connectionString: "",
    credentialsJson: "",
    archivalMode: "none",
    transitions: [],
    restoreSpeed: "",
    restoreDays: 7,
    suspectGraceDays: 7,
    reconcileSchedule: "0 3 * * *",
    ...patch,
  };
}

describe("cloudServiceSaveRequest", () => {
  test("an untouched credential is sent empty, which preserves the stored one", () => {
    const req = cloudServiceSaveRequest("svc-1", edit({ region: "eu-west-1" }));
    expect(req.accessKey).toBe("");
    expect(req.secretKey).toBe("");
    expect(req.connectionString).toBe("");
    expect(req.credentialsJson).toBe("");
    expect(req.region).toBe("eu-west-1");
  });

  test("only the credential the operator typed carries a value", () => {
    const req = cloudServiceSaveRequest("svc-1", edit({ secretKey: "rotated" }));
    expect(req.secretKey).toBe("rotated");
    expect(req.accessKey).toBe("");
  });

  test("credentialsConfigured is a read-only signal and is never sent back", () => {
    const req = cloudServiceSaveRequest("svc-1", edit());
    expect(req).not.toHaveProperty("credentialsConfigured");
  });
});
