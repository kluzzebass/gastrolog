import { describe, expect, test } from "bun:test";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

installMockClients();

import { CloudServiceCard } from "./CloudServiceCard";
import { cloudServiceSaveRequest } from "./cloudServiceEdit";
import { CloudService } from "../../api/gen/gastrolog/v1/storage_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

function renderCard(endpoint: string) {
  const service = new CloudService({ id: testId(1), name: "archive", provider: "s3", bucket: "chunks", endpoint });
  const qc = createTestQueryClient();
  return renderWritable(
    <CloudServiceCard service={service} dark expanded onToggle={() => {}} />,
    { wrapper: settingsWrapper(qc) },
  );
}

describe("CloudServiceCard endpoint gate", () => {
  test("a saved scheme-less endpoint disables Test and Save, each saying why", () => {
    const { getByText, getAllByText } = renderCard("minio.local:9000");

    expect(getByText("Test Connection").closest("button")!.disabled).toBe(true);
    expect(getByText("Save").closest("button")!.disabled).toBe(true);
    expect(getAllByText("Endpoint needs an http:// or https:// scheme")).toHaveLength(2);
  });

  test("a well-formed endpoint carries no reason", () => {
    const { getByText, queryByText } = renderCard("https://minio.local:9000");

    expect(getByText("Test Connection").closest("button")!.disabled).toBe(false);
    expect(queryByText(/needs an http/)).toBeNull();
  });
});

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
    clearCredentials: false,
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

  test("removing the stored credentials is an explicit instruction, not an empty field", () => {
    expect(cloudServiceSaveRequest("svc-1", edit()).clearCredentials).toBe(false);
    expect(
      cloudServiceSaveRequest("svc-1", edit({ clearCredentials: true })).clearCredentials,
    ).toBe(true);
  });
});
