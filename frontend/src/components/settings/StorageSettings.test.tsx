import { describe, test, expect, beforeEach } from "bun:test";
import { fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { StorageSettings } from "./StorageSettings";
import { GetSystemResponse } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleConfig = new GetSystemResponse({
  cloudServices: [{ id: testId(1), name: "archive", provider: "s3", bucket: "chunks" }],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
  nodeStorageConfigs: [{ nodeId: testId(40), fileStorages: [] }],
});

beforeEach(() => {
  m(mocks.systemClient, "generateName").mockClear();
});

function renderStorage(generatedName = "fresh-service") {
  m(mocks.systemClient, "generateName").mockResolvedValue({ name: generatedName });
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], sampleConfig);
  qc.setQueryData(["settings"], { nodeId: testId(40), nodeName: "node-1" });
  return renderWritable(<StorageSettings dark />, { wrapper: settingsWrapper(qc) });
}

describe("StorageSettings cloud storage create gates", () => {
  // With no generated name the form falls back to "cloud-service", which a
  // service of that name takes.
  test("a name conflict disables Create and says which name is taken", () => {
    m(mocks.systemClient, "generateName").mockRejectedValue(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({
      cloudServices: [{ id: testId(1), name: "cloud-service", provider: "s3", bucket: "chunks" }],
      nodeConfigs: [{ id: testId(40), name: "node-1" }],
    }));
    qc.setQueryData(["settings"], { nodeId: testId(40), nodeName: "node-1" });
    const { getByText } = renderWritable(<StorageSettings dark />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(getByText("Add Cloud Storage"));

    expect(getByText('A cloud storage named "cloud-service" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);
  });

  test("the missing bucket or container is named for the chosen provider", async () => {
    const { getByText, findAllByText, getByLabelText, queryAllByText } = renderStorage();
    fireEvent.click(getByText("Add Cloud Storage"));

    // Beside Create and beside Test Connection alike.
    expect(await findAllByText("Bucket is required")).toHaveLength(2);
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    fireEvent.change(getByLabelText("Provider"), { target: { value: "azure" } });
    await waitFor(() => {
      expect(queryAllByText("Bucket is required")).toHaveLength(0);
      expect(getByText("Container is required")).toBeTruthy();
    });
  });

  test("Test Connection carries the same reason while it is disabled", async () => {
    const { getByText, findByText } = renderStorage();
    fireEvent.click(getByText("Add Cloud Storage"));

    await findByText("Test Connection");
    expect(getByText("Test Connection").closest("button")!.disabled).toBe(true);
    // One beside Create, one beside Test Connection.
    const reasons = Array.from(document.querySelectorAll("span")).filter((s) => s.textContent === "Bucket is required");
    expect(reasons).toHaveLength(2);
  });
});

describe("StorageSettings file storage create gate", () => {
  test("a missing storage class disables Create with the reason, until one is set", async () => {
    const { getByText, getByLabelText, queryByText } = renderStorage();
    fireEvent.click(getByText("Add File Storage"));

    expect(getByText("Storage class is required")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    fireEvent.click(getByLabelText("Increase"));
    await waitFor(() => {
      expect(queryByText("Storage class is required")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });
});
