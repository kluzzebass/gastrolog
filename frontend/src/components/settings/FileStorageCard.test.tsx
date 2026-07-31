import { describe, expect, test, mock } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { FileStorageCard } from "./FileStorageCard";
import { FileStorage } from "../../api/gen/gastrolog/v1/storage_pb";
import { encode } from "../../api/glid";

// Disk Free Warn/Floor are edited on the storage surface (FileStorageCard,
// rendered from StorageSettings), not on the vault. Pins the round-trip:
// the card reads FileStorage.diskFreeWarn/diskFreeFloor as its edit
// defaults, and a save carries the operator's edited values back out — with
// the placeholder-inherits-node-default convention ("10%"/"3%").

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

describe("FileStorageCard", () => {
  test("renders empty disk-free fields as empty (inherits node default)", () => {
    const fs = new FileStorage({
      id: testId(1),
      name: "nvme-fast",
      path: "storage/nvme-fast",
      storageClass: 1,
    });
    render(
      <FileStorageCard
        fs={fs}
        nodeName="node-1"
        dark={true}
        expanded={true}
        onToggle={() => {}}
        onSave={async () => {}}
        onDelete={async () => {}}
        saving={false}
      />,
    );
    const warnInput = document.querySelector('input[placeholder="10%"]') as HTMLInputElement | null;
    const floorInput = document.querySelector('input[placeholder="3%"]') as HTMLInputElement | null;
    expect(warnInput).toBeTruthy();
    expect(floorInput).toBeTruthy();
    expect(warnInput?.value).toBe("");
    expect(floorInput?.value).toBe("");
  });

  test("existing thresholds populate the edit defaults verbatim", () => {
    const fs = new FileStorage({
      id: testId(2),
      name: "hdd-archive",
      path: "storage/hdd-archive",
      storageClass: 3,
      diskFreeWarn: "10%",
      diskFreeFloor: "3GB",
    });
    const { getByDisplayValue } = render(
      <FileStorageCard
        fs={fs}
        nodeName="node-1"
        dark={true}
        expanded={true}
        onToggle={() => {}}
        onSave={async () => {}}
        onDelete={async () => {}}
        saving={false}
      />,
    );
    expect(getByDisplayValue("10%")).toBeTruthy();
    expect(getByDisplayValue("3GB")).toBeTruthy();
  });

  // Note: fireEvent.change on <input> doesn't reliably trigger React 19
  // controlled input handlers in happy-dom (see FormField.test.tsx and
  // VaultsSettings.test.tsx for the same caveat) — the example-value quick
  // fill buttons are plain click targets, so they're the reliable way to
  // dirty a text field in this suite.
  test("clicking a Disk Free Floor example dirties the card and Save carries it through", () => {
    const fs = new FileStorage({
      id: testId(3),
      name: "ssd-pool",
      path: "storage/ssd-pool",
      storageClass: 2,
    });
    const onSave = mock(async (_storageId: string, _edit: { diskFreeFloor: string; diskFreeWarn: string }) => {});
    const { getByText } = render(
      <FileStorageCard
        fs={fs}
        nodeName="node-1"
        dark={true}
        expanded={true}
        onToggle={() => {}}
        onSave={onSave}
        onDelete={async () => {}}
        saving={false}
      />,
    );

    // "3%" only appears among the Disk Free Floor examples (the Warn
    // field's examples are 10%/10GB/50GB) — unambiguous click target.
    fireEvent.click(getByText("3%"));

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(false);
    fireEvent.click(saveBtn);

    expect(onSave).toHaveBeenCalledTimes(1);
    const [storageId, edit] = onSave.mock.calls[0]!;
    expect(storageId).toBe(encode(fs.id));
    expect(edit.diskFreeFloor).toBe("3%");
    expect(edit.diskFreeWarn).toBe("");
  });

  // The inverse cross-link (storage inspector card -> Settings) exists
  // already; this pins the storage config card's own link back to its
  // inspector card, matching VaultSettingsCard's and IngestersSettings'
  // "Open in Inspector" cross-link exactly (same icon, same title, same
  // entities:<type>:<name> deep-link format the inspector parses).
  describe("Open in Inspector cross-link", () => {
    const fs = new FileStorage({
      id: testId(4),
      name: "nvme-fast",
      path: "storage/nvme-fast",
      storageClass: 1,
    });

    test("omitted when onOpenInspector is not provided", () => {
      const { queryByTitle } = render(
        <FileStorageCard
          fs={fs}
          nodeName="node-1"
          dark={true}
          expanded={true}
          onToggle={() => {}}
          onSave={async () => {}}
          onDelete={async () => {}}
          saving={false}
        />,
      );
      expect(queryByTitle("Open in Inspector")).toBeNull();
    });

    test("navigates to the storage's entity card, named the same way ID fallback does elsewhere", () => {
      const onOpenInspector = mock((_param: string) => {});
      const { getByTitle } = render(
        <FileStorageCard
          fs={fs}
          nodeName="node-1"
          dark={true}
          expanded={true}
          onToggle={() => {}}
          onSave={async () => {}}
          onDelete={async () => {}}
          saving={false}
          onOpenInspector={onOpenInspector}
        />,
      );

      fireEvent.click(getByTitle("Open in Inspector"));

      expect(onOpenInspector).toHaveBeenCalledTimes(1);
      expect(onOpenInspector).toHaveBeenCalledWith("entities:storages:nvme-fast");
    });

    test("falls back to the encoded id when the storage has no name", () => {
      const unnamed = new FileStorage({ id: testId(5), path: "storage/unnamed", storageClass: 1 });
      const onOpenInspector = mock((_param: string) => {});
      const { getByTitle } = render(
        <FileStorageCard
          fs={unnamed}
          nodeName="node-1"
          dark={true}
          expanded={true}
          onToggle={() => {}}
          onSave={async () => {}}
          onDelete={async () => {}}
          saving={false}
          onOpenInspector={onOpenInspector}
        />,
      );

      fireEvent.click(getByTitle("Open in Inspector"));

      expect(onOpenInspector).toHaveBeenCalledWith(`entities:storages:${encode(unnamed.id)}`);
    });
  });
});
