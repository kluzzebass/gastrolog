import { describe, test, expect, mock } from "bun:test";
import { fireEvent, render } from "@testing-library/react";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";
import { encode } from "../../api/glid";

installMockClients();

import { renderWritable } from "../../testing/renderWritable";
import { FormField, NumberInput, SelectInput, SpinnerInput, TextArea, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { TlsClientFields, TlsListenerFields, CERTIFICATES_HELP_REF } from "./ingester-params/TlsFields";
import { DockerForm } from "./ingester-params/DockerForm";
import { NodeMultiSelect, NODE_ASSIGNMENT_HELP_REF } from "./NodeMultiSelect";
import { IngesterCommonFields } from "./IngesterCommonFields";
import { singletonLabel } from "./ingesterLabels";
import { ColumnPickers } from "./lookup/ColumnPickers";
import { csvColumnsUnavailable, mmdbFileIneligibility, tableColumnsUnavailable } from "./lookup/types";
import { FileDropZone } from "./FileDropZone";
import { MaxMindCard } from "./lookup/MaxMindCard";
import { CloudServiceFields, type CloudServiceFieldValues } from "./CloudServiceFields";
import { providerOptions, restoreDaysReason, restoreSpeedReason } from "./cloudProviders";
import { destinationState } from "./routeDisplay";
import { joinIneligibility } from "./joinEligibility";
import { SettingsDialog } from "./SettingsDialog";
import { VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import type { ManagedFileInfo } from "../../api/gen/gastrolog/v1/system_pb";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

/** The note rendered inside the FormField that owns this label or input, if any. */
function fieldNote(el: HTMLElement): Element | null {
  return el.closest(".flex-col")!.querySelector('[role="note"]');
}

function file(n: number, name: string): ManagedFileInfo {
  return { id: testId(n), name, size: 0n, sha256: "", uploadedAt: "" } as unknown as ManagedFileInfo;
}

function values(provider: string): CloudServiceFieldValues {
  return {
    provider, bucket: "b", region: "", endpoint: "", accessKey: "", secretKey: "", credentialsConfigured: false,
    clearCredentials: false, container: "c", connectionString: "", credentialsJson: "", archivalMode: "active",
    transitions: [], restoreSpeed: "", restoreDays: 7, suspectGraceDays: 7, reconcileSchedule: "",
  };
}

/** Render under a help provider whose calls are recorded. */
function withHelp(ui: React.ReactElement, qc = createTestQueryClient()) {
  const openHelp = mock((_helpRef?: string) => {});
  const result = renderWritable(<HelpProvider onOpen={openHelp}>{ui}</HelpProvider>, {
    wrapper: settingsWrapper(qc),
  });
  return { ...result, openHelp };
}

// ── The primitive: a gated FormField disables everything inside it ────

describe("FormField disabledReason", () => {
  test("every input kind inside a gated field is disabled and the reason is shown once", () => {
    const { container, getByRole } = renderWritable(
      <FormField label="Gated" disabledReason="Requires the thing." dark>
        <TextInput value="" onChange={() => {}} dark />
        <SelectInput value="" onChange={() => {}} options={[{ value: "", label: "x" }]} dark />
        <NumberInput value="" onChange={() => {}} dark />
        <SpinnerInput value="1" onChange={() => {}} dark />
        <TextArea value="" onChange={() => {}} dark />
        <Checkbox checked={false} onChange={() => {}} dark />
      </FormField>,
    );
    const controls = [...container.querySelectorAll("input, select, textarea, button")] as HTMLInputElement[];
    expect(controls.length).toBeGreaterThan(5);
    expect(controls.every((el) => el.disabled)).toBe(true);
    expect(container.querySelector('[role="checkbox"]')?.getAttribute("aria-disabled")).toBe("true");
    expect(getByRole("note").textContent).toBe("Requires the thing.");
  });

  test("a gated checkbox ignores clicks", () => {
    const onChange = mock(() => {});
    const { container } = renderWritable(
      <FormField label="Gated" disabledReason="Requires the thing." dark>
        <Checkbox checked={false} onChange={onChange} dark />
      </FormField>,
    );
    fireEvent.click(container.querySelector('[role="checkbox"]')!);
    expect(onChange).not.toHaveBeenCalled();
  });

  test("without a reason the same inputs are enabled and no note renders", () => {
    const { container, queryByRole } = renderWritable(
      <FormField label="Open" dark>
        <TextInput value="" onChange={() => {}} dark />
        <Checkbox checked={false} onChange={() => {}} dark />
      </FormField>,
    );
    expect((container.querySelector("input") as HTMLInputElement).disabled).toBe(false);
    expect(container.querySelector('[role="checkbox"]')?.getAttribute("aria-disabled")).toBeNull();
    expect(queryByRole("note")).toBeNull();
  });
});

// ── Ingester certificate pickers ──────────────────────────────────────

describe("ingester TLS certificate pickers", () => {
  const tlsOn = { tls: "true" };

  test("with no certificate stored every picker says so and links to adding one", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    const { getByText, getAllByRole, getAllByText, openHelp } = withHelp(
      <TlsListenerFields params={tlsOn} onChange={() => {}} dark />,
      qc,
    );
    const notes = getAllByRole("note").map((n) => n.textContent);
    expect(notes.filter((t) => t.includes("No certificates stored — add one in the Certificates tab.")).length).toBe(2);
    fireEvent.click(getAllByText("Eligibility rules")[0]!);
    expect(openHelp).toHaveBeenCalledWith(CERTIFICATES_HELP_REF);
    expect(getByText("Certificate")).toBeTruthy();
  });

  test("the client-side picker and the Docker picker have the same empty state", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    const client = withHelp(<TlsClientFields params={tlsOn} onChange={() => {}} dark />, qc);
    expect(client.getAllByRole("note").length).toBe(2);
    client.unmount();

    const docker = withHelp(<DockerForm params={{}} onChange={() => {}} dark defaults={{}} />, qc);
    expect(docker.getAllByRole("note").length).toBe(2);
    fireEvent.click(docker.getAllByText("Eligibility rules")[1]!);
    expect(docker.openHelp).toHaveBeenCalledWith(CERTIFICATES_HELP_REF);
  });

  test("stored certificates list by name and the Docker picker carries the ID as value", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [{ id: testId(1), name: "edge" }] });
    const listener = withHelp(<TlsListenerFields params={tlsOn} onChange={() => {}} dark />, qc);
    expect(listener.queryByRole("note")).not.toBeNull(); // only the CN gate note remains
    const certOption = [...listener.container.querySelectorAll("option")].find((o) => o.textContent === "edge")!;
    expect(certOption.value).toBe("edge");
    listener.unmount();

    const docker = withHelp(<DockerForm params={{}} onChange={() => {}} dark defaults={{}} />, qc);
    const dockerOption = [...docker.container.querySelectorAll("option")].find((o) => o.textContent === "edge")!;
    expect(dockerOption.value).toBe(encode(testId(1)));
  });

  test("Allowed Client CN is on screen but disabled until a client CA is chosen", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [{ id: testId(1), name: "ca" }] });
    const without = withHelp(<TlsListenerFields params={tlsOn} onChange={() => {}} dark />, qc);
    const label = without.getByText("Allowed Client CN");
    expect(fieldNote(label)?.textContent).toContain("Requires a client CA certificate");
    const input = without.container.querySelector(`#${CSS.escape((label as HTMLLabelElement).htmlFor)}`) as HTMLInputElement;
    expect(input.disabled).toBe(true);
    without.unmount();

    const withCa = withHelp(<TlsListenerFields params={{ ...tlsOn, tls_ca: "ca" }} onChange={() => {}} dark />, qc);
    const label2 = withCa.getByText("Allowed Client CN");
    expect(fieldNote(label2)).toBeNull();
    const input2 = withCa.container.querySelector(`#${CSS.escape((label2 as HTMLLabelElement).htmlFor)}`) as HTMLInputElement;
    expect(input2.disabled).toBe(false);
  });
});

// ── Node assignment ───────────────────────────────────────────────────

describe("NodeMultiSelect", () => {
  const cluster = {
    clusterEnabled: true,
    nodes: [
      { id: testId(1), name: "alpha" },
      { id: testId(2), name: "bravo" },
    ],
  };

  test("All nodes says it covers later joiners", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["clusterStatus"], cluster);
    const { getByText } = withHelp(
      <NodeMultiSelect value={[]} allNodes onValueChange={() => {}} onAllNodesChange={() => {}} dark />,
      qc,
    );
    expect(getByText("All nodes — including nodes that join later")).toBeTruthy();
  });

  test("nothing selected is named as every node and links to the rule", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["clusterStatus"], cluster);
    const { getByRole, getByText, openHelp } = withHelp(
      <NodeMultiSelect value={[]} allNodes={false} onValueChange={() => {}} onAllNodesChange={() => {}} dark />,
      qc,
    );
    expect(getByRole("note").textContent).toContain("No node selected — the ingester runs on every node");
    fireEvent.click(getByText("Node assignment"));
    expect(openHelp).toHaveBeenCalledWith(NODE_ASSIGNMENT_HELP_REF);
  });

  test("a ticked node or All nodes removes the note", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["clusterStatus"], cluster);
    const ticked = withHelp(
      <NodeMultiSelect value={[encode(testId(1))]} allNodes={false} onValueChange={() => {}} onAllNodesChange={() => {}} dark />,
      qc,
    );
    expect(ticked.queryByRole("note")).toBeNull();
    ticked.unmount();
    const all = withHelp(
      <NodeMultiSelect value={[]} allNodes onValueChange={() => {}} onAllNodesChange={() => {}} dark />,
      qc,
    );
    expect(all.queryByRole("note")).toBeNull();
  });
});

describe("singleton checkbox", () => {
  function renderFields(singletonSupported: boolean) {
    const qc = createTestQueryClient();
    qc.setQueryData(["clusterStatus"], { clusterEnabled: false, nodes: [] });
    const onSingletonChange = mock((_v: boolean) => {});
    const result = withHelp(
      <IngesterCommonFields
        name="x"
        onNameChange={() => {}}
        enabled
        onEnabledChange={() => {}}
        nodeIds={[]}
        onNodeIdsChange={() => {}}
        allNodes
        onAllNodesChange={() => {}}
        singleton={false}
        onSingletonChange={onSingletonChange}
        singletonSupported={singletonSupported}
        dark
      />,
      qc,
    );
    return { ...result, onSingletonChange };
  }

  test("an unsupported type keeps the checkbox on screen, disabled, with the reason in its label", () => {
    const { getByText, container, onSingletonChange } = renderFields(false);
    const label = getByText(singletonLabel(false));
    expect(label.textContent).toContain("not available for this type");
    const box = label.closest('[role="checkbox"]')!;
    expect(box.getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(box);
    expect(onSingletonChange).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Singleton (run on one node with automatic failover)");
  });

  test("a supported type offers the plain checkbox", () => {
    const { getByText, onSingletonChange } = renderFields(true);
    const box = getByText(singletonLabel(true)).closest('[role="checkbox"]')!;
    expect(box.getAttribute("aria-disabled")).toBeNull();
    fireEvent.click(box);
    expect(onSingletonChange).toHaveBeenCalledWith(true);
  });
});

// ── Lookup column pickers ─────────────────────────────────────────────

describe("ColumnPickers", () => {
  function renderPickers(props: Partial<Parameters<typeof ColumnPickers>[0]>) {
    const onValueColumnsChange = mock((_v: string[]) => {});
    const result = withHelp(
      <ColumnPickers
        columns={["ip", "host", "env"]}
        keyColumn=""
        valueColumns={[]}
        onKeyColumnChange={() => {}}
        onValueColumnsChange={onValueColumnsChange}
        helpRef="lookups-settings#csv-file-lookups"
        dark
        {...props}
      />,
    );
    return { ...result, onValueColumnsChange };
  }

  test("an empty value list shows as All, with every column ticked and locked", () => {
    const { getByText, container } = renderPickers({});
    const all = getByText(/All non-key columns/).closest('[role="checkbox"]')!;
    expect(all.getAttribute("aria-checked")).toBe("true");
    const cols = [...container.querySelectorAll('[role="checkbox"]')].slice(1);
    expect(cols.length).toBe(2);
    expect(cols.every((c) => c.getAttribute("aria-checked") === "true" && c.getAttribute("aria-disabled") === "true")).toBe(true);
  });

  test("turning All off writes the current non-key columns out as an explicit list", () => {
    const { getByText, onValueColumnsChange } = renderPickers({});
    fireEvent.click(getByText(/All non-key columns/).closest('[role="checkbox"]')!);
    expect(onValueColumnsChange).toHaveBeenCalledWith(["host", "env"]);
  });

  test("an explicit list stays explicit when every column is ticked", () => {
    const { getByText, container, onValueColumnsChange } = renderPickers({ valueColumns: ["host"] });
    expect(getByText(/All non-key columns/).closest('[role="checkbox"]')!.getAttribute("aria-checked")).toBe("false");
    const env = [...container.querySelectorAll('[role="checkbox"]')].find((b) => b.textContent === "env")!;
    fireEvent.click(env);
    expect(onValueColumnsChange).toHaveBeenCalledWith(["host", "env"]);
  });

  test("without a table both pickers stay on screen, disabled, with the reason", () => {
    const { getAllByRole, container } = renderPickers({ columns: [], unavailableReason: "Select a file first." });
    expect(getAllByRole("note").map((n) => n.textContent)).toEqual(["Select a file first.", "Select a file first."]);
    expect((container.querySelector("select") as HTMLSelectElement).disabled).toBe(true);
    expect(container.querySelector('[role="checkbox"]')?.getAttribute("aria-disabled")).toBe("true");
  });

  test("the reasons name the missing step", () => {
    expect(csvColumnsUnavailable("", false)).toContain("Select a file first");
    expect(csvColumnsUnavailable("f", true)).toBe("Waiting for the preview.");
    expect(csvColumnsUnavailable("f", false, { error: "bad", columns: [] })).toContain("preview failed");
    expect(csvColumnsUnavailable("f", false, { columns: [] })).toBe("The preview found no columns.");
    expect(csvColumnsUnavailable("f", false, { columns: ["a"] })).toBeUndefined();
    expect(tableColumnsUnavailable("", [])).toContain("Select a file first");
    expect(tableColumnsUnavailable("f", [])).toContain("array of objects");
    expect(tableColumnsUnavailable("f", ["a"])).toBeUndefined();
  });
});

// ── MMDB file picker ──────────────────────────────────────────────────

describe("MMDB file eligibility", () => {
  test("a file named as the other database type is ruled out; unnamed files are not", () => {
    expect(mmdbFileIneligibility("city", "GeoLite2-ASN.mmdb")).toBe("named as an ASN database");
    expect(mmdbFileIneligibility("asn", "GeoLite2-City.mmdb")).toBe("named as a GeoIP City database");
    expect(mmdbFileIneligibility("city", "GeoLite2-City.mmdb")).toBeUndefined();
    expect(mmdbFileIneligibility("asn", "custom.mmdb")).toBeUndefined();
  });

  const uploadFile = { isPending: false, mutate: () => {} } as unknown as Parameters<typeof FileDropZone>[0]["uploadFile"];

  test("an ineligible file is listed disabled with its reason and cannot be picked", () => {
    const onFileSelected = mock((_id: string) => {});
    const { getByText } = withHelp(
      <FileDropZone
        dark
        inputId="t"
        accept=".mmdb"
        label="x"
        pickableFiles={[file(1, "GeoLite2-ASN.mmdb"), file(2, "GeoLite2-City.mmdb")]}
        ineligibleReason={(f) => mmdbFileIneligibility("city", f.name)}
        helpRef="lookups-settings#mmdb-lookups"
        uploadFile={uploadFile}
        addToast={() => {}}
        onFileSelected={onFileSelected}
      />,
    );
    const asn = getByText("GeoLite2-ASN.mmdb").closest("button")!;
    expect(asn.disabled).toBe(true);
    expect(asn.textContent).toContain("— named as an ASN database");
    fireEvent.click(asn);
    expect(onFileSelected).not.toHaveBeenCalled();
    const city = getByText("GeoLite2-City.mmdb").closest("button")!;
    expect(city.disabled).toBe(false);
    fireEvent.click(city);
    expect(onFileSelected).toHaveBeenCalledWith(encode(testId(2)));
  });

  test("an empty list names the cause and links to what qualifies", () => {
    const { getByRole, getByText, openHelp } = withHelp(
      <FileDropZone dark inputId="t" accept=".mmdb" label="x" pickableFiles={[]} helpRef="lookups-settings#mmdb-lookups" uploadFile={uploadFile} addToast={() => {}} />,
    );
    expect(getByRole("note").textContent).toContain("No .mmdb files uploaded yet");
    fireEvent.click(getByText("What qualifies"));
    expect(openHelp).toHaveBeenCalledWith("lookups-settings#mmdb-lookups");
  });
});

// ── MaxMind card is always present ────────────────────────────────────

describe("MaxMindCard", () => {
  test("renders with a disabled status and no Disable action when nothing is saved", () => {
    const { getByText, queryByText } = withHelp(<MaxMindCard dark addToast={() => {}} />);
    expect(getByText("MaxMind Auto-Download")).toBeTruthy();
    expect(getByText("disabled")).toBeTruthy();
    fireEvent.click(getByText("MaxMind Auto-Download"));
    expect(queryByText("Disable")).toBeNull();
  });

  test("renders enabled with a Disable action when downloads are on", () => {
    const { getByText } = withHelp(
      <MaxMindCard dark addToast={() => {}} savedMaxmind={{ autoDownload: true, licenseConfigured: true } as never} />,
    );
    expect(getByText("enabled")).toBeTruthy();
    fireEvent.click(getByText("MaxMind Auto-Download"));
    expect(getByText("Disable")).toBeTruthy();
  });
});

// ── Cloud service restore fields and provider list ────────────────────

describe("cloud service restore fields", () => {
  test("GCS keeps Restore Speed and Restore Days on screen, disabled with the reason", () => {
    const { getByText, container } = renderWritable(<CloudServiceFields values={values("gcs")} onChange={() => {}} dark />);
    const speed = getByText("Restore Speed");
    expect(fieldNote(speed)?.textContent).toContain("GCS has no restore step");
    expect((container.querySelector(`#${CSS.escape((speed as HTMLLabelElement).htmlFor)}`) as HTMLSelectElement).disabled).toBe(true);
    const days = getByText("Restore Days");
    expect(fieldNote(days)?.textContent).toContain("S3 only");
    expect((container.querySelector(`#${CSS.escape((days as HTMLLabelElement).htmlFor)}`) as HTMLInputElement).disabled).toBe(true);
  });

  test("S3 enables both; Azure enables speed only", () => {
    const s3 = renderWritable(<CloudServiceFields values={values("s3")} onChange={() => {}} dark />);
    expect(fieldNote(s3.getByText("Restore Speed"))).toBeNull();
    expect(fieldNote(s3.getByText("Restore Days"))).toBeNull();
    s3.unmount();
    const azure = renderWritable(<CloudServiceFields values={values("azure")} onChange={() => {}} dark />);
    expect(fieldNote(azure.getByText("Restore Speed"))).toBeNull();
    expect(fieldNote(azure.getByText("Restore Days"))?.textContent).toContain("S3 only");
    expect(restoreSpeedReason("memory")).toContain("memory provider");
    expect(restoreDaysReason("memory")).toContain("S3 only");
  });

  test("the memory provider is offered with what it is in its label", () => {
    const memory = providerOptions.find((o) => o.value === "memory")!;
    expect(memory.label).toBe("Memory — in-process test store, lost on restart");
  });
});

// ── Route destinations and join eligibility (pure rules) ──────────────

describe("destinationState", () => {
  test("names JSONL sinks and disabled vaults, and nothing for an ordinary vault", () => {
    expect(destinationState({ type: VaultType.FILE, enabled: true })).toBeUndefined();
    expect(destinationState({ type: VaultType.JSONL, enabled: true })).toBe("JSONL sink, write-only");
    expect(destinationState({ type: VaultType.MEMORY, enabled: false })).toBe("disabled, not accepting records");
    expect(destinationState({ type: VaultType.JSONL, enabled: false })).toBe("JSONL sink, write-only; disabled, not accepting records");
  });
});

describe("joinIneligibility", () => {
  test("only a single-node cluster may join", () => {
    expect(joinIneligibility(1)).toBeUndefined();
    expect(joinIneligibility(3)).toContain("already one of 3 members");
  });
});

// ── Read-only dialog indicator ────────────────────────────────────────

describe("settings dialog read-only indicator", () => {
  function renderDialog(isAdmin: boolean) {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], {});
    qc.setQueryData(["certificates"], { certificates: [] });
    const openHelp = mock((_helpRef?: string) => {});
    const result = render(
      <HelpProvider onOpen={openHelp}>
        <SettingsDialog dark tab="service" onTabChange={() => {}} onClose={() => {}} isAdmin={isAdmin} />
      </HelpProvider>,
      { wrapper: settingsWrapper(qc) },
    );
    return { ...result, openHelp };
  }

  test("a non-admin sees one note naming the cause, linking to the roles section", () => {
    const { getAllByRole, getByText, openHelp } = renderDialog(false);
    const note = getAllByRole("note").find((n) => n.textContent.startsWith("Read-only"))!;
    expect(note.textContent).toContain("the user role can view settings but not change them");
    fireEvent.click(getByText("Roles"));
    expect(openHelp).toHaveBeenCalledWith("user-management#roles");
  });

  test("an admin sees no such note", () => {
    const { queryAllByRole } = renderDialog(true);
    expect(queryAllByRole("note").some((n) => n.textContent.startsWith("Read-only"))).toBe(false);
  });
});
