import { describe, test, expect, beforeEach, mock } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { encode } from "../../api/glid";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";

const mocks = installMockClients();

import { ServiceSettings } from "./ServiceSettings";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleSettings = {
  auth: {
    tokenDuration: "15m",
    refreshTokenDuration: "168h",
    jwtSecretConfigured: true,
    passwordPolicy: {
      minLength: 8,
      requireMixedCase: false,
      requireDigit: true,
      requireSpecial: false,
      maxConsecutiveRepeats: 0,
      forbidAnimalNoise: false,
    },
  },
  scheduler: { maxConcurrentJobs: 4 },
  query: { timeout: "30s", maxFollowDuration: "4h", maxResultCount: 10000 },
  tls: { defaultCert: "", enabled: false, httpToHttpsRedirect: false, httpsPort: "" },
  cluster: { broadcastInterval: "5s" },
};

beforeEach(() => {
  m(mocks.systemClient, "getSettings").mockClear();
  m(mocks.systemClient, "putServiceSettings").mockClear();
  m(mocks.systemClient, "listCertificates").mockClear();
});

describe("ServiceSettings", () => {
  test("renders all section cards", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Authentication")).toBeTruthy();
    expect(getByText("Password Policy")).toBeTruthy();
    expect(getByText("Scheduler")).toBeTruthy();
    expect(getByText("Query")).toBeTruthy();
    expect(getByText("TLS")).toBeTruthy();
    expect(getByText("Broadcasting")).toBeTruthy();
  });

  test("authentication section expanded by default with field labels", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Token Duration")).toBeTruthy();
    expect(getByText("Refresh Token Duration")).toBeTruthy();
    expect(getByText("JWT Secret")).toBeTruthy();
  });

  test("initializes form values from settings data", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByDisplayValue } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByDisplayValue("15m")).toBeTruthy();
    expect(getByDisplayValue("168h")).toBeTruthy();
  });

  test("password policy fields visible when expanded", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Expand password policy
    fireEvent.click(getByText("Password Policy"));
    expect(getByText("Minimum length")).toBeTruthy();
    expect(getByText("Max consecutive repeats")).toBeTruthy();
    expect(getByText(/Require mixed case/)).toBeTruthy();
    expect(getByText(/Require digit/)).toBeTruthy();
    expect(getByText(/Require special character/)).toBeTruthy();
    expect(getByText(/Forbid animal noises/)).toBeTruthy();
  });

  test("scheduler section shows max concurrent jobs", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Scheduler"));
    expect(getByText("Max Concurrent Jobs")).toBeTruthy();
  });

  test("query section shows timeout and limits", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Query"));
    expect(getByText("Query Timeout")).toBeTruthy();
    expect(getByText("Max Follow Duration")).toBeTruthy();
    expect(getByText("Max Result Count")).toBeTruthy();
  });

  test("TLS section shows certificate dropdown", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], {
      certificates: [{ id: testId(1), name: "my-cert" }],
    });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("TLS"));
    expect(getByText("Default certificate")).toBeTruthy();
    expect(getByText("my-cert")).toBeTruthy();
  });

  test("save button disabled when form is clean", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("noAuth prop shows disabled badge on auth sections", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getAllByText } = renderWritable(<ServiceSettings dark noAuth />, {
      wrapper: settingsWrapper(qc),
    });

    // Both Authentication and Password Policy get "disabled" badge
    expect(getAllByText("disabled").length).toBe(2);
  });

  // Under --no-auth the authentication settings are not consulted. A form
  // that still accepted edits would save into a mode that ignores them, so
  // the inputs are read-only and the secret cannot be regenerated. With auth
  // on, the same inputs are writable — the premise the first half rests on.
  test("noAuth makes authentication inputs read-only and hides the regenerate action", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const noAuth = renderWritable(<ServiceSettings dark noAuth />, { wrapper: settingsWrapper(qc) });
    const tokenInput = noAuth.getByDisplayValue("15m") as HTMLInputElement;
    expect(tokenInput.readOnly).toBe(true);
    expect(noAuth.queryByText("Regenerate Secret")).toBeNull();
    noAuth.unmount();

    const withAuth = renderWritable(<ServiceSettings dark />, { wrapper: settingsWrapper(qc) });
    expect((withAuth.getByDisplayValue("15m") as HTMLInputElement).readOnly).toBe(false);
    expect(withAuth.queryByText("Regenerate Secret")).not.toBeNull();
  });

  // The TLS header badge reads the saved configuration. Ticking the
  // checkbox is a proposal: the badge stays off until Save, and appears
  // once the saved settings echoed back from the server say enabled.
  test("TLS badge follows saved state, not the draft", async () => {
    const certId = testId(1);
    const settingsWithCert = {
      ...sampleSettings,
      tls: { defaultCert: encode(certId), enabled: false, httpToHttpsRedirect: false, httpsPort: "" },
    };
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], settingsWithCert);
    qc.setQueryData(["certificates"], { certificates: [{ id: certId, name: "my-cert" }] });
    m(mocks.systemClient, "putServiceSettings").mockResolvedValueOnce({
      echo: { settings: { ...settingsWithCert, tls: { ...settingsWithCert.tls, enabled: true } } },
    });

    const { getByText, queryByText, getAllByRole } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });
    fireEvent.click(getByText("TLS"));
    expect(queryByText("enabled")).toBeNull();

    // The only checkbox on screen is Enable TLS: Authentication (expanded)
    // has none and Password Policy is collapsed.
    const enableTls = getAllByRole("checkbox")[0]!;
    fireEvent.click(enableTls);
    expect(enableTls.getAttribute("aria-checked")).toBe("true");
    expect(queryByText("enabled")).toBeNull();

    fireEvent.click(getByText("Save"));
    await waitFor(() => expect(queryByText("enabled")).not.toBeNull());
  });

  test("TLS saved as enabled on a certificate that no longer exists says so", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], {
      ...sampleSettings,
      tls: { defaultCert: encode(testId(9)), enabled: true, httpToHttpsRedirect: false, httpsPort: "" },
    });
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText, queryByText } = renderWritable(<ServiceSettings dark />, { wrapper: settingsWrapper(qc) });
    expect(getByText("enabled")).toBeTruthy();
    fireEvent.click(getByText("TLS"));
    expect(queryByText(/saved default certificate no longer exists/)).not.toBeNull();
  });

  // The three query limits share one disable convention, stated as the
  // server reads it: an empty duration or 0s is no timeout; an empty or 0
  // count is no cap. The placeholders are empty because no typed default
  // is what empty resolves to.
  test("the query limits state the same disable convention and carry no placeholder", () => {
    const { getByText, container } = renderWithHelp();
    fireEvent.click(getByText("Query"));
    expect(getByText("Longest a Search, Histogram or GetContext query may run; empty or 0s means no limit.")).toBeTruthy();
    expect(getByText("Longest a Follow stream stays open before the server closes it; empty or 0s means no limit.")).toBeTruthy();
    expect(getByText("Most records one Search request returns; empty or 0 means no limit.")).toBeTruthy();
    for (const name of ["Query Timeout", "Max Follow Duration"]) {
      const label = getByText(name) as HTMLLabelElement;
      const input = container.querySelector(`#${CSS.escape(label.htmlFor)}`) as HTMLInputElement;
      expect(input.placeholder).toBe("");
    }
  });

  // A saved 0 is "no cap", and the form must show it as 0 — rewriting it to
  // the bootstrap default would both lie and save the lie back.
  test("a saved max result count of 0 shows as 0, not as the bootstrap default", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], { ...sampleSettings, query: { ...sampleSettings.query, maxResultCount: 0 } });
    qc.setQueryData(["certificates"], { certificates: [] });
    const { getByText, container } = renderWritable(<ServiceSettings dark />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(getByText("Query"));
    const label = getByText("Max Result Count") as HTMLLabelElement;
    const input = container.querySelector(`#${CSS.escape(label.htmlFor)}`) as HTMLInputElement;
    expect(input.value).toBe("0");
    expect((getByText("Save").closest("button") as HTMLButtonElement).disabled).toBe(true);
  });

  test("the animal-noise rule says what the server rejects", () => {
    const { getByText } = renderWithHelp();
    fireEvent.click(getByText("Password Policy"));
    expect(getByText("Forbid animal noises")).toBeTruthy();
    expect(getByText(/Rejects a password containing moo, woof, bark, meow/)).toBeTruthy();
    expect(getByText(/caw or gobble \(case-insensitive\)/)).toBeTruthy();
  });

  test("the broadcast interval names alarms, and the backlog budget is one line with its anchor", () => {
    const { getByText, openHelp } = renderWithHelp();
    fireEvent.click(getByText("Broadcasting"));
    expect(getByText(/\(vault stats, route stats, alarms\)/)).toBeTruthy();
    fireEvent.click(getByText("Pipeline"));
    expect(getByText("Per-vault bound on completed segments awaiting chunking; at it the vault refuses new records until chunking drains below it. Empty means unbounded.")).toBeTruthy();
    fireEvent.click(fieldHelpButton(getByText("Backlog Budget")));
    expect(openHelp).toHaveBeenCalledWith("service-settings#pipeline");
  });

  test("broadcasting section shows broadcast interval", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText, getByDisplayValue } = renderWritable(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Broadcasting"));
    expect(getByText("Broadcast Interval")).toBeTruthy();
    expect(getByDisplayValue("5s")).toBeTruthy();
  });
});

function renderWithHelp(noAuth?: boolean, certificates: { id: Uint8Array; name: string }[] = [{ id: testId(1), name: "my-cert" }]) {
  const qc = createTestQueryClient();
  qc.setQueryData(["settings"], sampleSettings);
  qc.setQueryData(["certificates"], { certificates });
  const openHelp = mock((_helpRef?: string) => {});
  const result = renderWritable(
    <HelpProvider onOpen={openHelp}>
      <ServiceSettings dark noAuth={noAuth} />
    </HelpProvider>,
    { wrapper: settingsWrapper(qc) },
  );
  return { ...result, openHelp };
}

/** The note rendered inside the FormField that owns this label or input, if any. */
function fieldNote(el: HTMLElement): Element | null {
  return el.closest(".flex-col")!.querySelector('[role="note"]');
}

/** The help button beside a form field's label. */
function fieldHelpButton(label: HTMLElement): HTMLButtonElement {
  const button = label.parentElement!.querySelector('button[aria-label="Help"]');
  expect(button).not.toBeNull();
  return button as HTMLButtonElement;
}

describe("ServiceSettings help", () => {
  test("the default certificate field opens the certificates section that explains it", () => {
    const { getByText, openHelp } = renderWithHelp();
    fireEvent.click(getByText("TLS"));

    fireEvent.click(fieldHelpButton(getByText("Default certificate")));

    expect(openHelp).toHaveBeenCalledWith("certificates#default-certificate");
  });

  test("with no certificate stored the picker says so and links to the default-certificate section", () => {
    const { getByText, getByLabelText, openHelp } = renderWithHelp(false, []);
    fireEvent.click(getByText("TLS"));
    expect(fieldNote(getByLabelText("Default certificate"))?.textContent).toContain(
      "No certificates stored — add one in the Certificates tab.",
    );
    fireEvent.click(getByText("Eligibility rules"));
    expect(openHelp).toHaveBeenCalledWith("certificates#default-certificate");
  });

  test("the TLS fields stay on screen without a certificate, disabled with the reason", () => {
    const { getByText, container } = renderWithHelp(false, []);
    fireEvent.click(getByText("TLS"));
    for (const label of ["Enable TLS (HTTPS)", "HTTPS port", "Redirect HTTP to HTTPS"]) {
      expect(fieldNote(getByText(label))?.textContent).toBe("Requires a default certificate.");
    }
    expect(getByText("Enable TLS (HTTPS)").closest(".flex-col")!.querySelector('[role="checkbox"]')?.getAttribute("aria-disabled")).toBe("true");
    const port = container.querySelector(`#${CSS.escape((getByText("HTTPS port") as HTMLLabelElement).htmlFor)}`) as HTMLInputElement;
    expect(port.disabled).toBe(true);
  });

  test("choosing a certificate frees Enable TLS; enabling TLS frees the port and redirect", () => {
    const { getByText, getByLabelText, container } = renderWithHelp(false);
    fireEvent.click(getByText("TLS"));
    fireEvent.change(getByLabelText("Default certificate"), { target: { value: encode(testId(1)) } });

    expect(fieldNote(getByText("Enable TLS (HTTPS)"))).toBeNull();
    expect(fieldNote(getByText("HTTPS port"))?.textContent).toBe("Requires TLS enabled.");
    expect(fieldNote(getByText("Redirect HTTP to HTTPS"))?.textContent).toBe("Requires TLS enabled.");

    fireEvent.click(getByText("Enable TLS (HTTPS)").closest(".flex-col")!.querySelector('[role="checkbox"]')!);
    expect(fieldNote(getByText("HTTPS port"))).toBeNull();
    expect(fieldNote(getByText("Redirect HTTP to HTTPS"))).toBeNull();
    const port = container.querySelector(`#${CSS.escape((getByText("HTTPS port") as HTMLLabelElement).htmlFor)}`) as HTMLInputElement;
    expect(port.disabled).toBe(false);
  });

  test("the --no-auth badge names its cause and opens the no-auth section", () => {
    const { getAllByText, openHelp } = renderWithHelp(true);
    const badge = getAllByText("disabled")[0]!;
    expect(badge.getAttribute("title")).toBe("disabled by --no-auth");

    fireEvent.click(badge);

    expect(openHelp).toHaveBeenCalledWith("user-management#no-auth-mode");
  });
});
