import { describe, test, expect, beforeEach } from "bun:test";
import { fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { encode } from "../../api/glid";

const mocks = installMockClients();

import { FilesSettings, fileLookupRefs } from "./FilesSettings";
import { LookupSettings } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const CITY_V1 = testId(1);
const CITY_V2 = testId(2);
const ASSETS = testId(3);

const files = [
  { id: CITY_V1, name: "GeoLite2-City.mmdb", size: 100n, sha256: "aaaa", uploadedAt: "2026-01-01T00:00:00Z" },
  { id: CITY_V2, name: "GeoLite2-City.mmdb", size: 100n, sha256: "bbbb", uploadedAt: "2026-02-01T00:00:00Z" },
  { id: ASSETS, name: "assets.csv", size: 10n, sha256: "cccc", uploadedAt: "2026-03-01T00:00:00Z" },
];

function seed(qc: ReturnType<typeof createTestQueryClient>, lookup: LookupSettings | undefined, maxmind?: object) {
  qc.setQueryData(["system"], { managedFiles: files, nodeConfigs: [], vaults: [], ingesters: [], routes: [] });
  qc.setQueryData(["settings"], { lookup, maxmind });
}

beforeEach(() => {
  m(mocks.systemClient, "putMaxMindSettings").mockClear();
});

// A file is "used by" a lookup when that lookup's saved configuration
// references one of the file's versions by id. The filename says nothing:
// a City database nobody configured is unused, and a lookup may read a file
// of any name.
describe("fileLookupRefs", () => {
  test("no lookups: no references, whatever the filename", () => {
    expect(fileLookupRefs([encode(CITY_V1)], new LookupSettings())).toEqual([]);
  });

  test("a lookup pointing at any version of the file references it", () => {
    const lookup = new LookupSettings({
      mmdbLookups: [{ name: "geoip", dbType: "city", fileId: CITY_V1 }],
      csvLookups: [{ name: "assets", fileId: ASSETS }],
    });
    expect(fileLookupRefs([encode(CITY_V2), encode(CITY_V1)], lookup)).toEqual(["lookup geoip"]);
    expect(fileLookupRefs([encode(ASSETS)], lookup)).toEqual(["lookup assets"]);
  });

  test("a lookup with no file (auto-downloaded database) references nothing", () => {
    const lookup = new LookupSettings({ mmdbLookups: [{ name: "geoip", dbType: "city" }] });
    expect(fileLookupRefs([encode(CITY_V1)], lookup)).toEqual([]);
  });
});

describe("FilesSettings used-by status", () => {
  test("a City database no lookup references carries no usage", () => {
    const qc = createTestQueryClient();
    seed(qc, new LookupSettings());
    const { queryByText } = renderWritable(<FilesSettings dark />, { wrapper: settingsWrapper(qc) });
    expect(queryByText("GeoLite2-City.mmdb")).not.toBeNull();
    expect(queryByText(/used by/)).toBeNull();
    expect(queryByText("lookup geoip")).toBeNull();
  });

  test("the lookups referencing a file by id are listed on its card", () => {
    const qc = createTestQueryClient();
    seed(qc, new LookupSettings({
      mmdbLookups: [{ name: "geoip", dbType: "city", fileId: CITY_V2 }],
      jsonFileLookups: [{ name: "hosts", fileId: ASSETS }],
    }));
    const { getByText } = renderWritable(<FilesSettings dark />, { wrapper: settingsWrapper(qc) });
    expect(getByText("lookup geoip")).toBeTruthy();
    expect(getByText("lookup hosts")).toBeTruthy();
  });
});

// The MaxMind card's "enabled" status reads the saved settings: ticking the
// checkbox proposes, Save decides, and the echo of the saved settings is
// what turns the status on.
describe("MaxMind status follows saved state", () => {
  test("toggling the draft leaves the status off until the save echoes back", async () => {
    const qc = createTestQueryClient();
    seed(qc, new LookupSettings(), { autoDownload: false, licenseConfigured: true });
    m(mocks.systemClient, "putMaxMindSettings").mockResolvedValueOnce({
      echo: { settings: { lookup: new LookupSettings(), maxmind: { autoDownload: true, licenseConfigured: true } } },
    });

    const { getByText, queryByText, getByRole } = renderWritable(<FilesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });
    expect(queryByText("enabled")).toBeNull();

    fireEvent.click(getByText("MaxMind Auto-Download"));
    fireEvent.click(getByRole("checkbox"));
    expect(queryByText("enabled")).toBeNull();

    fireEvent.click(getByText("Save"));
    await waitFor(() => expect(queryByText("enabled")).not.toBeNull());
  });
});
