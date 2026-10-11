import { describe, test, expect } from "bun:test";
import {
  CLOUD_BACKED_VAULT_BADGE,
  INDEX_MISSING_TITLE,
  INDEX_OK_TITLE,
  archivedBadgeTitle,
  chunkDiskClaimBytes,
  chunksEmptyText,
  indexesEmptyText,
  refusingBadgeText,
  vaultRefusingCauseLabels,
  vaultRefusalDetails,
} from "./VaultCard";
import {
  ChunkMeta,
  ContributionReport,
  DegradedPeer,
  GetIndexesResponse,
  VaultAdmissionCause,
  VaultAdmissionRefusal,
} from "../../api/gen/gastrolog/v1/vault_pb";

function degraded(n: number): ContributionReport {
  return new ContributionReport({
    degraded: Array.from({ length: n }, (_, i) => new DegradedPeer({ nodeId: `node-${i}`, reason: "timeout" })),
  });
}

// Every badge and empty state on the card says what its state means, in
// the canonical term, for the data that produces it.
describe("vault card labels", () => {
  test("the cloud-backed badge uses the canonical term and names the consequence", () => {
    expect(CLOUD_BACKED_VAULT_BADGE.text).toBe("cloud-backed");
    expect(CLOUD_BACKED_VAULT_BADGE.title).toContain("sealed chunks are uploaded");
    expect(CLOUD_BACKED_VAULT_BADGE.helpRef).toBe("storage-cloud");
  });

  test("the refusing badge carries every active cause in its text, not only on hover", () => {
    expect(refusingBadgeText(["storage below floor"])).toBe("refusing: storage below floor");
    expect(refusingBadgeText(["storage below floor", "at max-size bound"])).toBe(
      "refusing: storage below floor, at max-size bound",
    );
    expect(refusingBadgeText([])).toBe("refusing");
  });

  test("an empty chunk list is a cluster-wide fact unless a host dropped out of the merge", () => {
    expect(chunksEmptyText(null)).toBe("No chunks in this vault yet.");
    expect(chunksEmptyText(degraded(0))).toBe("No chunks in this vault yet.");
    expect(chunksEmptyText(degraded(1))).toBe("No chunks reported — 1 node unreached.");
    expect(chunksEmptyText(degraded(3))).toBe("No chunks reported — 3 nodes unreached.");
  });

  test("an empty index list names whether no node answered or the answer is uncertain", () => {
    expect(indexesEmptyText()).toBe("Indexes unavailable — no node answered for this chunk.");
    expect(indexesEmptyText(new GetIndexesResponse({}))).toBe(
      "Indexes unavailable — no node answered for this chunk.",
    );
    expect(indexesEmptyText(new GetIndexesResponse({ contributionReport: degraded(2) }))).toBe(
      "Indexes unknown — the chunk was not found on the nodes that answered, 2 nodes unreached.",
    );
  });

  test("index state titles say which node the verdict is about and what to do", () => {
    expect(INDEX_OK_TITLE).toContain("node that answered");
    expect(INDEX_MISSING_TITLE).toContain("node that answered");
    expect(INDEX_MISSING_TITLE).toContain("reindex");
  });

  test("the archived badge names the storage class the chunk sits in and the action", () => {
    expect(archivedBadgeTitle("GLACIER")).toBe("archived to GLACIER — restore it before it can be queried");
    expect(archivedBadgeTitle("")).toBe("archived to offline storage — restore it before it can be queried");
  });
});

// Test helper: refusals arrive on the wire as VaultAdmissionRefusal{cause,
// detail} — build them tersely for cause-only assertions.
function refusal(cause: VaultAdmissionCause, detail = ""): VaultAdmissionRefusal {
  return new VaultAdmissionRefusal({ cause, detail });
}

// Pins the size accounting: the vault size badge and per-row size cell both
// sum this LOCAL disk claim, never the cloud object size and never a
// logical-bytes fallback for an evicted cloud-backed chunk.
describe("chunkDiskClaimBytes", () => {
  test("local sealed chunk reports diskBytes", () => {
    const chunk = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(900) });
    expect(chunkDiskClaimBytes(chunk)).toBe(900);
  });

  test("pipeline GLCB chunk with no diskBytes falls back to logical bytes", () => {
    const chunk = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(0) });
    expect(chunkDiskClaimBytes(chunk)).toBe(4000);
  });

  test("cached cloud-backed chunk reports its cache size, not the cloud object size", () => {
    const chunk = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(1200),
      cloudBytes: BigInt(300),
    });
    expect(chunkDiskClaimBytes(chunk)).toBe(1200);
  });

  test("evicted cloud-backed chunk reports 0, never logical bytes or cloud bytes", () => {
    const chunk = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(0),
      cloudBytes: BigInt(300),
    });
    expect(chunkDiskClaimBytes(chunk)).toBe(0);
  });

  test("mixed vault: sum excludes evicted cloud chunks, includes cached at local size", () => {
    const plain = new ChunkMeta({ bytes: BigInt(4000), diskBytes: BigInt(900) });
    const cachedCloud = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(1200),
      cloudBytes: BigInt(300),
    });
    const evictedCloud = new ChunkMeta({
      bytes: BigInt(999999),
      cloudBacked: true,
      diskBytes: BigInt(0),
      cloudBytes: BigInt(300),
    });
    const sum = [plain, cachedCloud, evictedCloud].reduce(
      (acc, c) => acc + chunkDiskClaimBytes(c),
      0,
    );
    expect(sum).toBe(900 + 1200 + 0);
  });
});

// Pins the badge's source: the vault card's "refusing" badge reads a
// first-class backend field (VaultInfo.admissionRefused, populated by the
// responding node's own admission-causes collector) and maps its enum
// values to terse labels — never a UI-side derivation from alarm state.
// vaultRefusingCauseLabels' empty-vs-non-empty result is exactly what gates
// the badge's visibility in VaultCard (`refusingCauses.length > 0`).
//
// The wire type is VaultAdmissionRefusal{cause, detail}. Disk-free
// thresholds belong to the storage, not the vault, so STORAGE_DISK_PROTECT
// refuses every vault placed on a below-floor storage.
describe("vaultRefusingCauseLabels", () => {
  test("empty causes yields no labels (badge hidden)", () => {
    expect(vaultRefusingCauseLabels([])).toEqual([]);
  });

  test("MAX_SIZE_BOUND maps to its label", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.MAX_SIZE_BOUND)])).toEqual([
      "at max-size bound",
    ]);
  });

  test("STORAGE_DISK_PROTECT maps to its label", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.STORAGE_DISK_PROTECT)])).toEqual([
      "storage below floor",
    ]);
  });

  test("BACKLOG_BUDGET maps to its label", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.BACKLOG_BUDGET)])).toEqual([
      "backlog at budget",
    ]);
  });

  // Refusal covers age and chunk-count bounds, not only size.
  test("AGE_BOUND maps to its label", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.AGE_BOUND)])).toEqual([
      "past age bound",
    ]);
  });

  test("CHUNK_COUNT_BOUND maps to its label", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.CHUNK_COUNT_BOUND)])).toEqual([
      "over chunk-count bound",
    ]);
  });

  test("all five causes at once yields all five labels, in backend order", () => {
    expect(
      vaultRefusingCauseLabels([
        refusal(VaultAdmissionCause.STORAGE_DISK_PROTECT),
        refusal(VaultAdmissionCause.MAX_SIZE_BOUND),
        refusal(VaultAdmissionCause.BACKLOG_BUDGET),
        refusal(VaultAdmissionCause.AGE_BOUND),
        refusal(VaultAdmissionCause.CHUNK_COUNT_BOUND),
      ]),
    ).toEqual([
      "storage below floor",
      "at max-size bound",
      "backlog at budget",
      "past age bound",
      "over chunk-count bound",
    ]);
  });

  test("UNSPECIFIED is dropped, never rendered as a blank badge cause", () => {
    expect(
      vaultRefusingCauseLabels([
        refusal(VaultAdmissionCause.UNSPECIFIED),
        refusal(VaultAdmissionCause.MAX_SIZE_BOUND),
      ]),
    ).toEqual(["at max-size bound"]);
  });

  test("a vault with only UNSPECIFIED yields no labels (badge hidden)", () => {
    expect(vaultRefusingCauseLabels([refusal(VaultAdmissionCause.UNSPECIFIED)])).toEqual([]);
  });
});

// Pins the detail rendering: the inspector's expanded refusal section
// renders the backend's detail string VERBATIM alongside the cause label —
// no client-side reconstruction of which storage or bound is involved (the
// operator directive: every signal shown comes from published backend
// fields).
describe("vaultRefusalDetails", () => {
  test("empty refusals yields no details (section hidden)", () => {
    expect(vaultRefusalDetails([])).toEqual([]);
  });

  test("pairs each cause's label with its own detail string", () => {
    expect(
      vaultRefusalDetails([
        refusal(VaultAdmissionCause.STORAGE_DISK_PROTECT, "storage \"nvme-fast\": 1.2GB free, floor 3GB"),
        refusal(VaultAdmissionCause.MAX_SIZE_BOUND, "max-size bound: 10GB"),
      ]),
    ).toEqual([
      { label: "storage below floor", detail: "storage \"nvme-fast\": 1.2GB free, floor 3GB" },
      { label: "at max-size bound", detail: "max-size bound: 10GB" },
    ]);
  });

  test("UNSPECIFIED is dropped from the details list too", () => {
    expect(
      vaultRefusalDetails([
        refusal(VaultAdmissionCause.UNSPECIFIED, "should never render"),
        refusal(VaultAdmissionCause.BACKLOG_BUDGET, "backlog bound: 5GB"),
      ]),
    ).toEqual([{ label: "backlog at budget", detail: "backlog bound: 5GB" }]);
  });

  test("detail text passes through verbatim, including a peer-reported note", () => {
    expect(
      vaultRefusalDetails([
        refusal(VaultAdmissionCause.STORAGE_DISK_PROTECT, "reported by node-2"),
      ]),
    ).toEqual([{ label: "storage below floor", detail: "reported by node-2" }]);
  });
});
