import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import { VaultHoldingsBadges } from "./VaultCard";

// The vault card header counts chunks and records from the chunk list,
// which is a fan-out to every node hosting the vault. Until it returns there
// is nothing to count: the header says so instead of showing zeros that
// read as an empty vault.

describe("VaultHoldingsBadges", () => {
  test("before the chunk list arrives, a loading cue and never a zero", () => {
    const { container, queryByText } = render(<VaultHoldingsBadges chunks={undefined} dark />);
    expect(queryByText("loading chunks…")).not.toBeNull();
    expect(container.textContent).not.toContain("0 chunks");
    expect(container.textContent).not.toContain("0 records");
    expect(container.textContent).not.toContain("0 B");
  });

  test("with the chunk list, the counts and the labeled on-disk sum", () => {
    const chunks = [
      new ChunkMeta({ recordCount: BigInt(400), bytes: BigInt(5000), diskBytes: BigInt(1024) }),
      new ChunkMeta({ recordCount: BigInt(231), bytes: BigInt(9000), cloudBacked: true, diskBytes: BigInt(0), cloudBytes: BigInt(700) }),
    ];
    const { queryByText, container } = render(<VaultHoldingsBadges chunks={chunks} dark />);
    expect(queryByText("2 chunks")).not.toBeNull();
    expect(queryByText("631 records")).not.toBeNull();
    // The cloud-only chunk counts 0 on disk: 1024 B, labeled as the disk sum.
    expect(container.textContent).toContain("1.0 KiB on disk");
    expect(queryByText("loading chunks…")).toBeNull();
  });

  test("an empty chunk list is a real zero, not a loading state", () => {
    const { queryByText } = render(<VaultHoldingsBadges chunks={[]} dark />);
    expect(queryByText("0 chunks")).not.toBeNull();
    expect(queryByText("loading chunks…")).toBeNull();
  });
});
