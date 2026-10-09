import { describe, test, expect } from "bun:test";
import { findTopic, headingSlug, parseHelpRef } from "./topics";
import { TRANSFER_HELP_REF } from "../components/settings/transferEligibility";

/** The anchor slugs of a topic's level-2 and level-3 headings. */
function headingAnchors(markdown: string): string[] {
  return markdown
    .split("\n")
    .filter((line) => line.startsWith("## ") || line.startsWith("### "))
    .map((line) => headingSlug(line.replace(/^#+ /, "")));
}

describe("help references", () => {
  test("a reference splits into topic ID and section anchor", () => {
    expect(parseHelpRef("vaults-config")).toEqual({ topicId: "vaults-config" });
    expect(parseHelpRef("vaults-config#transfer-target")).toEqual({
      topicId: "vaults-config",
      anchor: "transfer-target",
    });
    expect(parseHelpRef("vaults-config#")).toEqual({ topicId: "vaults-config", anchor: undefined });
  });

  test("heading slugs are lowercase words joined by hyphens", () => {
    expect(headingSlug("Transfer Target")).toBe("transfer-target");
    expect(headingSlug("  Rotation and Retention Policies ")).toBe("rotation-and-retention-policies");
    expect(headingSlug("File Vault Settings")).toBe("file-vault-settings");
  });

  test("findTopic resolves a reference with an anchor, and through an alias", () => {
    expect(findTopic("vaults-config#transfer-target")?.id).toBe("vaults-config");
    expect(findTopic("storage-engines#transfer-target")?.id).toBe("vaults-config");
  });

  test("the transfer eligibility link lands on a real section", async () => {
    const { topicId, anchor } = parseHelpRef(TRANSFER_HELP_REF);
    const topic = findTopic(topicId);
    expect(topic).toBeDefined();
    expect(headingAnchors(await topic!.load())).toContain(anchor!);
  });
});
