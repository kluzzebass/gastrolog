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

  test("every help reference in the source lands on a real topic and section", async () => {
    const refs = await collectHelpRefs();
    expect(refs.length).toBeGreaterThan(20);
    const headingsByTopic = new Map<string, string[]>();
    const broken: string[] = [];
    for (const { ref, file } of refs) {
      const { topicId, anchor } = parseHelpRef(ref);
      const topic = findTopic(topicId);
      if (!topic) {
        broken.push(`${file}: "${ref}" names no topic`);
        continue;
      }
      if (!anchor) continue;
      let headings = headingsByTopic.get(topic.id);
      if (!headings) {
        headings = headingAnchors(await topic.load());
        headingsByTopic.set(topic.id, headings);
      }
      if (!headings.includes(anchor)) {
        broken.push(`${file}: "${ref}" names no heading of ${topic.id} (have: ${headings.join(", ")})`);
      }
    }
    expect(broken).toEqual([]);
  });
});

/**
 * Every help reference written as a literal in the frontend source: the
 * `helpRef` prop or object key, a `*_HELP_REF` constant, or an `openHelp`
 * call. A reference that only exists at runtime (built from a variable)
 * is outside this check.
 */
async function collectHelpRefs(): Promise<{ ref: string; file: string }[]> {
  const root = new URL("../", import.meta.url).pathname;
  const glob = new Bun.Glob("**/*.{ts,tsx}");
  const pattern = /(?:helpRef\s*[=:]\s*\{?\s*|HELP_REF\w*\s*=\s*|openHelp\(\s*)["'`]([^"'`]+)["'`]/g;
  const out: { ref: string; file: string }[] = [];
  for await (const file of glob.scan(root)) {
    if (file.includes(".test.") || file.startsWith("api/gen/")) continue;
    const text = await Bun.file(root + file).text();
    for (const m of text.matchAll(pattern)) {
      const ref = m[1];
      if (ref) out.push({ ref, file });
    }
  }
  return out;
}
