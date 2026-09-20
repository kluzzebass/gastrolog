import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

// The shared Button, IconButton and DropdownButton hide themselves when the
// caller cannot write. A raw <button> does not, and three separate rounds of
// this bug were raw buttons: a rule row's remove, a grid's add row, and the
// component tree that added a rule on click. Each was found by a person
// clicking it, which is not a way to find the fourth.
//
// So: every raw <button> in these surfaces either honours read-only, or is
// named below as navigation with the reason it is safe. A new one that does
// neither fails here rather than shipping.

const SURFACES = ["src/components/settings", "src/components/inspector"];

// Navigation and disclosure: they move the caller around or reveal something
// already on screen. None of them changes state the server would store.
const NAVIGATION = new Map<string, string>([
  ["settings/SettingsDialog.tsx", "tab switching"],
  ["settings/SettingsCard.tsx", "expand/collapse and the delete-confirm flow, which only renders with write access"],
  ["settings/Buttons.tsx", "the shared primitives, which hide themselves"],
  ["settings/Checkbox.tsx", "a wrapper that only stops propagation"],
  ["settings/UsersSettings.tsx", "show/hide password on a field the caller already sees"],
  ["settings/NodesSettings.tsx", "reveal the join token, plus the join form gated separately"],
  ["settings/UsedByStatus.tsx", "cross-links to the entity using this one"],
  ["settings/FormField.tsx", "the input primitives, each gated individually"],
  ["inspector/InspectorDialog.tsx", "navigating between nodes and entity types"],
  ["inspector/ModeToggle.tsx", "switching inspector mode"],
  ["inspector/CrossLinkBadge.tsx", "cross-links to a related entity"],
]);

function tsxFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry);
    if (statSync(full).isDirectory()) {
      out.push(...tsxFiles(full));
      continue;
    }
    if (entry.endsWith(".tsx") && !entry.includes(".test.")) out.push(full);
  }
  return out;
}

/** Each raw <button …> with the attributes up to its closing bracket. */
function rawButtons(source: string): { line: number; attrs: string }[] {
  const found: { line: number; attrs: string }[] = [];
  const lines = source.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? "";
    // JSX opens the tag on its own line, so there is nothing after it.
    if (!/<button(?![A-Za-z])/.test(line)) continue;
    // Attributes run until the tag closes; buttons here are multi-line.
    let attrs = "";
    for (let j = i; j < Math.min(i + 20, lines.length); j++) {
      const attrLine = lines[j] ?? "";
      attrs += attrLine + "\n";
      if (/^\s*>/.test(attrLine) || /\/?>\s*$/.test(attrLine)) break;
    }
    found.push({ line: i + 1, attrs });
  }
  return found;
}

describe("raw buttons honour read-only", () => {
  test("every raw <button> is gated or named as navigation", () => {
    const offenders: string[] = [];

    for (const surface of SURFACES) {
      for (const file of tsxFiles(surface)) {
        const key = file.split("src/components/")[1] ?? file;
        if (NAVIGATION.has(key)) continue;

        const source = readFileSync(file, "utf8");
        for (const { line, attrs } of rawButtons(source)) {
          if (!attrs.includes("readOnly")) {
            offenders.push(`${key}:${line}`);
          }
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  test("the navigation list names real files", () => {
    // An entry that no longer matches a file would silently exempt nothing,
    // or worse, mask a file that got renamed into an ungated one.
    const present = new Set(
      SURFACES.flatMap((s) => tsxFiles(s)).map((f) => f.split("src/components/")[1] ?? f),
    );
    for (const key of NAVIGATION.keys()) {
      expect(present.has(key)).toBe(true);
    }
  });
});
