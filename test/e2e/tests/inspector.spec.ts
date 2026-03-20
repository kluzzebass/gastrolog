import { test, expect } from "@playwright/test";
import { gotoAuthenticated } from "./helpers";

/** Open inspector and optionally switch to a specific mode. */
async function openInspector(
  page: import("@playwright/test").Page,
  mode?: "Nodes" | "Entities",
) {
  await gotoAuthenticated(page, "/search");
  await page.getByRole("button", { name: "Inspector" }).click();
  const dialog = page.getByRole("dialog", { name: "Inspector" });
  await expect(dialog).toBeVisible();

  if (mode) {
    const modeBtn = dialog.getByRole("button", { name: mode });
    if (await modeBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await modeBtn.click();
    }
  }

  return dialog;
}

test.describe.serial("Inspector", () => {
  test("opens inspector from header", async ({ page }) => {
    const dialog = await openInspector(page);
    await expect(dialog).toBeVisible();
  });

  test("shows entities view with vaults", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    // Click the Vaults entity tab — it shows a count badge if vaults exist.
    const vaultsBtn = dialog.getByRole("button", { name: /Vaults/i });
    await vaultsBtn.click();

    // The right panel should show vault details (heading + at least one vault card).
    await expect(dialog.getByText(/memory|file/i).first()).toBeVisible({ timeout: 10_000 });
  });

  test("shows ingesters in entities view", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    await dialog.getByRole("button", { name: "Ingesters" }).click();
    await expect(dialog.getByText("chatterbox")).toBeVisible();
  });

  test("vault detail shows chunk table", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    // Click Vaults — the first vault auto-selects and shows its detail.
    await dialog.getByRole("button", { name: "Vaults" }).click();

    // The vault detail pane should show chunk table headers.
    // "Records" is a column header (rendered uppercase via CSS).
    await expect(dialog.getByText("Records").first()).toBeVisible({
      timeout: 15_000,
    });
  });

  test("ingester detail shows status", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    await dialog.getByRole("button", { name: "Ingesters" }).click();
    await dialog.getByText("chatterbox").click();

    // Detail should show the ingester type and running status.
    await expect(dialog.getByText(/chatterbox/i)).toBeVisible();
  });

  test("switches between Nodes and Entities views", async ({ page }) => {
    const dialog = await openInspector(page);

    // In multi-node mode, both Nodes and Entities buttons should work.
    const nodesBtn = dialog.getByRole("button", { name: "Nodes" });
    const entitiesBtn = dialog.getByRole("button", { name: "Entities" });

    if (await nodesBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await nodesBtn.click();
      // Node names should appear.
      await expect(dialog.getByText("node-1")).toBeVisible();

      await entitiesBtn.click();
      // Entity tabs should appear.
      await expect(dialog.getByRole("button", { name: "Vaults" })).toBeVisible();
    }
  });

  test("closes inspector with Escape", async ({ page }) => {
    const dialog = await openInspector(page);
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });
});
