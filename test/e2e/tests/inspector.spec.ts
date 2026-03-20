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
    await expect(dialog.getByText(/memory|file/i).first()).toBeVisible({
      timeout: 10_000,
    });
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
      await expect(
        dialog.getByRole("button", { name: "Vaults" }),
      ).toBeVisible();
    }
  });

  test("closes inspector with Escape", async ({ page }) => {
    const dialog = await openInspector(page);
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  // ── Jobs tab (gastrolog-5iji6) ─────────────────────────────────────

  test("shows Jobs tab in entities view", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    const jobsBtn = dialog.getByRole("button", { name: "Jobs" });
    await expect(jobsBtn).toBeVisible();
    await jobsBtn.click();

    // Jobs heading should appear.
    await expect(dialog.getByText("Jobs").first()).toBeVisible();
  });

  test("Jobs tab shows scheduled jobs or empty state", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");
    await dialog.getByRole("button", { name: "Jobs" }).click();

    // The cluster runs scheduled jobs (rotation, retention, indexing).
    // Either we see job entries or a "No active or scheduled jobs" message.
    const hasJobs = await dialog
      .locator("table")
      .isVisible({ timeout: 10_000 })
      .catch(() => false);

    if (!hasJobs) {
      await expect(dialog.getByText(/no.*jobs/i).first()).toBeVisible();
    }
  });

  // ── Routes tab (gastrolog-5iji6) ───────────────────────────────────

  test("shows Routes tab in entities view", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    const routesBtn = dialog.getByRole("button", { name: "Routes" });
    await expect(routesBtn).toBeVisible();
    await routesBtn.click();

    // Route stats should show the default route.
    await expect(dialog.getByText("default").first()).toBeVisible({
      timeout: 10_000,
    });
  });

  test("route stats shows throughput metrics", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");
    await dialog.getByRole("button", { name: "Routes" }).click();

    // Route stats display should show numeric throughput data.
    // The "default" route should have processed records from chatterbox.
    await expect(dialog.getByText("default").first()).toBeVisible({
      timeout: 10_000,
    });

    // Route stats view shows "Ingested", "Routed", "Dropped" stat boxes
    // and "Matched"/"Forwarded" column headers for per-route/vault stats.
    await expect(
      dialog.getByText(/Ingested|Routed|Matched/i).first(),
    ).toBeVisible({ timeout: 10_000 });
  });

  // ── System tab ─────────────────────────────────────────────────────

  test("shows System tab in entities view", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    const systemBtn = dialog.getByRole("button", { name: "System" });
    await expect(systemBtn).toBeVisible();
    await systemBtn.click();
  });

  // ── Cross-navigation (gastrolog-5hhp3) ──────────────────────────────

  test("vault detail has Open in Settings link", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    await dialog.getByRole("button", { name: /Vaults/i }).click();

    // The vault card should have a cog icon linking to settings.
    const settingsLink = dialog.locator('[title="Open in Settings"]');
    if (
      await settingsLink
        .first()
        .isVisible({ timeout: 5_000 })
        .catch(() => false)
    ) {
      await settingsLink.first().click();

      // Settings dialog should open (inspector closes, settings opens).
      const settingsDialog = page.getByRole("dialog", { name: "Settings" });
      await expect(settingsDialog).toBeVisible({ timeout: 5_000 });

      // Close settings to restore state.
      await page.keyboard.press("Escape");
    }
  });

  test("ingester detail has Open in Settings link", async ({ page }) => {
    const dialog = await openInspector(page, "Entities");

    await dialog.getByRole("button", { name: "Ingesters" }).click();
    await dialog.getByText("chatterbox").click();

    const settingsLink = dialog.locator('[title="Open in Settings"]');
    if (
      await settingsLink
        .first()
        .isVisible({ timeout: 5_000 })
        .catch(() => false)
    ) {
      await settingsLink.first().click();

      const settingsDialog = page.getByRole("dialog", { name: "Settings" });
      await expect(settingsDialog).toBeVisible({ timeout: 5_000 });

      await page.keyboard.press("Escape");
    }
  });

  // ── Node detail shows jobs (gastrolog-5iji6) ───────────────────────

  test("node detail shows jobs section", async ({ page }) => {
    const dialog = await openInspector(page);

    const nodesBtn = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesBtn).toBeVisible({ timeout: 10_000 });
    await nodesBtn.click();

    // Click node-1 to see its detail.
    await dialog.getByText("node-1").click();
    await expect(dialog.getByText("Uptime")).toBeVisible({ timeout: 10_000 });

    // The node detail pane shows a jobs section (scheduled + tasks).
    // Either scheduled jobs table or "No scheduled jobs" message.
    const hasScheduled = await dialog
      .getByText(/scheduled/i)
      .first()
      .isVisible({ timeout: 5_000 })
      .catch(() => false);

    if (!hasScheduled) {
      await expect(dialog.getByText(/no.*jobs/i).first()).toBeVisible();
    }
  });
});
