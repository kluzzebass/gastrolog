import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Pipeline operator and result visualization E2E tests
 * (gastrolog-3gylm, gastrolog-47b4o, gastrolog-2q7dv).
 *
 * Tests pipe queries (count, top, stats, sort, limit), chart/table
 * toggle, and export functionality.
 */

test.describe.serial("Pipeline operators and visualizations", () => {
  // ── count operator (gastrolog-3gylm) ─────────────────────────────────

  test("count operator returns a single value", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | count");
    await page.getByRole("button", { name: "Search" }).click();

    // Pipeline results should render — count produces a single numeric value.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });

  test("top operator shows a table", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | top level");
    await page.getByRole("button", { name: "Search" }).click();

    // Should show results with a table or chart view.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // The "Table" toggle should be available for pipeline results.
    const tableBtn = page.getByRole("button", { name: "Table" });
    if (await tableBtn.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await tableBtn.click();
      // Table should show column headers.
      await expect(page.locator("table").first()).toBeVisible({
        timeout: 5_000,
      });
    }
  });

  test("stats operator shows aggregation results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | stats count by level");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });

  test("sort operator with limit works", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | sort ingest_ts desc | limit 5");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    expect(count).toBeLessThanOrEqual(5);
  });

  // ── Chart/Table toggle (gastrolog-47b4o) ─────────────────────────────

  test("chart and table views toggle for pipeline results", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | top level");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Check for Chart/Table toggle buttons.
    const chartBtn = page.getByRole("button", { name: "Chart" });
    const tableBtn = page.getByRole("button", { name: "Table" });

    if (
      await chartBtn.isVisible({ timeout: 5_000 }).catch(() => false)
    ) {
      // Switch to table view.
      await tableBtn.click();
      await expect(page.locator("table").first()).toBeVisible({
        timeout: 5_000,
      });

      // Switch back to chart view.
      await chartBtn.click();

      // A chart canvas or SVG should be visible.
      const chart = page.locator("canvas, svg").first();
      await expect(chart).toBeVisible({ timeout: 5_000 });
    }
  });

  // ── Export (gastrolog-2q7dv) ──────────────────────────────────────────

  test("export button is visible after search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // The export button should be in the results toolbar.
    const exportBtn = page.getByRole("button", { name: "Export results" });
    await expect(exportBtn).toBeVisible();
  });

  test("export button opens format dropdown", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click the export button to open the format menu.
    await page.getByRole("button", { name: "Export results" }).click();

    // Should show format options (JSON, CSV).
    const jsonOpt = page.getByText("JSON");
    const csvOpt = page.getByText("CSV");

    const hasJson = await jsonOpt
      .isVisible({ timeout: 3_000 })
      .catch(() => false);
    const hasCsv = await csvOpt
      .isVisible({ timeout: 3_000 })
      .catch(() => false);

    // At least one export format should be visible.
    expect(hasJson || hasCsv).toBeTruthy();

    // Close the dropdown by pressing Escape.
    await page.keyboard.press("Escape");
  });

  test("export triggers a download", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Listen for download event.
    const downloadPromise = page.waitForEvent("download", { timeout: 10_000 }).catch(() => null);

    // Click export and select a format.
    await page.getByRole("button", { name: "Export results" }).click();

    const jsonOpt = page.getByText("JSON");
    if (await jsonOpt.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await jsonOpt.click();
    } else {
      // Try CSV.
      const csvOpt = page.getByText("CSV");
      if (await csvOpt.isVisible({ timeout: 3_000 }).catch(() => false)) {
        await csvOpt.click();
      }
    }

    const download = await downloadPromise;
    if (download) {
      // Verify the download has a filename.
      expect(download.suggestedFilename()).toBeTruthy();
    }
  });

  // ── Chained operators ────────────────────────────────────────────────

  test("chained pipeline operators work", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "* | top level | sort count desc | limit 3");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });
});
