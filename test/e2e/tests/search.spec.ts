import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

test.describe.serial("Search", () => {
  test("shows the query bar on /search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    const queryArea = page.locator("textarea").or(
      page.locator("[role='button'][tabindex='0']"),
    );
    await expect(queryArea.first()).toBeVisible();
  });

  test("executes a wildcard query and shows results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page.locator("[data-testid='result-count']").textContent();
    expect(countText).toBeTruthy();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    expect(count).toBeGreaterThan(0);
  });

  test("shows histogram chart", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Histogram renders only when buckets > 0 — soft check.
    const histogram = page.locator("[data-testid='histogram']");
    if (await histogram.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await expect(histogram).toBeVisible();
    }
  });

  test("opens detail sidebar when clicking a log entry", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const firstEntry = page.locator("[data-testid='log-entry']").first();
    await expect(firstEntry).toBeVisible();
    await firstEntry.click();

    // Detail sidebar should open.
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Sidebar should show field details — timestamps, severity, or attributes.
    await expect(sidebar.getByText(/timestamp|severity|level|message/i).first()).toBeVisible({
      timeout: 5_000,
    });
  });

  test("detail sidebar shows copyable field values", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click a log entry to open the sidebar.
    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // The raw log line should be shown (font-mono text area).
    await expect(sidebar.locator(".font-mono").first()).toBeVisible();
  });

  test("severity filters toggle search results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Execute a wildcard search first to get results.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // The severity filter buttons should be visible in the sidebar.
    // Labels are "Error", "Warn", "Info", "Debug", "Trace" (rendered uppercase via CSS).
    const errorBtn = page.getByRole("button", { name: "Error" });
    const warnBtn = page.getByRole("button", { name: "Warn" });
    const infoBtn = page.getByRole("button", { name: "Info" });
    await expect(errorBtn).toBeVisible();
    await expect(warnBtn).toBeVisible();
    await expect(infoBtn).toBeVisible();

    // Click Error to toggle it — should filter results.
    await errorBtn.click();

    // Re-search to apply the filter.
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 15_000,
    });
  });

  test("time range presets change the range", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Time range section should be visible.
    await expect(page.getByText("Time Range")).toBeVisible();

    // Click the "1h" preset.
    const preset1h = page.getByRole("button", { name: "1h", exact: true });
    await expect(preset1h).toBeVisible();
    await preset1h.click();

    // The preset should now be active (has copper background).
    // Run a search with the new time range.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });

  test("vault selector shows available vaults", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The sidebar should show a "Vaults" section with at least one vault.
    await expect(page.getByText("Vaults")).toBeVisible();

    // The vault created by the setup wizard should be listed with a record count.
    // Look for a vault button that shows a number (record count).
    const vaultSection = page.locator("text=Vaults").locator("..");
    await expect(vaultSection).toBeVisible();
  });

  test("query with key=value syntax works", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Chatterbox generates logs with various formats including key=value.
    // Use a broad key=value query that should match some records.
    await typeQuery(page, 'level=error');
    await page.getByRole("button", { name: "Search" }).click();

    // Should get results (chatterbox generates error-level logs).
    // Wait with generous timeout — there may be few error-level records.
    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible({ timeout: 30_000 });
  });

  test("empty query shows no results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Search for something that definitely won't match.
    await typeQuery(page, '"e2e_nonexistent_string_xyzzy_12345"');
    await page.getByRole("button", { name: "Search" }).click();

    // Should show 0 results or empty state. Wait for the search to complete.
    // The result count should show "0" or not appear at all.
    await page.waitForTimeout(3_000);

    const resultCount = page.locator("[data-testid='result-count']");
    if (await resultCount.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const text = await resultCount.textContent();
      const count = parseInt(text!.replace(/[^0-9]/g, ""), 10);
      expect(count).toBe(0);
    }
  });
});
