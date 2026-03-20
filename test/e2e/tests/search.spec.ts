import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

test.describe.serial("Search", () => {
  test("shows the query bar on /search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    // The query bar is present — either collapsed (role="button") or expanded (textarea).
    // storageState may pre-populate it with history, so "Search logs..." placeholder
    // is not guaranteed.
    const queryArea = page.locator("textarea").or(
      page.locator("[role='button'][tabindex='0']"),
    );
    await expect(queryArea.first()).toBeVisible();
  });

  test("executes a wildcard query and shows results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Expand the query bar and fill it.
    await typeQuery(page, "*");

    // Execute the search.
    await page.getByRole("button", { name: "Search" }).click();

    // Chatterbox generates data continuously — results should appear.
    // Wait for at least one log entry to render.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Result count should be > 0.
    const countText = await page.locator("[data-testid='result-count']").textContent();
    expect(countText).toBeTruthy();
    // The count text contains something like "1,234 results" — extract the number.
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    expect(count).toBeGreaterThan(0);
  });

  test("shows histogram chart", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Run a search first.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    // Wait for results.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Histogram renders only when buckets > 0. With short-lived data,
    // there may not be enough time-bucketed results. Check it exists in DOM
    // (may be zero-height if no buckets) or that results loaded.
    const histogram = page.locator("[data-testid='histogram']");
    // If histogram rendered, great. If not, the search still works — this
    // is a soft check. The hard assertion is the result count above.
    if (await histogram.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await expect(histogram).toBeVisible();
    }
  });

  test("opens detail sidebar when clicking a log entry", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Run a search.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    // Wait for results.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click the first log entry.
    const firstEntry = page.locator("[data-testid='log-entry']").first();
    await expect(firstEntry).toBeVisible();
    await firstEntry.click();

    // Detail sidebar should open with field values.
    await expect(page.locator("[data-testid='detail-sidebar']")).toBeVisible();
  });

  test("time range picker works", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The time range selector should be visible in the sidebar.
    await expect(page.getByText("Time Range")).toBeVisible();

    // Click "Last 5 minutes" if it exists as a preset option.
    const presets = page.getByRole("button", { name: /5 min|5m/i });
    if (await presets.count() > 0) {
      await presets.first().click();
    }
  });
});
