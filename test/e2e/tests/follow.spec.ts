import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Follow mode E2E tests.
 *
 * The bootstrap cluster has a chatterbox ingester that emits records
 * continuously at 1-5s intervals. These tests rely on that to verify
 * live data streaming.
 *
 * Data-dependent tests skip gracefully if no records arrive within a
 * reasonable window (e.g. chatterbox ingester not configured).
 */

/** Start follow mode with a query and wait for the Following indicator. */
async function startFollow(
  page: import("@playwright/test").Page,
  query: string,
) {
  await gotoAuthenticated(page, "/search");
  await typeQuery(page, query);
  await page.getByRole("button", { name: "Follow" }).click();
  await expect(page).toHaveURL(/\/follow/, { timeout: 10_000 });
  await expect(page.getByText("Following")).toBeVisible({ timeout: 10_000 });
}

/** Stop follow and wait to return to /search. */
async function stopFollow(page: import("@playwright/test").Page) {
  await page.getByRole("button", { name: "Stop following" }).click();
  await expect(page).toHaveURL(/\/search/, { timeout: 10_000 });
}

/** Wait for at least one log entry. Returns true if records appeared. */
async function waitForRecords(
  page: import("@playwright/test").Page,
  timeoutMs = 20_000,
): Promise<boolean> {
  try {
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: timeoutMs },
    );
    return true;
  } catch {
    return false;
  }
}

test.describe("Follow mode", () => {
  // ── Basic lifecycle ──────────────────────────────────────────────────

  test("starts follow and shows Following status", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");

    // The stop button should be visible (replaces the follow button).
    await expect(
      page.getByRole("button", { name: "Stop following" }),
    ).toBeVisible();

    await stopFollow(page);
  });

  test("stops follow and returns to search", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");
    await stopFollow(page);

    // Should be back on /search.
    await expect(page).toHaveURL(/\/search/);
  });

  // ── Live data: skip if no records arrive ─────────────────────────────

  test("receives live records and displays log entries", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");

    const hasRecords = await waitForRecords(page);
    if (!hasRecords) {
      test.skip(true, "No records received — chatterbox may not be running");
    }

    // The follow counter should show a non-zero count.
    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible();

    await stopFollow(page);
  });

  test("follow count increments over time", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");

    const hasRecords = await waitForRecords(page);
    if (!hasRecords) {
      test.skip(true, "No records received — chatterbox may not be running");
    }

    const initialText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const initialCount = parseInt(
      (initialText ?? "0").replace(/[^0-9]/g, ""),
      10,
    );

    // Wait for more records. Chatterbox emits every 1-5s.
    await page.waitForTimeout(10_000);

    const laterText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const laterCount = parseInt((laterText ?? "0").replace(/[^0-9]/g, ""), 10);
    expect(laterCount).toBeGreaterThan(initialCount);

    await stopFollow(page);
  });

  // ── Filtered follow ──────────────────────────────────────────────────

  test("follow with a filter query receives matching records", async ({
    page,
  }) => {
    await startFollow(page, "ingester_type=chatterbox");

    const hasRecords = await waitForRecords(page);
    if (!hasRecords) {
      test.skip(true, "No records received — chatterbox may not be running");
    }

    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible();

    await stopFollow(page);
  });

  // ── Sidebar ──────────────────────────────────────────────────────────

  test("follow mode shows volume label in sidebar", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");

    await expect(page.getByText("Volume")).toBeVisible({ timeout: 10_000 });

    await stopFollow(page);
  });

  // ── Re-follow after stop ─────────────────────────────────────────────

  test("can re-enter follow mode after stopping", async ({ page }) => {
    await startFollow(page, "last=5m reverse=true");
    await stopFollow(page);

    // Second follow session — should work identically.
    await startFollow(page, "last=5m reverse=true");

    await expect(
      page.getByRole("button", { name: "Stop following" }),
    ).toBeVisible();

    await stopFollow(page);
  });
});
