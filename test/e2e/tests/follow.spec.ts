import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Follow mode E2E tests.
 *
 * The bootstrap cluster has a chatterbox ingester that emits records
 * continuously at 1-5s intervals. These tests rely on that to verify
 * live data streaming.
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

/** Read the numeric follow count from the result-count badge. */
async function getFollowCount(
  page: import("@playwright/test").Page,
): Promise<number> {
  const text = await page.locator("[data-testid='result-count']").textContent();
  return parseInt((text ?? "0").replace(/[^0-9]/g, ""), 10);
}

test.describe.serial("Follow mode", () => {
  // ── Basic lifecycle ──────────────────────────────────────────────────

  test("starts follow and shows Following status", async ({ page }) => {
    await startFollow(page, "*");

    // The stop button should be visible (replaces the follow button).
    await expect(
      page.getByRole("button", { name: "Stop following" }),
    ).toBeVisible();

    await stopFollow(page);
  });

  test("stops follow and returns to search", async ({ page }) => {
    await startFollow(page, "*");
    await stopFollow(page);

    // Should be back on /search.
    await expect(page).toHaveURL(/\/search/);
  });

  // ── Live data: hard assertions ───────────────────────────────────────

  test("receives live records and displays log entries", async ({ page }) => {
    await startFollow(page, "*");

    // Hard assert: at least one log entry MUST appear within 45s.
    // Chatterbox emits every 1-5s, so this is generous.
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: 45_000 },
    );

    // The follow counter should show a non-zero count.
    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible();

    const count = await getFollowCount(page);
    expect(count).toBeGreaterThan(0);

    await stopFollow(page);
  });

  test("follow count increments over time", async ({ page }) => {
    await startFollow(page, "*");

    // Wait for initial records.
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: 45_000 },
    );

    const initialCount = await getFollowCount(page);

    // Wait for more records to arrive. Chatterbox emits every 1-5s,
    // so 15s should produce several more.
    await page.waitForTimeout(15_000);

    const laterCount = await getFollowCount(page);
    expect(laterCount).toBeGreaterThan(initialCount);

    await stopFollow(page);
  });

  // ── Filtered follow ──────────────────────────────────────────────────

  test("follow with a filter query receives matching records", async ({
    page,
  }) => {
    // Chatterbox records contain "chatterbox_type" field. Follow with
    // a query that matches chatterbox output specifically.
    await startFollow(page, "ingester_type=chatterbox");

    // Records should still arrive since chatterbox is the active ingester.
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: 45_000 },
    );

    const count = await getFollowCount(page);
    expect(count).toBeGreaterThan(0);

    await stopFollow(page);
  });

  // ── Sidebar ──────────────────────────────────────────────────────────

  test("follow mode shows volume label in sidebar", async ({ page }) => {
    await startFollow(page, "*");

    await expect(page.getByText("Volume")).toBeVisible({ timeout: 10_000 });

    await stopFollow(page);
  });

  // ── Re-follow after stop ─────────────────────────────────────────────

  test("can re-enter follow mode after stopping", async ({ page }) => {
    // First follow session.
    await startFollow(page, "*");
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: 45_000 },
    );
    await stopFollow(page);

    // Second follow session — should work identically.
    await startFollow(page, "*");
    await expect(page.locator("[data-testid='log-entry']").first()).toBeVisible(
      { timeout: 45_000 },
    );

    const count = await getFollowCount(page);
    expect(count).toBeGreaterThan(0);

    await stopFollow(page);
  });
});
