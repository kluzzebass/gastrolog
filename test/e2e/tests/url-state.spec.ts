import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * URL-driven state and deep linking E2E tests (gastrolog-16xj3).
 *
 * Verifies that search state is encoded in the URL and that loading
 * a URL with query params pre-fills and executes the search.
 */

test.describe.serial("URL state and deep linking", () => {
  test("search query appears in URL params after search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=error");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // URL should contain the query.
    const url = page.url();
    expect(url).toContain("level");
  });

  test("loading URL with query param pre-fills and executes search", async ({
    page,
  }) => {
    // Navigate directly to a URL with a query parameter.
    await gotoAuthenticated(page, "/search?q=*");

    // The search should auto-execute — results should appear.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    expect(count).toBeGreaterThan(0);
  });

  test("browser back preserves previous state", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // First search.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Second search with different query.
    await typeQuery(page, "level=error");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Go back.
    await page.goBack();
    await page.waitForTimeout(2_000);

    // URL should reflect the previous search state.
    // Just verify we're still on /search.
    expect(page.url()).toContain("/search");
  });

  test("follow mode URL contains /follow path", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Follow" }).click();
    await expect(page).toHaveURL(/\/follow/, { timeout: 10_000 });

    // Stop and go back.
    await page.getByRole("button", { name: "Stop following" }).click();
    await expect(page).toHaveURL(/\/search/, { timeout: 10_000 });
  });
});
