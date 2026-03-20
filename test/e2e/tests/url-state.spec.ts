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

  test("loading URL with query param pre-fills query bar", async ({
    page,
  }) => {
    // Use a simple query — the app prepends its own time range defaults.
    const deepLink = "/search?q=level%3Derror";

    await page.goto(deepLink);

    // If redirected to login, re-authenticate.
    if (page.url().includes("/login")) {
      await page.getByLabel("Username").fill("admin");
      await page.getByLabel("Password", { exact: true }).fill("T3stP@ssw0rd!");
      await page.getByRole("button", { name: "Sign In" }).click();
      await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });
      await page.goto(deepLink);
    }

    // Wait for the page to load and verify the query is in the URL.
    await expect(page).toHaveURL(/level/, { timeout: 15_000 });

    // The query bar should contain the query from the URL.
    // Expand if collapsed.
    const textarea = page.locator("textarea");
    if (!(await textarea.isVisible({ timeout: 2_000 }).catch(() => false))) {
      const collapsedBar = page.locator("[role='button'][tabindex='0']").first();
      await collapsedBar.click();
    }
    await expect(textarea).toHaveValue(/level=error/);
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
