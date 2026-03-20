import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Query history and saved queries E2E tests (gastrolog-1yzy2).
 *
 * Each test is self-contained — seeds its own data since Playwright
 * gives each test a fresh browser context (no shared localStorage).
 */

/** Expand the query bar so the toolbar icons are visible. */
async function expandQueryBar(page: import("@playwright/test").Page) {
  const textarea = page.locator("textarea");
  if (!(await textarea.isVisible({ timeout: 2_000 }).catch(() => false))) {
    const collapsedBar = page.locator("[role='button'][tabindex='0']").first();
    await collapsedBar.click();
  }
  await expect(textarea).toBeVisible({ timeout: 3_000 });
}

/** Run a search to populate history, then open the history panel. */
async function seedAndOpenHistory(
  page: import("@playwright/test").Page,
  queries: string[],
) {
  await gotoAuthenticated(page, "/search");
  for (const q of queries) {
    await typeQuery(page, q);
    await page.getByRole("button", { name: "Search" }).click();
    await page.waitForTimeout(2_000);
  }
  await expandQueryBar(page);
  await page.getByRole("button", { name: "Query history" }).click();
  await expect(page.getByText("Recent queries")).toBeVisible({
    timeout: 5_000,
  });
}

test.describe("Query history and saved queries", () => {
  // ── History tests ────────────────────────────────────────────────────

  test("running a search adds to history", async ({ page }) => {
    await seedAndOpenHistory(page, [
      "e2e_history_test_alpha",
      "e2e_history_test_beta",
    ]);

    // Both queries should appear in history.
    await expect(
      page.getByRole("button", { name: /e2e_history_test_alpha/ }).first(),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /e2e_history_test_beta/ }).first(),
    ).toBeVisible();
  });

  test("selecting a history entry fills the query bar", async ({ page }) => {
    await seedAndOpenHistory(page, ["e2e_history_select_test"]);

    await page
      .getByRole("button", { name: /e2e_history_select_test/ })
      .first()
      .click();

    // Clicking a history entry may collapse the query bar. Expand it.
    await expandQueryBar(page);
    const textarea = page.locator("textarea");
    await expect(textarea).toHaveValue(/e2e_history_select_test/);
  });

  test("removing a history entry removes it from the list", async ({
    page,
  }) => {
    await seedAndOpenHistory(page, ["e2e_history_remove_test"]);

    const entry = page
      .getByRole("button", { name: /e2e_history_remove_test.*Remove/ })
      .first();
    await entry.hover();

    const removeBtn = page
      .getByRole("button", { name: "Remove from history" })
      .first();
    await expect(removeBtn).toBeVisible();
    await removeBtn.click();

    // The history panel entry should be gone. The collapsed query bar may
    // still contain the text, so check only within the history list.
    await expect(page.getByText("Recent queries")).not.toBeVisible({
      timeout: 5_000,
    });
  });

  test("clearing history removes all entries", async ({ page }) => {
    await seedAndOpenHistory(page, ["e2e_history_clear_test"]);

    // Use exact button role to avoid matching textarea and other text.
    await page.getByRole("button", { name: "Clear", exact: true }).click();

    await expect(page.getByText("Recent queries")).not.toBeVisible({
      timeout: 5_000,
    });
  });

  // ── Saved queries ────────────────────────────────────────────────────
  // Saved queries are stored in the cluster config (not localStorage),
  // so they persist across browser contexts.

  test("saves and deletes a query", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await expandQueryBar(page);

    // Type a query first.
    await page.locator("textarea").fill("level=error");

    // Open saved queries panel and save.
    await page.getByRole("button", { name: "Saved queries" }).click();
    await expect(page.getByText("Save Current Query")).toBeVisible({
      timeout: 5_000,
    });

    const name = `E2E-${Date.now()}`;
    await page.getByLabel("Query name").fill(name);
    await page.getByRole("button", { name: "Save", exact: true }).click();

    await expect(page.getByText(name)).toBeVisible({ timeout: 5_000 });

    // Clean up: delete the saved query.
    await page.getByText(name).hover();
    const deleteBtn = page
      .getByRole("button", { name: "Delete saved query" })
      .first();
    await expect(deleteBtn).toBeVisible();
    await deleteBtn.click();
  });
});
