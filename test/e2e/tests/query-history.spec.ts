import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Query history and saved queries E2E tests (gastrolog-1yzy2).
 *
 * The query bar has two popover panels: history (recent queries) and
 * saved queries (named bookmarks persisted in the cluster config).
 * Both are accessed via icon buttons inside the expanded query input.
 */

/** Expand the query bar so the toolbar icons are visible. */
async function expandQueryBar(page: import("@playwright/test").Page) {
  await gotoAuthenticated(page, "/search");

  const textarea = page.locator("textarea");
  if (!(await textarea.isVisible({ timeout: 2_000 }).catch(() => false))) {
    const collapsedBar = page.locator("[role='button'][tabindex='0']").first();
    await collapsedBar.click();
  }
  await expect(textarea).toBeVisible({ timeout: 3_000 });
}

test.describe.serial("Query history and saved queries", () => {
  // ── Generate history by running a search ─────────────────────────────

  test("running a search adds to history", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "e2e_history_test_alpha");
    await page.getByRole("button", { name: "Search" }).click();

    // Wait for search to complete (even if 0 results).
    await page.waitForTimeout(2_000);

    // Run a second unique query.
    await typeQuery(page, "e2e_history_test_beta");
    await page.getByRole("button", { name: "Search" }).click();
    await page.waitForTimeout(2_000);

    // Open history popup.
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Query history" }).click();

    // History panel should show "Recent queries" header.
    await expect(page.getByText("Recent queries")).toBeVisible({
      timeout: 5_000,
    });

    // Both queries should appear in history.
    await expect(page.getByText("e2e_history_test_alpha")).toBeVisible();
    await expect(page.getByText("e2e_history_test_beta")).toBeVisible();
  });

  test("selecting a history entry fills the query bar", async ({ page }) => {
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Query history" }).click();

    await expect(page.getByText("Recent queries")).toBeVisible({
      timeout: 5_000,
    });

    // Click on the alpha entry.
    await page.getByText("e2e_history_test_alpha").click();

    // The query bar should now contain the selected query.
    const textarea = page.locator("textarea");
    await expect(textarea).toHaveValue("e2e_history_test_alpha");
  });

  test("removing a history entry removes it from the list", async ({
    page,
  }) => {
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Query history" }).click();

    await expect(page.getByText("Recent queries")).toBeVisible({
      timeout: 5_000,
    });

    // Hover over the alpha entry to reveal the remove button.
    const alphaEntry = page.getByText("e2e_history_test_alpha");
    await alphaEntry.hover();

    // Click the remove button (× with aria-label "Remove from history").
    const removeBtn = page
      .getByRole("button", { name: "Remove from history" })
      .first();
    await expect(removeBtn).toBeVisible();
    await removeBtn.click();

    // Alpha should be gone.
    await expect(page.getByText("e2e_history_test_alpha")).not.toBeVisible({
      timeout: 5_000,
    });
  });

  test("clearing history removes all entries", async ({ page }) => {
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Query history" }).click();

    await expect(page.getByText("Recent queries")).toBeVisible({
      timeout: 5_000,
    });

    // Click "Clear" to remove all history.
    await page.getByText("Clear").click();

    // History panel should disappear (no entries = component returns null).
    await expect(page.getByText("Recent queries")).not.toBeVisible({
      timeout: 5_000,
    });
  });

  // ── Saved queries ────────────────────────────────────────────────────

  test("saved queries panel shows empty state", async ({ page }) => {
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Saved queries" }).click();

    await expect(page.getByText("No saved queries yet")).toBeVisible({
      timeout: 5_000,
    });
  });

  test("saves current query with a name", async ({ page }) => {
    await expandQueryBar(page);

    // Type a query first.
    await page.locator("textarea").fill("level=error");

    // Open saved queries panel.
    await page.getByRole("button", { name: "Saved queries" }).click();
    await expect(page.getByText("Save Current Query")).toBeVisible({
      timeout: 5_000,
    });

    // Fill the name and save.
    await page.getByLabel("Query name").fill("E2E Test Query");
    await page.getByRole("button", { name: "Save" }).click();

    // The saved query should appear in the list.
    await expect(page.getByText("E2E Test Query")).toBeVisible({
      timeout: 5_000,
    });
  });

  test("loading a saved query fills the query bar", async ({ page }) => {
    await expandQueryBar(page);

    // Clear the current query.
    await page.locator("textarea").fill("");

    // Open saved queries.
    await page.getByRole("button", { name: "Saved queries" }).click();
    await expect(page.getByText("E2E Test Query")).toBeVisible({
      timeout: 5_000,
    });

    // Click the saved query to load it.
    await page.getByText("E2E Test Query").click();

    // Query bar should now contain the saved query.
    await expect(page.locator("textarea")).toHaveValue("level=error");
  });

  test("saved query persists after page reload", async ({ page }) => {
    await page.reload();
    await expect(
      page.getByRole("heading", { name: "GastroLog" }),
    ).toBeVisible({ timeout: 10_000 });

    await expandQueryBar(page);
    await page.getByRole("button", { name: "Saved queries" }).click();

    // The saved query should still be there after reload.
    await expect(page.getByText("E2E Test Query")).toBeVisible({
      timeout: 5_000,
    });
  });

  test("deletes a saved query", async ({ page }) => {
    await expandQueryBar(page);
    await page.getByRole("button", { name: "Saved queries" }).click();

    await expect(page.getByText("E2E Test Query")).toBeVisible({
      timeout: 5_000,
    });

    // Hover to reveal delete button.
    await page.getByText("E2E Test Query").hover();
    const deleteBtn = page
      .getByRole("button", { name: "Delete saved query" })
      .first();
    await expect(deleteBtn).toBeVisible();
    await deleteBtn.click();

    // Query should be gone, back to empty state.
    await expect(page.getByText("No saved queries yet")).toBeVisible({
      timeout: 5_000,
    });
  });
});
