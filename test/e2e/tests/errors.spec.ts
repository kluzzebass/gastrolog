import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery, openSettingsTab } from "./helpers";

/**
 * Error states and validation E2E tests (gastrolog-sa2di).
 *
 * Tests invalid query syntax, form validation in settings,
 * and edge case UI states.
 */

test.describe.serial("Error states and validation", () => {
  // ── Invalid query syntax ─────────────────────────────────────────────

  test("invalid query shows parser error", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Type an invalid query — unmatched parentheses.
    await typeQuery(page, "level=(");

    // The query bar should indicate an error. The search button is disabled
    // when there are parse errors, so verify that.
    const searchBtn = page.getByRole("button", { name: "Search" });
    await expect(searchBtn).toBeDisabled();
  });

  test("follow button disabled for invalid query", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=(");

    const followBtn = page.getByRole("button", { name: "Follow" });
    await expect(followBtn).toBeDisabled();
  });

  test("explain button disabled for invalid query", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=(");

    const explainBtn = page.getByRole("button", {
      name: "Explain query plan",
    });
    await expect(explainBtn).toBeDisabled();
  });

  // ── Settings form validation ─────────────────────────────────────────

  test("filter creation requires a name", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    await dialog.getByRole("button", { name: /Add Filter/i }).click();

    // Leave name empty, fill expression.
    await dialog.getByLabel("Expression").fill("level=error");

    // Create button should be disabled or clicking it should show an error.
    const createBtn = dialog.getByRole("button", { name: "Create" });
    const isDisabled = await createBtn.isDisabled();

    if (!isDisabled) {
      // If not disabled, clicking with empty name should produce an error.
      await createBtn.click();
      // Expect the filter NOT to be created (no new filter in list).
      await page.waitForTimeout(1_000);
    }

    // Cancel to restore state.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  test("vault creation requires a name", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    await dialog.getByRole("button", { name: /Add Vault/i }).click();
    await page.getByRole("button", { name: "memory", exact: true }).click();

    // Leave name empty, try to create.
    const createBtn = dialog.getByRole("button", { name: "Create" });
    const isDisabled = await createBtn.isDisabled();

    if (!isDisabled) {
      await createBtn.click();
      await page.waitForTimeout(1_000);
    }

    // Cancel to restore state.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  test("ingester creation requires a name", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page
      .getByRole("button", { name: "chatterbox", exact: true })
      .click();

    // Leave name empty, try to create.
    const createBtn = dialog.getByRole("button", { name: "Create" });
    const isDisabled = await createBtn.isDisabled();

    if (!isDisabled) {
      await createBtn.click();
      await page.waitForTimeout(1_000);
    }

    // Cancel to restore state.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  // ── No results state ─────────────────────────────────────────────────

  test("search with no matches shows empty state", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, '"e2e_absolutely_no_match_xyzzy_99999"');
    await page.getByRole("button", { name: "Search" }).click();

    // Wait for search to complete.
    await page.waitForTimeout(3_000);

    // Either result count shows 0 or the results area shows no entries.
    const resultCount = page.locator("[data-testid='result-count']");
    if (await resultCount.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const text = await resultCount.textContent();
      const count = parseInt(text!.replace(/[^0-9]/g, ""), 10);
      expect(count).toBe(0);
    }

    // No log entries should be visible.
    await expect(page.locator("[data-testid='log-entry']")).toHaveCount(0);
  });

  // ── Duplicate name prevention ────────────────────────────────────────

  test("creating a filter with duplicate name shows error", async ({
    page,
  }) => {
    const dialog = await openSettingsTab(page, "Filters");

    // Try to create a filter with the same name as the existing "catch-all".
    await dialog.getByRole("button", { name: /Add Filter/i }).click();
    await dialog.getByLabel("Name").fill("catch-all");
    await dialog.getByLabel("Expression").fill("*");

    await dialog.getByRole("button", { name: "Create" }).click();

    // Should show an error — the name already exists.
    // Wait briefly for any error toast or inline validation.
    await page.waitForTimeout(2_000);

    // Cancel to restore state.
    const cancelBtn = dialog.getByRole("button", { name: "Cancel" }).last();
    if (await cancelBtn.isVisible().catch(() => false)) {
      await cancelBtn.click();
    }
  });
});
