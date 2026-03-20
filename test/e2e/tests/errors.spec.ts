import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery, openSettingsTab } from "./helpers";

/**
 * Error states and validation E2E tests (gastrolog-sa2di).
 *
 * Tests invalid query syntax, form validation in settings,
 * and edge case UI states.
 */

test.describe("Error states and validation", () => {
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

  test("follow button visible for invalid query", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=(");

    // Follow remains enabled for invalid queries (like Explain).
    // Only the Search button is disabled on parse errors.
    const followBtn = page.getByRole("button", { name: "Follow" });
    await expect(followBtn).toBeVisible();
  });

  test("explain button visible for invalid query", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=(");

    // The Explain button remains enabled even for invalid queries
    // (explanation can describe the parse error). Search/Follow are disabled.
    const explainBtn = page.getByRole("button", {
      name: "Explain query plan",
    });
    await expect(explainBtn).toBeVisible();
  });

  // ── Settings form validation ─────────────────────────────────────────
  // All entity creation forms (filter, vault, ingester) auto-generate a
  // placeholder name, so "Create" is enabled even with an empty name field.

  test("filter creation with empty name uses generated name", async ({
    page,
  }) => {
    const dialog = await openSettingsTab(page, "Filters");

    await dialog.getByRole("button", { name: /Add Filter/i }).click();
    await dialog.getByLabel("Expression").fill("level=error");

    // Create button should be enabled (generated placeholder name is used).
    const createBtn = dialog.getByRole("button", { name: "Create" });
    await expect(createBtn).toBeEnabled({ timeout: 5_000 });

    // Cancel without creating.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  test("vault creation with empty name uses generated name", async ({
    page,
  }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    await dialog.getByRole("button", { name: /Add Vault/i }).click();
    await page.getByRole("button", { name: "memory", exact: true }).click();

    // Create button should be enabled (generated placeholder name is used).
    const createBtn = dialog.getByRole("button", { name: "Create" });
    await expect(createBtn).toBeEnabled({ timeout: 5_000 });

    // Cancel without creating.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  test("ingester creation with empty name uses generated name", async ({
    page,
  }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page.getByRole("button", { name: "chatterbox", exact: true }).click();

    // Create button should be enabled (generated placeholder name is used).
    const createBtn = dialog.getByRole("button", { name: "Create" });
    await expect(createBtn).toBeEnabled({ timeout: 5_000 });

    // Cancel without creating.
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

  test("duplicate filter name disables create button", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    await dialog.getByRole("button", { name: /Add Filter/i }).click();
    await dialog.getByLabel("Name").fill("catch-all");
    await dialog.getByLabel("Expression").fill("*");

    // The Create button should be disabled because "catch-all" already exists.
    const createBtn = dialog.getByRole("button", { name: "Create" });
    await expect(createBtn).toBeDisabled();

    // Cancel to restore state.
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });
});
