import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Keyboard shortcut E2E tests (gastrolog-qowgw).
 *
 * Verifies global keyboard shortcuts work regardless of current focus state.
 */

test.describe.serial("Keyboard shortcuts", () => {
  test("Escape closes settings dialog", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  test("Escape closes inspector dialog", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  test("Escape closes help dialog", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  test("Escape closes preferences dialog", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: /User menu/ }).click();
    await page.getByRole("button", { name: "Preferences" }).click();

    const dialog = page.getByRole("dialog", { name: "Preferences" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  test("Escape deselects a selected log entry", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click a log entry to select it and open the detail sidebar.
    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Press Escape to deselect — sidebar should close.
    await page.keyboard.press("Escape");
    await expect(sidebar).not.toBeVisible({ timeout: 5_000 });
  });

  test("Arrow keys navigate between log entries", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Ensure multiple entries exist.
    const entries = page.locator("[data-testid='log-entry']");
    await expect(entries.first()).toBeVisible();
    const count = await entries.count();
    if (count < 2) return; // Need at least 2 to navigate.

    // Press ArrowDown to select the first entry.
    await page.keyboard.press("ArrowDown");

    // The detail sidebar should open.
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible({ timeout: 5_000 });

    // Press ArrowDown again to move to the next entry.
    await page.keyboard.press("ArrowDown");

    // Sidebar should still be visible (different entry selected).
    await expect(sidebar).toBeVisible();

    // Clean up — Escape to deselect.
    await page.keyboard.press("Escape");
  });

  test("Enter in query bar triggers search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");

    // Press Enter in the query textarea.
    await page.locator("textarea").press("Enter");

    // Should trigger a search — result count should appear.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });
});
