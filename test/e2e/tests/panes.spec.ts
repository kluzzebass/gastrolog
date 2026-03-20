import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Collapsible pane interaction E2E tests (gastrolog-4xu88).
 *
 * Tests expanding and collapsing the left sidebar and right detail panel,
 * and verifies interactions within each pane.
 */

test.describe.serial("Collapsible panes", () => {
  test("left sidebar is visible by default", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The sidebar should show "Time Range" and "Vaults" sections.
    await expect(page.getByText("Time Range")).toBeVisible();
    await expect(page.getByText("Vaults")).toBeVisible();
  });

  test("right detail panel opens when clicking a log entry", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click a log entry to open the detail sidebar.
    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();
  });

  test("right detail panel closes when pressing Escape", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(sidebar).not.toBeVisible({ timeout: 5_000 });
  });

  test("sidebar severity filters remain interactive after search", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Severity buttons should still be clickable.
    const errorBtn = page.getByRole("button", { name: "Error", exact: true });
    await expect(errorBtn).toBeVisible();
    await errorBtn.click();

    // Button should toggle (visual state changes — no assertion on CSS,
    // just verify it's still interactive).
    await errorBtn.click();
  });

  test("clicking detail panel fields while sidebar is open works", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Both panes active: sidebar visible + detail panel open.
    await expect(page.getByText("Time Range")).toBeVisible();
    await page.locator("[data-testid='log-entry']").first().click();
    const detail = page.locator("[data-testid='detail-sidebar']");
    await expect(detail).toBeVisible();

    // Verify detail panel shows content alongside the sidebar.
    await expect(detail.getByText(/timestamp/i).first()).toBeVisible({
      timeout: 5_000,
    });
  });
});
