import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

/**
 * Data seeding: waits for the setup wizard's chatterbox to generate
 * searchable records before the rest of the e2e suite runs. The wizard
 * creates a fully-shaped vault, route, and chatterbox in one step, so
 * no extra setup is needed here — only a wait.
 *
 * Runs AFTER auth (setup wizard) and BEFORE app tests.
 */

test.describe.serial("Data seeding", () => {
  test("waits for searchable data", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Retry search until records appear.
    for (let attempt = 0; attempt < 15; attempt++) {
      await typeQuery(page, "last=5m reverse=true");
      await page.getByRole("button", { name: "Search" }).click();

      const resultCount = page.locator("[data-testid='result-count']");
      if (await resultCount.isVisible({ timeout: 10_000 }).catch(() => false)) {
        const text = await resultCount.textContent();
        const count = parseInt(text?.replace(/[^0-9]/g, "") ?? "0", 10);
        if (count > 0) {
          return;
        }
      }

      await page.waitForTimeout(2_000);
    }

    // Final assertion.
    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible({ timeout: 10_000 });
    const text = await resultCount.textContent();
    const count = parseInt(text?.replace(/[^0-9]/g, "") ?? "0", 10);
    expect(count).toBeGreaterThan(0);
  });
});
