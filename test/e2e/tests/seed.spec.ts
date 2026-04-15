import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery, openSettingsTab } from "./helpers";

/**
 * Data seeding: ensures the setup wizard's vault has a memory tier and
 * waits for the chatterbox to generate searchable records.
 *
 * The setup wizard creates a vault + chatterbox + route but no tier.
 * Without a tier, records have nowhere to land. This seed project adds
 * a memory tier, then waits for data.
 *
 * Runs AFTER auth (setup wizard) and BEFORE app tests.
 */

test.describe.serial("Data seeding", () => {
  test("adds a memory tier to the setup wizard vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    // Find the wizard-created vault and expand it.
    // The vault name is randomly generated — find the first "no tiers configured" card.
    const vaultBtn = dialog.getByRole("button", { name: /no tiers configured/ });
    if (!(await vaultBtn.isVisible({ timeout: 5_000 }).catch(() => false))) {
      // All vaults already have tiers — nothing to do.
      return;
    }
    await vaultBtn.click();

    const addTierBtn = dialog.getByRole("button", { name: /Add Tier/i });
    await expect(addTierBtn).toBeVisible({ timeout: 5_000 });

    await addTierBtn.click();
    const memBtn = page.getByRole("button", { name: "Memory", exact: true });
    await memBtn.waitFor({ state: "visible", timeout: 5_000 });
    await memBtn.click();

    await dialog.getByRole("button", { name: "Save" }).click();

    // Wait for save to complete.
    await page.waitForTimeout(3_000);
  });

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
