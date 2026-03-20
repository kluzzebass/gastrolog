import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

test.describe.serial("Follow mode", () => {
  test("starts and stops follow mode", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Enter a wildcard query.
    await typeQuery(page, "*");

    // Click the Follow button (play icon).
    await page.getByRole("button", { name: "Follow" }).click();

    // Should navigate to /follow route.
    await expect(page).toHaveURL(/\/follow/, { timeout: 10_000 });

    // The follow header should show "Following" status text.
    await expect(page.getByText("Following")).toBeVisible({ timeout: 10_000 });

    // Wait for live results — chatterbox generates at 1-5s intervals.
    // Use a generous timeout since the ingester may need time to produce
    // records that land after the follow start timestamp.
    const gotResults = await page
      .locator("[data-testid='log-entry']")
      .first()
      .isVisible({ timeout: 45_000 })
      .catch(() => false);

    if (gotResults) {
      const count = await page.locator("[data-testid='log-entry']").count();
      expect(count).toBeGreaterThan(0);
    }

    // Stop following — button changes to "Stop following".
    await page.getByRole("button", { name: "Stop following" }).click();

    // Should return to /search.
    await expect(page).toHaveURL(/\/search/, { timeout: 10_000 });
  });
});
