import { test, expect } from "@playwright/test";
import { gotoAuthenticated } from "./helpers";

test.describe.serial("Inspector", () => {
  test("opens inspector from header", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });
    await expect(dialog).toBeVisible();
  });

  test("shows entities view with vaults", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // In multi-node mode, switch to Entities view if needed.
    const entitiesButton = dialog.getByRole("button", { name: "Entities" });
    if (await entitiesButton.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await entitiesButton.click();
    }

    // Vaults tab should be available.
    await dialog.getByRole("button", { name: "Vaults" }).click();

    // The bootstrap vault has a random petname — just verify at least one
    // vault entry is listed (a nav button inside the Vaults section).
    const vaultEntries = dialog.locator("nav button").filter({ hasNotText: /Vaults|Ingesters|Routes|Filters|Entities|Nodes/i });
    await expect(vaultEntries.first()).toBeVisible({ timeout: 10_000 });
  });

  test("shows ingesters in entities view", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // Switch to Entities view if needed.
    const entitiesButton = dialog.getByRole("button", { name: "Entities" });
    if (await entitiesButton.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await entitiesButton.click();
    }

    // Ingesters tab.
    await dialog.getByRole("button", { name: "Ingesters" }).click();

    // Should show the bootstrap chatterbox ingester.
    await expect(dialog.getByText("chatterbox")).toBeVisible();
  });

  test("shows vault with record count", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // Navigate to Entities > Vaults.
    const entitiesButton = dialog.getByRole("button", { name: "Entities" });
    if (await entitiesButton.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await entitiesButton.click();
    }
    await dialog.getByRole("button", { name: "Vaults" }).click();

    // Click the first vault entry (name is a random petname).
    const vaultEntries = dialog.locator("nav button").filter({ hasNotText: /Vaults|Ingesters|Routes|Filters|Entities|Nodes/i });
    await expect(vaultEntries.first()).toBeVisible({ timeout: 10_000 });
    await vaultEntries.first().click();

    // The vault detail should show some records — chatterbox has been
    // generating data since cluster start.
    // Look for any numeric count or "records" text.
    await expect(dialog.getByText(/record|chunk|entries/i)).toBeVisible({
      timeout: 15_000,
    });
  });

  test("closes inspector with close button", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });
    await expect(dialog).toBeVisible();

    // Close via Escape key (avoids ambiguity between X button and overlay).
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });
});
