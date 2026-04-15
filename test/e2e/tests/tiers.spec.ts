import { test, expect } from "@playwright/test";
import {
  gotoAuthenticated,
  openSettingsTab,
  openSettingsTabOnNode,
  NODE_URLS,
} from "./helpers";

// ---------------------------------------------------------------------------
// Tier management E2E tests
//
// Tests vault tier creation, removal (drain/delete), reordering, and
// cross-node propagation. These run against the live 3-node cluster.
// ---------------------------------------------------------------------------

test.describe.serial("Tier management", () => {
  const VAULT_NAME = "e2e-tier-test";

  // ── Tier creation via vault settings ───────────────────────────────

  test("creates a vault with a memory tier", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    // Idempotent: skip if vault already exists (retry after prior failure).
    if (await dialog.getByText(VAULT_NAME).isVisible().catch(() => false)) {
      return;
    }

    // Open the vault creation form, then add a memory tier.
    await dialog.getByRole("button", { name: /Add Vault/i }).click();
    await dialog.getByRole("button", { name: /Add Tier/i }).click();
    const memBtn = page.getByRole("button", { name: "Memory", exact: true });
    await memBtn.waitFor({ state: "visible", timeout: 5_000 });
    await memBtn.click();

    await dialog.getByLabel("Name").fill(VAULT_NAME);

    // The vault should have one auto-added memory tier.
    await expect(dialog.getByText("Memory").first()).toBeVisible();

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(VAULT_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  test("adds a second memory tier to the vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    // Expand the vault card.
    await dialog.getByText(VAULT_NAME).click();

    // Click "+ Add Tier" and select "Memory".
    await dialog.getByRole("button", { name: /Add Tier/i }).click();
    const memBtn2 = page.getByRole("button", { name: "Memory", exact: true });
    await memBtn2.waitFor({ state: "visible", timeout: 5_000 });
    await memBtn2.click();

    // Verify tier was added: the Save button should now be enabled
    // (dirty state from adding a tier).
    await expect(
      dialog.getByRole("button", { name: "Save" }),
    ).toBeEnabled({ timeout: 5_000 });

    await dialog.getByRole("button", { name: "Save" }).click();

    // Wait for save to complete.
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });
  });

  test("vault shows both tiers after save", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();

    // Both tiers should be visible.
    const memoryBadges = dialog.getByText("Memory");
    await expect(memoryBadges.first()).toBeVisible({ timeout: 5_000 });
    // Count tier rows — should have at least 2 numbered entries.
    const tierNumbers = dialog.getByRole("button", { name: "Remove tier" });
    await expect(tierNumbers).toHaveCount(2, { timeout: 5_000 });
  });

  // ── Tier removal with Drain/Delete/Cancel ─────────────────────────

  test("tier remove button shows Drain/Delete/Cancel options", async ({
    page,
  }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();

    // Click the "Remove" button on the first tier.
    const removeButtons = dialog.getByRole("button", { name: "Remove" });
    await removeButtons.first().click();

    // Should show the confirmation prompt with Drain/Delete/Cancel.
    const prompt = dialog.getByText("Remove tier?");
    await expect(prompt).toBeVisible({ timeout: 5_000 });

    // Scope to the prompt's parent to avoid matching buttons elsewhere.
    const promptSection = prompt.locator("../..");
    await expect(promptSection.getByRole("button", { name: "Drain" })).toBeVisible();
    await expect(promptSection.getByRole("button", { name: "Delete" })).toBeVisible();
    await expect(promptSection.getByRole("button", { name: "Cancel" })).toBeVisible();
  });

  test("cancel dismisses the tier removal prompt", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();

    const removeButtons = dialog.getByRole("button", { name: "Remove" });
    await removeButtons.first().click();

    const prompt1 = dialog.getByText("Remove tier?");
    await expect(prompt1).toBeVisible();
    const promptSection1 = prompt1.locator("../..");
    await promptSection1.getByRole("button", { name: "Cancel" }).click();

    // Prompt should be dismissed.
    await expect(prompt1).not.toBeVisible();
  });

  test("delete removes a tier immediately on save", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();

    // Remove the second tier (Delete — no drain).
    const removeButtons = dialog.getByRole("button", { name: "Remove" });
    await removeButtons.last().click();

    const prompt2 = dialog.getByText("Remove tier?");
    await expect(prompt2).toBeVisible();
    const promptSection2 = prompt2.locator("../..");
    await promptSection2.getByRole("button", { name: "Delete" }).click();

    // Save the vault.
    await dialog.getByRole("button", { name: "Save" }).click();
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });

    // Re-expand and verify only 1 tier remains.
    await dialog.getByText(VAULT_NAME).click();
    const tierNumbers = dialog.getByRole("button", { name: "Remove tier" });
    await expect(tierNumbers).toHaveCount(1, { timeout: 5_000 });
  });

  // ── Cross-node tier visibility ────────────────────────────────────

  test("new tier is visible on node-2 after creation", async ({ page }) => {
    // First, add a second tier on node-1.
    const dialog1 = await openSettingsTab(page, "Vaults");
    await dialog1.getByText(VAULT_NAME).click();

    await dialog1.getByRole("button", { name: /Add Tier/i }).click();
    await page
      .getByRole("button", { name: "Memory", exact: true })
      .click();

    await dialog1.getByRole("button", { name: "Save" }).click();
    await expect(
      dialog1.getByRole("button", { name: "Save" }),
    ).toBeVisible({ timeout: 10_000 });

    // Now check node-2 — the vault should have 2 tiers.
    const dialog2 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Vaults",
    );

    await dialog2.getByText(VAULT_NAME).click();

    const tierNumbers = dialog2.getByRole("button", { name: "Remove tier" });
    await expect(tierNumbers).toHaveCount(2, { timeout: 15_000 });
  });

  // ── Inspector shows vault with tiers ────────────────────────────────

  test("inspector shows the vault in entities view", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });
    await expect(dialog).toBeVisible();

    // Switch to Entities mode and select Vaults.
    const entitiesBtn = dialog.getByRole("button", { name: "Entities" });
    if (await entitiesBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await entitiesBtn.click();
    }
    await dialog.getByRole("button", { name: "Vaults" }).click();

    // The test vault should appear in the vault list.
    await expect(dialog.getByText(VAULT_NAME)).toBeVisible({
      timeout: 10_000,
    });

    // Click on it — the detail pane should show chunk table headers.
    await dialog.getByText(VAULT_NAME).click();
    await expect(dialog.getByText("Records").first()).toBeVisible({
      timeout: 15_000,
    });
  });

  // ── Cleanup ───────────────────────────────────────────────────────

  test("deletes the test vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();

    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText(VAULT_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });
});
