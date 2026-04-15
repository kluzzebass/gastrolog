import { test, expect } from "@playwright/test";
import { typeQuery, openSettingsTab } from "./helpers";

/**
 * Full pipeline E2E: creates every entity from scratch, wires them together,
 * then verifies that data actually flows through the pipeline into the vault.
 *
 * Steps:
 *   1. Create a rotation policy
 *   2. Create a retention policy
 *   3. Create a filter with a specific expression
 *   4. Create a memory vault referencing the rotation policy
 *   5. Create a route using the filter and pointing to the vault
 *   6. Create a scatterbox ingester (one-shot mode)
 *   7. Trigger the scatterbox to emit records
 *   8. Search and verify that records landed in the new vault
 *   9. Cleanup: delete all created entities
 */

const PREFIX = "e2e-pipeline";
const ROTATION_NAME = `${PREFIX}-rotation`;
const RETENTION_NAME = `${PREFIX}-retention`;
const FILTER_NAME = `${PREFIX}-filter`;
const VAULT_NAME = `${PREFIX}-vault`;
const ROUTE_NAME = `${PREFIX}-route`;
const INGESTER_NAME = `${PREFIX}-scatter`;

test.describe.serial("Pipeline: full ingestion workflow", () => {
  // ── Step 1: Create rotation policy ───────────────────────────────────

  test("creates a rotation policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Rotation Policies");

    // Idempotent: skip creation if the policy already exists (e.g. retry
    // after a prior test in the serial chain already created it).
    if (await dialog.getByText(ROTATION_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Policy/i }).click();

    await dialog.getByLabel("Name").fill(ROTATION_NAME);
    await dialog.getByLabel("Max Age").fill("1h");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(ROTATION_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 2: Create retention policy ──────────────────────────────────

  test("creates a retention policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Retention Policies");

    if (await dialog.getByText(RETENTION_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Policy/i }).click();

    await dialog.getByLabel("Name").fill(RETENTION_NAME);
    await dialog.getByLabel("Max Chunks").fill("50");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(RETENTION_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 3: Create filter ────────────────────────────────────────────

  test("creates a filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    if (await dialog.getByText(FILTER_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Filter/i }).click();

    await dialog.getByLabel("Name").fill(FILTER_NAME);
    // Match scatterbox records by their ingester_type attribute.
    await dialog.getByLabel("Expression").fill("ingester_type=scatterbox");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(FILTER_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 4: Create vault with rotation policy ────────────────────────

  test("creates a memory vault with rotation policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    if (await dialog.getByText(VAULT_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Vault/i }).click();
    await dialog.getByRole("button", { name: /Add Tier/i }).click();
    const memBtn = page.getByRole("button", { name: "Memory", exact: true });
    await memBtn.waitFor({ state: "visible", timeout: 5_000 });
    await memBtn.click();

    await dialog.getByLabel("Name").fill(VAULT_NAME);

    // Select the rotation policy we created.
    await dialog.getByLabel("Rotation Policy").selectOption({
      label: ROTATION_NAME,
    });

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(VAULT_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 5: Create route with filter → vault ─────────────────────────

  test("creates a route pointing to the vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Routes");

    if (await dialog.getByText(ROUTE_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Route/i }).click();

    await dialog.getByLabel("Name").fill(ROUTE_NAME);

    // Select the filter.
    await dialog.getByLabel("Filter").selectOption({
      label: FILTER_NAME,
    });

    // Distribution defaults to Fanout — leave it.

    // Add the vault as a destination.
    await dialog.getByLabel("Destinations").selectOption({
      label: VAULT_NAME,
    });

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(ROUTE_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 6: Create scatterbox ingester in one-shot mode ──────────────

  test("creates a scatterbox ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    if (await dialog.getByText(INGESTER_NAME).isVisible().catch(() => false)) {
      return;
    }

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page
      .getByRole("button", { name: "scatterbox", exact: true })
      .click();

    await dialog.getByLabel("Name").fill(INGESTER_NAME);

    // One-shot mode: interval=0 so it only emits on trigger.
    await dialog.getByLabel("Interval").fill("0s");
    await dialog.getByLabel("Burst").fill("10");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText(INGESTER_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Step 7: Trigger scatterbox and verify data ───────────────────────

  test("triggers scatterbox and verifies records in vault", async ({
    page,
  }) => {
    // Open the ingester card and trigger emission.
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByText(INGESTER_NAME).click();

    // In one-shot mode the button says "Emit Burst".
    const triggerBtn = dialog.getByRole("button", {
      name: /Emit Burst/i,
    });
    await expect(triggerBtn).toBeVisible({ timeout: 10_000 });

    // Trigger a few times to ensure records flow through the pipeline.
    await triggerBtn.click();
    await page.waitForTimeout(500);
    await triggerBtn.click();
    await page.waitForTimeout(500);
    await triggerBtn.click();

    // Close settings.
    await page.keyboard.press("Escape");

    // Wait for records to be ingested and indexed.
    await page.waitForTimeout(3_000);

    // Search for scatterbox records — this attribute is unique to scatterbox,
    // so any results prove the full pipeline (ingester → route → vault → search).
    await typeQuery(page, "ingester_type=scatterbox");
    await page.getByRole("button", { name: "Search" }).click();

    // Verify records appeared.
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    expect(countText).toBeTruthy();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    // We triggered 3 times with burst=10, so expect at least some records.
    // The exact count depends on routing — at minimum we should see records.
    expect(count).toBeGreaterThan(0);

    // Verify a log entry is visible and contains scatterbox JSON structure.
    const firstEntry = page.locator("[data-testid='log-entry']").first();
    await expect(firstEntry).toBeVisible();

    // Click to open detail sidebar — verify seq field exists (scatterbox signature).
    await firstEntry.click();
    const detailSidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(detailSidebar).toBeVisible();
    // Scatterbox records contain "seq" in their JSON body — just verify
    // at least one instance is visible (multiple matches are expected).
    await expect(detailSidebar.getByText(/seq/).first()).toBeVisible({
      timeout: 5_000,
    });
  });

  // ── Cleanup: delete all created entities in reverse order ────────────

  test("cleans up: deletes ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");
    await dialog.getByText(INGESTER_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(INGESTER_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes route", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Routes");
    await dialog.getByText(ROUTE_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(ROUTE_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");
    await dialog.getByText(VAULT_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(VAULT_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");
    await dialog.getByText(FILTER_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(FILTER_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes retention policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Retention Policies");
    await dialog.getByText(RETENTION_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(RETENTION_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes rotation policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Rotation Policies");
    await dialog.getByText(ROTATION_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(ROTATION_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });
});
