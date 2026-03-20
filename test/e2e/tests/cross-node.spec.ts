import { test, expect } from "@playwright/test";
import {
  NODE_URLS,
  openSettingsTabOnNode,
  gotoNode,
  typeQuery,
} from "./helpers";

/**
 * Cross-node config propagation tests.
 *
 * Verifies that config changes made via one node are visible when
 * querying through a different node. This exercises the WatchConfig
 * push mechanism and Raft-replicated FSM.
 *
 * The E2E cluster exposes three nodes:
 *   node-1: localhost:14564 (default)
 *   node-2: localhost:14574
 *   node-3: localhost:14584
 */

const PREFIX = "e2e-xnode";
const VAULT_NAME = `${PREFIX}-vault`;
const FILTER_NAME = `${PREFIX}-filter`;
const ROUTE_NAME = `${PREFIX}-route`;
const INGESTER_NAME = `${PREFIX}-scatter`;

test.describe.serial("Cross-node config propagation", () => {
  // ── Config push: create on node-1, verify on node-2 ─────────────────

  test("vault created on node-1 appears on node-2", async ({ page }) => {
    // Create vault on node-1.
    const dialog1 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Vaults",
    );

    await dialog1.getByRole("button", { name: /Add Vault/i }).click();
    await page.getByRole("button", { name: "memory", exact: true }).click();
    await dialog1.getByLabel("Name").fill(VAULT_NAME);
    await dialog1.getByRole("button", { name: "Create" }).click();
    await expect(dialog1.getByText(VAULT_NAME)).toBeVisible({
      timeout: 10_000,
    });

    // Navigate to node-2 and verify the vault is visible.
    const dialog2 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Vaults",
    );
    await expect(dialog2.getByText(VAULT_NAME)).toBeVisible({
      timeout: 15_000,
    });
  });

  // ── Config push: edit on node-2, verify on node-3 ───────────────────

  test("vault edited on node-2 reflects on node-3", async ({ page }) => {
    // Edit vault on node-2: disable it.
    const dialog2 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Vaults",
    );
    await dialog2.getByText(VAULT_NAME).click();
    await dialog2.getByRole("checkbox", { name: "Enabled" }).click();
    await dialog2.getByRole("button", { name: "Save" }).click();
    await expect(dialog2.getByText("disabled")).toBeVisible({
      timeout: 10_000,
    });

    // Verify on node-3.
    const dialog3 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node3,
      "Vaults",
    );
    await dialog3.getByText(VAULT_NAME).click();
    const checkbox = dialog3.getByRole("checkbox", { name: "Enabled" });
    await expect(checkbox).toBeVisible();
    await expect(checkbox).not.toBeChecked();
  });

  // ── Config push: re-enable on node-3, verify on node-1 ──────────────

  test("vault re-enabled on node-3 reflects on node-1", async ({ page }) => {
    const dialog3 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node3,
      "Vaults",
    );
    await dialog3.getByText(VAULT_NAME).click();
    await dialog3.getByRole("checkbox", { name: "Enabled" }).click();
    await dialog3.getByRole("button", { name: "Save" }).click();

    // "disabled" badge should disappear.
    await expect(dialog3.getByText("disabled")).not.toBeVisible({
      timeout: 10_000,
    });

    // Verify on node-1.
    const dialog1 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Vaults",
    );
    await dialog1.getByText(VAULT_NAME).click();
    const checkbox = dialog1.getByRole("checkbox", { name: "Enabled" });
    await expect(checkbox).toBeVisible();
    await expect(checkbox).toBeChecked();
  });

  // ── Full cross-node pipeline: ingest on node-1, search from node-2 ──

  test("creates filter and route on node-1", async ({ page }) => {
    // Create filter.
    const filterDialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Filters",
    );
    await filterDialog.getByRole("button", { name: /Add Filter/i }).click();
    await filterDialog.getByLabel("Name").fill(FILTER_NAME);
    await filterDialog
      .getByLabel("Expression")
      .fill("ingester_type=scatterbox");
    await filterDialog.getByRole("button", { name: "Create" }).click();
    await expect(filterDialog.getByText(FILTER_NAME)).toBeVisible({
      timeout: 10_000,
    });

    // Create route.
    const routeDialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Routes",
    );
    await routeDialog.getByRole("button", { name: /Add Route/i }).click();
    await routeDialog.getByLabel("Name").fill(ROUTE_NAME);
    await routeDialog
      .getByLabel("Filter")
      .selectOption({ label: FILTER_NAME });
    await routeDialog
      .getByLabel("Destinations")
      .selectOption({ label: VAULT_NAME });
    await routeDialog.getByRole("button", { name: "Create" }).click();
    await expect(routeDialog.getByText(ROUTE_NAME)).toBeVisible({
      timeout: 10_000,
    });
  });

  test("creates scatterbox ingester on node-2 and triggers it", async ({
    page,
  }) => {
    const dialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Ingesters",
    );

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page
      .getByRole("button", { name: "scatterbox", exact: true })
      .click();
    await dialog.getByLabel("Name").fill(INGESTER_NAME);
    await dialog.getByLabel("Interval").fill("0s");
    await dialog.getByLabel("Burst").fill("10");
    await dialog.getByRole("button", { name: "Create" }).click();
    await expect(dialog.getByText(INGESTER_NAME)).toBeVisible({
      timeout: 10_000,
    });

    // Trigger bursts.
    await dialog.getByText(INGESTER_NAME).click();
    const triggerBtn = dialog.getByRole("button", { name: /Emit Burst/i });
    await expect(triggerBtn).toBeVisible({ timeout: 10_000 });
    await triggerBtn.click();
    await page.waitForTimeout(500);
    await triggerBtn.click();
    await page.waitForTimeout(500);
    await triggerBtn.click();
  });

  test("searches from node-3 and finds records ingested via node-2", async ({
    page,
  }) => {
    // Wait for ingestion and indexing.
    await page.waitForTimeout(3_000);

    await gotoNode(page, NODE_URLS.node3, "/search");
    await typeQuery(page, "ingester_type=scatterbox");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    expect(countText).toBeTruthy();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    expect(count).toBeGreaterThan(0);
  });

  // ── Cleanup: delete everything, verify cross-node ───────────────────

  test("cleans up: deletes ingester from node-2", async ({ page }) => {
    const dialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Ingesters",
    );
    await dialog.getByText(INGESTER_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(INGESTER_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes route from node-3", async ({ page }) => {
    const dialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node3,
      "Routes",
    );
    await dialog.getByText(ROUTE_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(ROUTE_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes filter from node-1", async ({ page }) => {
    const dialog = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Filters",
    );
    await dialog.getByText(FILTER_NAME).click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();
    await expect(dialog.getByText(FILTER_NAME)).not.toBeVisible({
      timeout: 10_000,
    });
  });

  test("cleans up: deletes vault from node-1, verifies gone on node-2", async ({
    page,
  }) => {
    // Delete on node-1.
    const dialog1 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node1,
      "Vaults",
    );
    await dialog1.getByText(VAULT_NAME).click();
    await dialog1.getByRole("button", { name: "Delete" }).click();
    await dialog1.getByRole("button", { name: "Yes" }).click();
    await expect(dialog1.getByText(VAULT_NAME)).not.toBeVisible({
      timeout: 10_000,
    });

    // Verify deletion propagated to node-2.
    const dialog2 = await openSettingsTabOnNode(
      page,
      NODE_URLS.node2,
      "Vaults",
    );
    await expect(dialog2.getByText(VAULT_NAME)).not.toBeVisible({
      timeout: 15_000,
    });
  });
});
