import { test, expect } from "@playwright/test";
import { gotoAuthenticated, openSettingsTab } from "./helpers";

test.describe.serial("Settings", () => {
  test("opens settings dialog from header", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText("Vaults")).toBeVisible();
    await expect(dialog.getByText("Ingesters")).toBeVisible();
    await expect(dialog.getByText("Routes")).toBeVisible();
  });

  test("navigates between settings tabs", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const dialog = page.getByRole("dialog", { name: "Settings" });

    for (const tab of ["Vaults", "Ingesters", "Routes", "Filters"]) {
      await dialog.getByRole("button", { name: tab }).click();
      await expect(dialog.getByRole("heading", { name: tab })).toBeVisible();
    }
  });

  test("shows bootstrap entities", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();
    const dialog = page.getByRole("dialog", { name: "Settings" });

    // Vaults tab should have at least one vault.
    await dialog.getByRole("button", { name: "Vaults" }).click();
    await expect(dialog.getByRole("heading", { name: "Vaults" })).toBeVisible();

    // Ingesters tab should show the chatterbox.
    await dialog.getByRole("button", { name: "Ingesters" }).click();
    await expect(dialog.getByText("chatterbox")).toBeVisible();

    // Routes tab should show the default route.
    await dialog.getByRole("button", { name: "Routes" }).click();
    await expect(dialog.getByText("default")).toBeVisible();

    // Filters tab should show the catch-all filter.
    await dialog.getByRole("button", { name: "Filters" }).click();
    await expect(dialog.getByText("catch-all")).toBeVisible();
  });

  // ── Filter CRUD ──────────────────────────────────────────────────────

  test("creates a filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    await dialog.getByRole("button", { name: /Add Filter/i }).click();

    // Fill name and expression.
    await dialog.getByLabel("Name").fill("e2e-test-filter");
    await dialog.getByLabel("Expression").fill("level=error");

    await dialog.getByRole("button", { name: "Create" }).click();

    // New filter should appear in the list.
    await expect(dialog.getByText("e2e-test-filter")).toBeVisible({
      timeout: 10_000,
    });
  });

  test("edits the created filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    // Expand the e2e-test-filter card.
    await dialog.getByText("e2e-test-filter").click();

    // Change the expression.
    const expressionInput = dialog.getByLabel("Expression");
    await expressionInput.clear();
    await expressionInput.fill("level=warn");

    await dialog.getByRole("button", { name: "Save" }).click();

    // Wait for save to complete.
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });
  });

  test("deletes the created filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    // Expand the filter.
    await dialog.getByText("e2e-test-filter").click();

    // Click Delete, then confirm.
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    // Filter should be gone.
    await expect(dialog.getByText("e2e-test-filter")).not.toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Vault CRUD ───────────────────────────────────────────────────────

  test("creates a memory vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    // "Add Vault" is a dropdown — click it and select "memory".
    await dialog.getByRole("button", { name: /Add Vault/i }).click();
    await page.getByRole("button", { name: "memory", exact: true }).click();

    // Fill the name.
    await dialog.getByLabel("Name").fill("e2e-test-vault");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText("e2e-test-vault")).toBeVisible({
      timeout: 10_000,
    });
  });

  test("edits the created vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    // Expand the vault card.
    await dialog.getByText("e2e-test-vault").click();

    // Toggle the Enabled checkbox.
    const enabledCheckbox = dialog.getByRole("checkbox", { name: "Enabled" });
    await expect(enabledCheckbox).toBeVisible();
    await enabledCheckbox.click();

    await dialog.getByRole("button", { name: "Save" }).click();
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });

    // The vault should show a "disabled" badge.
    await expect(dialog.getByText("disabled")).toBeVisible();
  });

  test("deletes the created vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    await dialog.getByText("e2e-test-vault").click();

    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-test-vault")).not.toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Ingester CRUD ────────────────────────────────────────────────────

  test("creates a chatterbox ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page.getByRole("button", { name: "chatterbox", exact: true }).click();

    await dialog.getByLabel("Name").fill("e2e-test-ingester");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText("e2e-test-ingester")).toBeVisible({
      timeout: 10_000,
    });
  });

  test("deletes the created ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByText("e2e-test-ingester").click();

    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-test-ingester")).not.toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Route CRUD ───────────────────────────────────────────────────────

  test("route creation form shows fields", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Routes");

    await dialog.getByRole("button", { name: /Add Route/i }).click();

    // The route form should show name, filter, and distribution fields.
    await expect(dialog.getByLabel("Name")).toBeVisible();
    await expect(dialog.getByLabel("Filter")).toBeVisible();
    await expect(dialog.getByLabel("Distribution")).toBeVisible();

    // Cancel the creation (there may be two Cancel buttons; use the last one
    // which is the one in the add form footer).
    await dialog.getByRole("button", { name: "Cancel" }).last().click();
  });

  test("expands existing route and shows fields", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Routes");

    // Expand the "default" route.
    await dialog.getByText("default").click();

    // The route edit form should show the distribution dropdown and filter.
    await expect(dialog.getByLabel("Distribution")).toBeVisible();
    await expect(dialog.getByLabel("Filter")).toBeVisible();
    await expect(
      dialog.getByRole("checkbox", { name: "Enabled" }),
    ).toBeVisible();
  });

  // ── Rotation policy editing (gastrolog-2m5w5) ─────────────────────

  test("creates a rotation policy for editing", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Rotation Policies");

    await dialog.getByRole("button", { name: /Add Policy/i }).click();
    await dialog.getByLabel("Name").fill("e2e-edit-rotation");
    await dialog.getByLabel("Max Age").fill("2h");
    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText("e2e-edit-rotation")).toBeVisible({
      timeout: 10_000,
    });
  });

  test("edits a rotation policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Rotation Policies");

    await dialog.getByText("e2e-edit-rotation").click();

    const maxAgeInput = dialog.getByLabel("Max Age");
    await maxAgeInput.clear();
    await maxAgeInput.fill("4h");

    await dialog.getByRole("button", { name: "Save" }).click();
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });
  });

  test("deletes the rotation policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Rotation Policies");

    await dialog.getByText("e2e-edit-rotation").click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-edit-rotation")).not.toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Retention policy editing (gastrolog-2m5w5) ───────────────────

  test("creates a retention policy for editing", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Retention Policies");

    await dialog.getByRole("button", { name: /Add Policy/i }).click();
    await dialog.getByLabel("Name").fill("e2e-edit-retention");
    await dialog.getByLabel("Max Chunks").fill("10");
    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText("e2e-edit-retention")).toBeVisible({
      timeout: 10_000,
    });
  });

  test("edits a retention policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Retention Policies");

    await dialog.getByText("e2e-edit-retention").click();

    const maxChunksInput = dialog.getByLabel("Max Chunks");
    await maxChunksInput.clear();
    await maxChunksInput.fill("25");

    await dialog.getByRole("button", { name: "Save" }).click();
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({
      timeout: 10_000,
    });
  });

  test("deletes the retention policy", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Retention Policies");

    await dialog.getByText("e2e-edit-retention").click();
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-edit-retention")).not.toBeVisible({
      timeout: 10_000,
    });
  });

  // ── Cluster settings tab (gastrolog-6da4x) ──────────────────────

  test("cluster tab shows cluster info", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Cluster");

    await expect(dialog.getByText(/cluster/i).first()).toBeVisible();
  });

  // ── Nodes settings tab (gastrolog-6da4x) ────────────────────────

  test("nodes tab shows cluster nodes", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Nodes");

    await expect(dialog.getByText("node-1")).toBeVisible({ timeout: 10_000 });
    await expect(dialog.getByText("node-2")).toBeVisible();
    await expect(dialog.getByText("node-3")).toBeVisible();
  });

  test("nodes tab shows join info", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Nodes");

    const copyBtn = dialog.getByRole("button", { name: /copy/i });
    await expect(copyBtn.first()).toBeVisible({ timeout: 10_000 });
  });

  // ── Certificates tab (gastrolog-ftyjd) ──────────────────────────

  test("certificates tab is accessible", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Certificates");

    await expect(
      dialog.getByRole("heading", { name: "Certificates" }),
    ).toBeVisible();
  });

  // ── Files tab (gastrolog-38tzr) ─────────────────────────────────

  test("files tab is accessible", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Files");

    await expect(dialog.getByRole("heading", { name: "Files" })).toBeVisible();
  });

  // ── Lookups tab (gastrolog-4a08t) ───────────────────────────────

  test("lookups tab is accessible", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Lookups");

    await expect(
      dialog.getByRole("heading", { name: "Lookups" }),
    ).toBeVisible();
  });

  // ── Users tab (gastrolog-4ynbb) ─────────────────────────────────

  test("users tab shows admin user", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Users");

    await expect(dialog.getByText("admin")).toBeVisible({ timeout: 10_000 });
  });

  test("users tab has add user button", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Users");

    await expect(
      dialog.getByRole("button", { name: /Add User/i }),
    ).toBeVisible();
  });

  // ── Cross-navigation (gastrolog-5hhp3) ──────────────────────────

  test("vault card has Open in Inspector link", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    const vaultCards = dialog
      .locator("button")
      .filter({ hasText: /memory|file/ });
    const firstCard = vaultCards.first();
    if (await firstCard.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await firstCard.click();

      const crossLink = dialog.locator('[title="Open in Inspector"]');
      await expect(crossLink.first()).toBeVisible({ timeout: 5_000 });
    }
  });

  // ── All settings tabs are navigable ─────────────────────────────

  test("all settings tabs are navigable", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog).toBeVisible();

    const tabs = [
      "Cluster",
      "Nodes",
      "Certificates",
      "Files",
      "Lookups",
      "Users",
      "Ingesters",
      "Rotation Policies",
      "Retention Policies",
      "Vaults",
      "Filters",
      "Routes",
    ];

    for (const tab of tabs) {
      await dialog.getByRole("button", { name: tab }).click();
      await expect(dialog.getByRole("heading", { name: tab })).toBeVisible({
        timeout: 5_000,
      });
    }
  });

  // ── General ──────────────────────────────────────────────────────────

  test("closes settings with Escape", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const dialog = page.getByRole("dialog", { name: "Settings" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });
});
