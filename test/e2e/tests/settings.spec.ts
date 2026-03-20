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
    await expect(dialog.getByText("e2e-test-filter")).toBeVisible({ timeout: 10_000 });
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
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({ timeout: 10_000 });
  });

  test("deletes the created filter", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Filters");

    // Expand the filter.
    await dialog.getByText("e2e-test-filter").click();

    // Click Delete, then confirm.
    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    // Filter should be gone.
    await expect(dialog.getByText("e2e-test-filter")).not.toBeVisible({ timeout: 10_000 });
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

    await expect(dialog.getByText("e2e-test-vault")).toBeVisible({ timeout: 10_000 });
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
    await expect(dialog.getByRole("button", { name: "Save" })).toBeVisible({ timeout: 10_000 });

    // The vault should show a "disabled" badge.
    await expect(dialog.getByText("disabled")).toBeVisible();
  });

  test("deletes the created vault", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Vaults");

    await dialog.getByText("e2e-test-vault").click();

    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-test-vault")).not.toBeVisible({ timeout: 10_000 });
  });

  // ── Ingester CRUD ────────────────────────────────────────────────────

  test("creates a chatterbox ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByRole("button", { name: /Add Ingester/i }).click();
    await page.getByRole("button", { name: "chatterbox", exact: true }).click();

    await dialog.getByLabel("Name").fill("e2e-test-ingester");

    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(dialog.getByText("e2e-test-ingester")).toBeVisible({ timeout: 10_000 });
  });

  test("deletes the created ingester", async ({ page }) => {
    const dialog = await openSettingsTab(page, "Ingesters");

    await dialog.getByText("e2e-test-ingester").click();

    await dialog.getByRole("button", { name: "Delete" }).click();
    await dialog.getByRole("button", { name: "Yes" }).click();

    await expect(dialog.getByText("e2e-test-ingester")).not.toBeVisible({ timeout: 10_000 });
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
    await expect(dialog.getByRole("checkbox", { name: "Enabled" })).toBeVisible();
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
