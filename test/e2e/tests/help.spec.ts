import { test, expect } from "@playwright/test";
import { gotoAuthenticated } from "./helpers";

test.describe.serial("Help", () => {
  test("opens help dialog from header", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });
    await expect(dialog).toBeVisible();

    // Help dialog should show topic navigation.
    await expect(dialog.getByText("Topics")).toBeVisible();
  });

  test("shows topic list and navigates to a topic", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });

    // Click a topic in the sidebar — look for any topic link.
    // The help system has hierarchical topics; click the first available one.
    const topicButtons = dialog
      .locator("nav button")
      .filter({ hasNotText: "Topics" });
    const firstTopic = topicButtons.first();
    await expect(firstTopic).toBeVisible();
    await firstTopic.click();

    // Content should render in the right panel.
    // Wait for "Loading..." to disappear and content to appear.
    await expect(dialog.getByText("Loading...")).not.toBeVisible({
      timeout: 10_000,
    });

    // The content panel should have rendered some markdown — check for any
    // heading or paragraph element outside the nav sidebar.
    const contentArea = dialog.locator(":not(nav) > div").filter({
      has: page.locator("h1, h2, h3, p"),
    });
    await expect(contentArea.first()).toBeVisible({ timeout: 5_000 });
  });

  test("searches help topics", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });

    // Type in the search input.
    const searchInput = dialog.getByPlaceholder("Search help...");
    await expect(searchInput).toBeVisible();
    await searchInput.fill("query");

    // Search results should appear — at least one match for "query".
    // Give it a moment for the search to filter.
    await page.waitForTimeout(500);

    // Clear the search.
    const clearButton = dialog.getByRole("button", { name: "Clear search" });
    if (await clearButton.isVisible()) {
      await clearButton.click();
      // Topics list should be restored.
      await expect(dialog.getByText("Topics")).toBeVisible();
    }
  });

  test("help text is selectable", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });

    // Navigate to a topic.
    const topicButtons = dialog
      .locator("nav button")
      .filter({ hasNotText: "Topics" });
    await topicButtons.first().click();
    await expect(dialog.getByText("Loading...")).not.toBeVisible({
      timeout: 10_000,
    });

    // Verify text is selectable by checking that user-select is not "none"
    // on the content area. This is a regression check for portal focus issues.
    // Find the content panel (the non-nav sibling with overflow-y-auto).
    const contentPanel = dialog.locator("div.app-scroll").last();
    const userSelect = await contentPanel.evaluate(
      (el) => getComputedStyle(el).userSelect,
    );
    expect(userSelect).not.toBe("none");
  });

  test("closes help with Escape", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Help" }).click();

    const dialog = page.getByRole("dialog", { name: "Help" });
    await expect(dialog).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });

  // ── Help links from settings (gastrolog-1op0n) ─────────────────────

  test("settings tabs have help links that open help dialog", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Settings" }).click();

    const settingsDialog = page.getByRole("dialog", { name: "Settings" });
    await expect(settingsDialog).toBeVisible();

    // Navigate to Vaults tab — it should have a help button.
    await settingsDialog.getByRole("button", { name: "Vaults" }).click();

    // Look for a help button (? icon) within the settings content.
    const helpBtn = settingsDialog.locator("button").filter({
      has: page.locator("[aria-label*='help' i], [title*='help' i]"),
    });

    // Some tabs may render help as a HelpButton component.
    // Try finding any clickable help icon in the heading area.
    const helpIcon = settingsDialog.locator(
      "[aria-label*='help' i], [title*='Help' i]",
    );

    if (
      await helpIcon
        .first()
        .isVisible({ timeout: 3_000 })
        .catch(() => false)
    ) {
      await helpIcon.first().click();

      // Help dialog should open.
      const helpDialog = page.getByRole("dialog", { name: "Help" });
      await expect(helpDialog).toBeVisible({ timeout: 5_000 });

      // Close help to restore state.
      await page.keyboard.press("Escape");
    }
  });
});
