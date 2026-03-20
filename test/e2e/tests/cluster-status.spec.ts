import { test, expect } from "@playwright/test";
import { gotoAuthenticated } from "./helpers";

test.describe.serial("Cluster Status", () => {
  test("shows 3 nodes in the inspector nodes view", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // In multi-node mode, the Nodes/Entities toggle should be visible.
    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    // All 3 nodes should be listed in the navigation.
    await expect(dialog.getByText("node-1")).toBeVisible();
    await expect(dialog.getByText("node-2")).toBeVisible();
    await expect(dialog.getByText("node-3")).toBeVisible();
  });

  test("shows leader badge on one node", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // Switch to Nodes view.
    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    // Exactly one node should have the "leader" badge — it's in the nav list,
    // not in the detail pane.
    const navSection = dialog.locator("nav");
    await expect(navSection.getByText("leader")).toBeVisible();
  });

  test("header shows aggregated stats", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Wait for cluster status to load — stat pills appear in the header.
    // Stats are in a `hidden lg:flex` container (only visible at ≥1024px viewport).
    // StatPill labels are "CPU", "Memory", "Storage" with CSS uppercase.
    const statsRibbon = page.locator(".lg\\:flex").filter({ hasText: "CPU" });
    await expect(statsRibbon).toBeVisible({ timeout: 15_000 });
  });

  test("node detail shows stats", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    // Switch to Nodes view.
    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    // Click on node-1 to see its detail.
    await dialog.getByText("node-1").click();

    // Node detail pane should show some stats.
    await expect(dialog.getByText("Uptime")).toBeVisible({
      timeout: 10_000,
    });
  });
});
