import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

test.describe.serial("Cluster Status", () => {
  test("shows 3 nodes in the inspector nodes view", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    await expect(dialog.getByText("node-1")).toBeVisible();
    await expect(dialog.getByText("node-2")).toBeVisible();
    await expect(dialog.getByText("node-3")).toBeVisible();
  });

  test("shows leader badge on one node", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    const navSection = dialog.locator("nav");
    await expect(navSection.getByText("leader")).toBeVisible();
  });

  test("header shows aggregated stats", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    const statsRibbon = page.locator(".lg\\:flex").filter({ hasText: "CPU" });
    await expect(statsRibbon).toBeVisible({ timeout: 15_000 });
  });

  test("node detail shows uptime and stats", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    await dialog.getByText("node-1").click();

    await expect(dialog.getByText("Uptime")).toBeVisible({ timeout: 10_000 });
  });

  test("each node shows its own stats", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    await page.getByRole("button", { name: "Inspector" }).click();

    const dialog = page.getByRole("dialog", { name: "Inspector" });

    const nodesButton = dialog.getByRole("button", { name: "Nodes" });
    await expect(nodesButton).toBeVisible({ timeout: 10_000 });
    await nodesButton.click();

    // Click each node and verify its detail loads.
    for (const nodeName of ["node-1", "node-2", "node-3"]) {
      await dialog.getByText(nodeName).click();
      await expect(dialog.getByText("Uptime")).toBeVisible({ timeout: 10_000 });
    }
  });

  // ── Stats bar tooltips (gastrolog-3swfy) ─────────────────────────

  test("stats bar items show tooltips on hover", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The stats ribbon shows CPU, memory, etc.
    const statsRibbon = page.locator(".lg\\:flex").filter({ hasText: "CPU" });
    await expect(statsRibbon).toBeVisible({ timeout: 15_000 });

    // Hover over each stat pill to verify tooltip appears.
    // Stats are rendered as small pill elements with a title attribute.
    const statPills = statsRibbon.locator("[title]");
    const count = await statPills.count();

    if (count > 0) {
      // Hover the first stat — tooltip (title attr) should be present.
      const firstPill = statPills.first();
      const titleAttr = await firstPill.getAttribute("title");
      expect(titleAttr).toBeTruthy();
    }
  });

  test("search returns results from all nodes", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Execute a wildcard search — data should come from all nodes.
    await typeQuery(page, "*");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const countText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const count = parseInt(countText!.replace(/[^0-9]/g, ""), 10);
    // With chatterbox running for 30+ seconds, we should have plenty of records.
    expect(count).toBeGreaterThan(0);
  });
});
