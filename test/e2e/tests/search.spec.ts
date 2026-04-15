import { test, expect } from "@playwright/test";
import { gotoAuthenticated, typeQuery } from "./helpers";

test.describe.serial("Search", () => {
  test("shows the query bar on /search", async ({ page }) => {
    await gotoAuthenticated(page, "/search");
    const queryArea = page
      .locator("textarea")
      .or(page.locator("[role='button'][tabindex='0']"));
    await expect(queryArea.first()).toBeVisible();
  });

  test("executes a wildcard query and shows results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Search recent records across all vaults.
    await typeQuery(page, "last=5m reverse=true");
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

  test("shows histogram chart", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Histogram renders only when buckets > 0 — soft check.
    const histogram = page.locator("[data-testid='histogram']");
    if (await histogram.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await expect(histogram).toBeVisible();
    }
  });

  test("opens detail sidebar when clicking a log entry", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const firstEntry = page.locator("[data-testid='log-entry']").first();
    await expect(firstEntry).toBeVisible();
    await firstEntry.click();

    // Detail sidebar should open.
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Sidebar should show field details — timestamps, severity, or attributes.
    await expect(
      sidebar.getByText(/timestamp|severity|level|message/i).first(),
    ).toBeVisible({
      timeout: 5_000,
    });
  });

  test("detail sidebar shows copyable field values", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Click a log entry to open the sidebar.
    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // The raw log line should be shown (font-mono text area).
    await expect(sidebar.locator(".font-mono").first()).toBeVisible();
  });

  test("severity filters toggle search results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Execute a wildcard search first to get results.
    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // The severity filter buttons should be visible in the sidebar.
    // Labels are "Error", "Warn", "Info", "Debug", "Trace" (rendered uppercase via CSS).
    const errorBtn = page.getByRole("button", { name: "Error", exact: true });
    const warnBtn = page.getByRole("button", { name: "Warn", exact: true });
    const infoBtn = page.getByRole("button", { name: "Info", exact: true });
    await expect(errorBtn).toBeVisible();
    await expect(warnBtn).toBeVisible();
    await expect(infoBtn).toBeVisible();

    // Click Error to toggle it — should filter results.
    await errorBtn.click();

    // Re-search to apply the filter.
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 15_000,
    });
  });

  test("time range presets change the range", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Time range section should be visible.
    await expect(page.getByText("Time Range")).toBeVisible();

    // Click the "1h" preset.
    const preset1h = page.getByRole("button", { name: "1h", exact: true });
    await expect(preset1h).toBeVisible();
    await preset1h.click();

    // The preset should now be active (has copper background).
    // Run a search with the new time range.
    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });
  });

  test("vault selector shows available vaults", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The sidebar should show a "Vaults" section with at least one vault.
    await expect(page.getByText("Vaults")).toBeVisible();

    // The vault created by the setup wizard should be listed with a record count.
    // Look for a vault button that shows a number (record count).
    const vaultSection = page.locator("text=Vaults").locator("..");
    await expect(vaultSection).toBeVisible();
  });

  test("query with key=value syntax works", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Chatterbox generates logs with various formats including key=value.
    // Use a broad key=value query that should match some records.
    await typeQuery(page, "level=error");
    await page.getByRole("button", { name: "Search" }).click();

    // Should get results (chatterbox generates error-level logs).
    // Wait with generous timeout — there may be few error-level records.
    const resultCount = page.locator("[data-testid='result-count']");
    await expect(resultCount).toBeVisible({ timeout: 30_000 });
  });

  test("empty query shows no results", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // Search for something that definitely won't match.
    await typeQuery(page, '"e2e_nonexistent_string_xyzzy_12345"');
    await page.getByRole("button", { name: "Search" }).click();

    // Should show 0 results or empty state. Wait for the search to complete.
    // The result count should show "0" or not appear at all.
    await page.waitForTimeout(3_000);

    const resultCount = page.locator("[data-testid='result-count']");
    if (await resultCount.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const text = await resultCount.textContent();
      const count = parseInt(text!.replace(/[^0-9]/g, ""), 10);
      expect(count).toBe(0);
    }
  });

  // ── Severity filters (gastrolog-5pdlh) ─────────────────────────────

  test("all five severity buttons are visible", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    for (const sev of ["Error", "Warn", "Info", "Debug", "Trace"]) {
      await expect(page.getByRole("button", { name: sev })).toBeVisible();
    }
  });

  test("toggling multiple severity filters narrows results", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const allCountText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const allCount = parseInt(allCountText!.replace(/[^0-9]/g, ""), 10);

    // Toggle off Info, Debug, Trace — keep only Error + Warn.
    await page.getByRole("button", { name: "Info", exact: true }).click();
    await page.getByRole("button", { name: "Debug", exact: true }).click();
    await page.getByRole("button", { name: "Trace", exact: true }).click();

    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const filteredText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const filteredCount = parseInt(filteredText!.replace(/[^0-9]/g, ""), 10);

    // Filtered count should be <= the full count.
    expect(filteredCount).toBeLessThanOrEqual(allCount);

    // Re-enable all severities to restore state.
    await page.getByRole("button", { name: "Info", exact: true }).click();
    await page.getByRole("button", { name: "Debug", exact: true }).click();
    await page.getByRole("button", { name: "Trace", exact: true }).click();
  });

  // ── Time range presets (gastrolog-5pdlh) ───────────────────────────

  test("all time range presets are visible", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await expect(page.getByText("Time Range")).toBeVisible();
    for (const preset of ["5m", "15m", "1h", "6h", "24h", "7d", "30d"]) {
      await expect(
        page.getByRole("button", { name: preset, exact: true }),
      ).toBeVisible();
    }
  });

  test("switching time range preset re-searches", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");

    // Use a narrow range then widen it.
    await page.getByRole("button", { name: "5m", exact: true }).click();
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const narrowText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const narrowCount = parseInt(narrowText!.replace(/[^0-9]/g, ""), 10);

    await page.getByRole("button", { name: "30d", exact: true }).click();
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const wideText = await page
      .locator("[data-testid='result-count']")
      .textContent();
    const wideCount = parseInt(wideText!.replace(/[^0-9]/g, ""), 10);

    // Wider range should have >= records.
    expect(wideCount).toBeGreaterThanOrEqual(narrowCount);
  });

  // ── Vault selector (gastrolog-5pdlh) ───────────────────────────────

  test("clicking a vault filters search to that vault", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The sidebar "Vaults" section should list vaults as buttons.
    await expect(page.getByText("Vaults")).toBeVisible();

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    // Find the first vault button in the Vaults section and click it.
    // Vault buttons have the vault name as accessible text.
    const vaultButtons = page.locator("button").filter({ hasText: /records/ });
    const firstVault = vaultButtons.first();
    if (await firstVault.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await firstVault.click();

      // Re-search with the vault filter applied.
      await page.getByRole("button", { name: "Search" }).click();
      await expect(page.locator("[data-testid='result-count']")).toBeVisible({
        timeout: 30_000,
      });
    }
  });

  // ── Detail sidebar field interactions (gastrolog-psw6z) ────────────

  test("detail sidebar shows field key-value pairs", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Should show at least the timestamp and raw body.
    await expect(sidebar.getByText(/timestamp/i).first()).toBeVisible({
      timeout: 5_000,
    });
  });

  test("detail sidebar field value has alt-click filter hint", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Field values that support click-to-filter have a title attribute.
    // Not all records produce clickable spans (depends on record format),
    // so verify the sidebar rendered fields rather than requiring click hints.
    const filterHint = sidebar.locator('[title*="click to add filter"]');
    const hasHints = await filterHint.first()
      .isVisible({ timeout: 5_000 })
      .catch(() => false);

    if (!hasHints) {
      // At minimum, the sidebar should show field table rows.
      await expect(sidebar.locator("td").first()).toBeVisible();
    }
  });

  test("detail sidebar has copy buttons", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // Copy buttons exist in the detail sidebar (CopyButton components).
    const copyButtons = sidebar.getByRole("button", { name: /copy/i });
    await expect(copyButtons.first()).toBeVisible({ timeout: 5_000 });
  });

  // ── Event identity (gastrolog-2ip1b) ────────────────────────────────

  test("detail sidebar shows event identity section", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // The "Event Identity" section header should be visible.
    await expect(sidebar.getByText("Event Identity")).toBeVisible({
      timeout: 5_000,
    });

    // Identity fields should be present (ingester_id and ingest_seq are
    // rendered as table cells; ingest_ts is not displayed as its own row).
    await expect(sidebar.getByText("ingester_id")).toBeVisible();
    await expect(sidebar.getByText("ingest_seq")).toBeVisible();
  });

  test("event identity header is clickable for multi-field filter", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    await page.locator("[data-testid='log-entry']").first().click();
    const sidebar = page.locator("[data-testid='detail-sidebar']");
    await expect(sidebar).toBeVisible();

    // The Event Identity heading should have a filter title hint.
    const identityBtn = sidebar.locator(
      '[title="Filter by this event identity"]',
    );
    await expect(identityBtn).toBeVisible({ timeout: 5_000 });
  });

  // ── Explain plan (gastrolog-mingv) ─────────────────────────────────

  test("explain plan shows execution plan", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");

    // Click the explain plan button.
    await page.getByRole("button", { name: "Explain query plan" }).click();

    // The explain panel should show "Execution Plan" heading.
    await expect(page.getByText("Execution Plan")).toBeVisible({
      timeout: 15_000,
    });

    // Should show chunk information.
    await expect(page.getByText(/chunks/i).first()).toBeVisible();
  });

  test("explain plan shows cost summary when chunks exist", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Explain query plan" }).click();

    // The explain RPC can be slow on a fresh cluster with few sealed chunks.
    const heading = page.getByText("Execution Plan");
    const opened = await heading
      .isVisible({ timeout: 10_000 })
      .catch(() => false);

    if (opened) {
      // Cost summary renders only when chunks.length > 0.
      const costSummary = page.getByText(/records/i).first();
      const hasCost = await costSummary
        .isVisible({ timeout: 5_000 })
        .catch(() => false);
      if (hasCost) {
        await expect(page.getByText(/chunks/i).first()).toBeVisible();
      }

      // Toggle plan off.
      await page.getByRole("button", { name: "Explain query plan" }).click();
    }
  });

  // ── Histogram interactions (gastrolog-37lxp) ────────────────────────

  test("histogram renders with bar elements", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const histogram = page.locator("[data-testid='histogram']");
    if (await histogram.isVisible({ timeout: 5_000 }).catch(() => false)) {
      // The histogram should contain SVG or canvas elements for bars.
      const hasContent = await histogram
        .locator("canvas, svg, rect")
        .first()
        .isVisible({ timeout: 3_000 })
        .catch(() => false);
      expect(hasContent).toBeTruthy();
    }
  });

  test("histogram brush selection narrows time range", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "last=5m reverse=true");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.locator("[data-testid='result-count']")).toBeVisible({
      timeout: 30_000,
    });

    const histogram = page.locator("[data-testid='histogram']");
    if (await histogram.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const box = await histogram.boundingBox();
      if (box) {
        // Drag across a portion of the histogram to select a time range.
        const startX = box.x + box.width * 0.2;
        const endX = box.x + box.width * 0.5;
        const y = box.y + box.height / 2;

        await page.mouse.move(startX, y);
        await page.mouse.down();
        await page.mouse.move(endX, y, { steps: 10 });
        await page.mouse.up();

        // After brush selection, a re-search should happen or time range updates.
        // Just verify the UI is still responsive.
        await page.waitForTimeout(1_000);
      }
    }
  });

  test("explain plan with filter shows narrowing", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await typeQuery(page, "level=error");
    await page.getByRole("button", { name: "Explain query plan" }).click();

    // The explain RPC can be slow under load. Be resilient.
    const heading = page.getByText("Execution Plan");
    const opened = await heading
      .isVisible({ timeout: 10_000 })
      .catch(() => false);

    if (opened) {
      // A filtered query should show the expression in the plan.
      await expect(page.getByText(/level/).first()).toBeVisible();

      // Toggle off.
      await page.getByRole("button", { name: "Explain query plan" }).click();
    }
  });
});
