import type { Page } from "@playwright/test";
import { expect } from "@playwright/test";

const ADMIN_USER = "admin";
const ADMIN_PASS = "T3stP@ssw0rd!";

/**
 * Expand the query bar (if collapsed) and fill the textarea with `query`.
 * The collapsed bar may already contain text from storageState history,
 * so we can't rely on "Search logs..." placeholder being visible.
 */
export async function typeQuery(page: Page, query: string) {
  const textarea = page.locator("textarea");
  if (!(await textarea.isVisible({ timeout: 2_000 }).catch(() => false))) {
    // Bar is collapsed — click the role="button" to expand.
    const collapsedBar = page.locator("[role='button'][tabindex='0']").first();
    await collapsedBar.click();
  }
  await expect(textarea).toBeVisible({ timeout: 3_000 });
  await textarea.fill(query);
}

/**
 * Navigate to a route, handling auth redirects. If the saved auth state
 * has expired (JWT tokens last 15 minutes), logs in automatically.
 */
export async function gotoAuthenticated(page: Page, path: string) {
  await page.goto(path);

  // If the token is still valid, the page stays on the requested path.
  // If expired, the unauthInterceptor redirects to /login.
  // Wait briefly for any redirect to settle.
  await page.waitForURL((url) => {
    const p = url.pathname;
    return p === path || p === "/login";
  }, { timeout: 10_000 });

  if (page.url().includes("/login")) {
    // Token expired — re-login.
    await page.getByLabel("Username").fill(ADMIN_USER);
    await page.getByLabel("Password", { exact: true }).fill(ADMIN_PASS);
    await page.getByRole("button", { name: "Sign In" }).click();
    await expect(page).toHaveURL(new RegExp(path.replace("/", "\\/")), {
      timeout: 15_000,
    });
  }

  // Verify we're on the right page with the header visible.
  await expect(page.getByRole("heading", { name: "GastroLog" })).toBeVisible({
    timeout: 10_000,
  });
}

/** Open settings dialog and navigate to a specific tab. */
export async function openSettingsTab(page: Page, tab: string) {
  await gotoAuthenticated(page, "/search");
  await page.getByRole("button", { name: "Settings" }).click();
  const dialog = page.getByRole("dialog", { name: "Settings" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: tab }).click();
  await expect(dialog.getByRole("heading", { name: tab })).toBeVisible();
  return dialog;
}
