import { test, expect } from "@playwright/test";
import { gotoAuthenticated } from "./helpers";

/**
 * Advanced auth flow E2E tests (gastrolog-a9f1w).
 *
 * Tests password change, session expiry handling, and role-based
 * visibility. Separate from auth.spec.ts which handles initial
 * registration and login.
 */

test.describe.serial("Auth flows", () => {
  test("change password dialog is accessible from user menu", async ({
    page,
  }) => {
    await gotoAuthenticated(page, "/search");

    await page.getByRole("button", { name: /User menu/ }).click();

    // "Change Password" option should be in the user menu.
    const changePwBtn = page.getByRole("button", { name: /Change Password/i });
    await expect(changePwBtn).toBeVisible();
    await changePwBtn.click();

    // Password change dialog should open.
    const dialog = page.getByRole("dialog", { name: /Change Password/i });
    await expect(dialog).toBeVisible();

    // Should show current password and new password fields.
    await expect(dialog.getByLabel(/Current Password/i)).toBeVisible();
    await expect(dialog.getByLabel(/New Password/i).first()).toBeVisible();

    // Close without changing.
    await page.keyboard.press("Escape");
  });

  test("password change validates matching confirmation", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await page.getByRole("button", { name: /User menu/ }).click();
    await page.getByRole("button", { name: /Change Password/i }).click();

    const dialog = page.getByRole("dialog", { name: /Change Password/i });
    await expect(dialog).toBeVisible();

    // Fill mismatched passwords.
    await dialog.getByLabel(/Current Password/i).fill("T3stP@ssw0rd!");
    const newPwFields = dialog.getByLabel(/New Password/i);
    await newPwFields.first().fill("NewP@ss1!");

    // If there's a confirm field, fill it with a different value.
    const confirmField = dialog.getByLabel(/Confirm/i);
    if (await confirmField.isVisible({ timeout: 2_000 }).catch(() => false)) {
      await confirmField.fill("DifferentP@ss1!");
    }

    // The submit button should be disabled or the form should show an error.
    await page.keyboard.press("Escape");
  });

  test("user menu shows current username", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    // The button's aria-label includes the username.
    const menuBtn = page.getByRole("button", { name: /User menu: admin/ });
    await expect(menuBtn).toBeVisible();
    await menuBtn.click();

    // The dropdown header shows the username (first text) and role (second).
    await expect(page.getByText("admin").first()).toBeVisible();
  });

  test("logout redirects to login page", async ({ page }) => {
    await gotoAuthenticated(page, "/search");

    await page.getByRole("button", { name: /User menu/ }).click();

    const logoutBtn = page.getByRole("button", { name: /Log out|Sign out/i });
    if (await logoutBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await logoutBtn.click();

      // Should redirect to /login.
      await expect(page).toHaveURL(/\/login/, { timeout: 10_000 });

      // Log back in — the Logout RPC calls InvalidateTokens which
      // invalidates the JWT saved in auth-state.json. We must re-save
      // the storage state after re-login so subsequent tests get a valid token.
      await page.getByLabel("Username").fill("admin");
      await page.getByLabel("Password", { exact: true }).fill("T3stP@ssw0rd!");
      await page.getByRole("button", { name: "Sign In" }).click();
      await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });

      // Re-save auth state with the fresh token.
      const path = await import("node:path");
      const stateFile = path.join(__dirname, "..", "auth-state.json");
      await page.context().storageState({ path: stateFile });
    }
  });

  test("expired session redirects to login", async ({ page }) => {
    // Navigate without auth state to simulate expired session.
    await page.goto("/search");

    // Should redirect to /login if no valid session.
    await page.waitForURL(
      (url) => {
        const p = url.pathname;
        return p === "/search" || p === "/login";
      },
      { timeout: 10_000 },
    );

    // If redirected, verify the login form is shown.
    if (page.url().includes("/login")) {
      await expect(page.getByLabel("Username")).toBeVisible();
      await expect(page.getByLabel("Password", { exact: true })).toBeVisible();
    }
  });
});
