import { test, expect } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

// Auth tests run without saved state — this is the first project.
// They ensure an admin user exists (registering if needed), test
// auth flows, and save auth state for other test files.
//
// IDEMPOTENT: Works on both fresh clusters (no users → /register)
// and existing clusters (users exist → /login). The setup wizard
// and registration tests are skipped when users already exist.
//
// IMPORTANT: ChangePassword calls InvalidateTokens on the server,
// which permanently rejects all JWT tokens issued before the change.
// The auth state MUST be saved from a login that happens AFTER the
// last password change. The final test does exactly this.
//
// Rate limit: 5 req/min per IP, burst of 5. Budget per test noted below.

const ADMIN_USER = "admin";
const ADMIN_PASS = "T3stP@ssw0rd!";
const NEW_PASS = "N3wP@ssw0rd!";
const AUTH_STATE = path.join(__dirname, "..", "auth-state.json");

async function waitForToken(page: import("@playwright/test").Page) {
  await page.waitForFunction(
    () => !!localStorage.getItem("gastrolog_token"),
    null,
    { timeout: 10_000 },
  );
}

async function completeSetupWizard(page: import("@playwright/test").Page) {
  await expect(page.getByText("Welcome to GastroLog")).toBeVisible({
    timeout: 10_000,
  });
  await page.getByRole("button", { name: "Get Started" }).click();

  await expect(page.getByText("Configure Vault")).toBeVisible();
  await page.getByRole("combobox").selectOption("memory");
  await page.getByRole("button", { name: "Next" }).click();

  // Rotation + Retention: defaults fine.
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();

  await expect(page.getByText("Configure Ingester")).toBeVisible();
  await page.getByRole("button", { name: "Chatterbox" }).click();
  await page.getByRole("button", { name: "Next" }).click();

  await page.getByRole("button", { name: "Create" }).click();
  await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });
}

/** Navigate to / and determine whether the cluster has users. */
async function probeClusterState(
  page: import("@playwright/test").Page,
): Promise<"fresh" | "existing"> {
  await page.goto("/");
  // The app redirects / → /login, then if needsSetup is true,
  // /login → /register. Wait for the initial redirect, then give
  // the secondary redirect time to fire.
  await page.waitForURL(/\/(register|login)/, { timeout: 15_000 });
  // Allow the needsSetup redirect to settle.
  await page.waitForTimeout(2_000);
  const url = page.url();
  return url.includes("/register") ? "fresh" : "existing";
}

test.describe.serial("Authentication", () => {
  let clusterState: "fresh" | "existing";

  test("detects cluster state and registers if needed", async ({ page }) => {
    clusterState = await probeClusterState(page);

    if (clusterState === "fresh") {
      // Fresh cluster — register the admin user and complete setup.
      await page.goto("/register");
      await page.getByLabel("Username").fill(ADMIN_USER);
      await page.getByLabel("Password", { exact: true }).fill(ADMIN_PASS);
      await page.getByLabel("Confirm Password").fill(ADMIN_PASS);
      await page.getByRole("button", { name: "Create Account" }).click();

      await expect(page).toHaveURL(/\/setup/, { timeout: 15_000 });
      await waitForToken(page);
      await completeSetupWizard(page);
    }
    // If existing, nothing to do — user already exists.
  });

  test("logs in and logs out", async ({ page }) => {
    await page.goto("/login");
    await page.getByLabel("Username").fill(ADMIN_USER);
    await page.getByLabel("Password", { exact: true }).fill(ADMIN_PASS);
    await page.getByRole("button", { name: "Sign In" }).click();
    await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });
    await expect(
      page.getByRole("heading", { name: "GastroLog" }),
    ).toBeVisible();

    // Log out — verify redirect back to /login.
    await page.getByRole("button", { name: /User menu/ }).click();
    await page.getByRole("button", { name: "Sign out" }).click();
    await expect(page).toHaveURL(/\/login/, { timeout: 10_000 });
  });

  test("changes password and verifies new password works", async ({ page }) => {
    await page.goto("/login");
    await page.getByLabel("Username").fill(ADMIN_USER);
    await page.getByLabel("Password", { exact: true }).fill(ADMIN_PASS);
    await page.getByRole("button", { name: "Sign In" }).click();
    await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });

    // Change password to NEW_PASS.
    await page.getByRole("button", { name: /User menu/ }).click();
    await page.getByRole("button", { name: "Change password" }).click();

    const dialog = page.getByRole("dialog", { name: "Change Password" });
    await expect(dialog).toBeVisible();
    await dialog.getByLabel("Current Password").fill(ADMIN_PASS);
    await dialog.getByLabel("New Password", { exact: true }).fill(NEW_PASS);
    await dialog.getByLabel("Confirm New Password").fill(NEW_PASS);
    await dialog.getByRole("button", { name: "Change Password" }).click();
    await expect(dialog).not.toBeVisible({ timeout: 10_000 });

    // ChangePassword calls InvalidateTokens — the current JWT is now dead.
    // Navigate to /login explicitly and re-login with the new password.
    await page.goto("/login");
    await page.getByLabel("Username").fill(ADMIN_USER);
    await page.getByLabel("Password", { exact: true }).fill(NEW_PASS);
    await page.getByRole("button", { name: "Sign In" }).click();
    await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });

    // Restore original password (now with a fresh JWT from the re-login above).
    await page.getByRole("button", { name: /User menu/ }).click();
    await page.getByRole("button", { name: "Change password" }).click();

    const dialog2 = page.getByRole("dialog", { name: "Change Password" });
    await dialog2.getByLabel("Current Password").fill(NEW_PASS);
    await dialog2.getByLabel("New Password", { exact: true }).fill(ADMIN_PASS);
    await dialog2.getByLabel("Confirm New Password").fill(ADMIN_PASS);
    await dialog2.getByRole("button", { name: "Change Password" }).click();
    await expect(dialog2).not.toBeVisible({ timeout: 10_000 });
  });

  // This MUST be the last test — it saves auth state with a token issued
  // AFTER all password changes (which call InvalidateTokens).
  test("saves auth state for app tests", async ({ page }) => {
    // Wait for rate limiter.
    await page.waitForTimeout(12_000);

    await page.goto("/login");
    await page.getByLabel("Username").fill(ADMIN_USER);
    await page.getByLabel("Password", { exact: true }).fill(ADMIN_PASS);
    await page.getByRole("button", { name: "Sign In" }).click();
    await expect(page).toHaveURL(/\/search/, { timeout: 15_000 });

    await waitForToken(page);
    await page.context().storageState({ path: AUTH_STATE });
    expect(fs.existsSync(AUTH_STATE)).toBe(true);
  });
});
