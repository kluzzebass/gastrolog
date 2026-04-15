import { defineConfig, devices } from "@playwright/test";

// When running inside Docker (fully containerized), Playwright connects
// directly to node-1 on the Docker network. For local dev mode, the
// compose file exposes node-1's API on host port 14564.
const baseURL = process.env.E2E_BASE_URL ?? "http://localhost:14564";

export default defineConfig({
  testDir: "./tests",
  outputDir: "./results/artifacts",

  // Cluster is shared state — run serially with a single worker.
  fullyParallel: false,
  workers: 1,

  // Retry once on failure to handle timing-sensitive cluster tests.
  retries: 1,

  // Generous timeout — cluster operations can be slow.
  timeout: 60_000,
  expect: { timeout: 15_000 },

  reporter: [
    ["list"],
    ["html", { outputFolder: "./results/report", open: "never" }],
  ],

  use: {
    baseURL,
    // Capture trace on first retry for debugging failures.
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },

  projects: [
    // Auth tests run first — unauthenticated, registers admin, saves state.
    {
      name: "auth",
      testMatch: "auth.spec.ts",
      use: { ...devices["Desktop Chrome"], storageState: undefined },
    },
    // Seed project: creates a chatterbox ingester and waits for searchable
    // data. Runs after auth (setup wizard creates the vault/route), before
    // app tests that need records.
    {
      name: "seed",
      testMatch: "seed.spec.ts",
      dependencies: ["auth"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: "./auth-state.json",
      },
    },
    // All other tests depend on seed (which depends on auth).
    {
      name: "app",
      testIgnore: ["auth.spec.ts", "seed.spec.ts"],
      dependencies: ["seed"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: "./auth-state.json",
      },
    },
  ],

  globalSetup: "./global-setup.ts",
});
