import { mock } from "bun:test";

type MockFn = ReturnType<typeof mock>;

/**
 * Creates a Proxy-backed mock client: any property access returns a stable
 * mock function that resolves to `{}` by default. Tests can reconfigure
 * individual methods via `.mockResolvedValue()` / `.mockImplementation()`.
 *
 * Usage:
 *   const client = autoMockClient();
 *   m(client, "getConfig").mockResolvedValueOnce(new GetConfigResponse({ ... }));
 */
function autoMockClient(): Record<string, MockFn> {
  const methods: Record<string, MockFn> = {};
  return new Proxy(methods, {
    get(target, prop: string) {
      if (!(prop in target)) {
        target[prop] = mock(() => Promise.resolve({}));
      }
      return target[prop]!;
    },
  });
}

/** Type-safe accessor for a mock method — avoids noUncheckedIndexedAccess issues. */
export function m(client: Record<string, MockFn>, method: string): MockFn {
  return client[method]!;
}

/**
 * Installs mock.module for `src/api/client` with auto-mock clients.
 * Returns the mock clients so tests can configure per-method responses.
 *
 * MUST be called at the top of the test file, before any imports that
 * transitively pull in `../client`.
 *
 * Usage:
 *   const mocks = installMockClients();
 *   // now import hooks...
 *   import { useConfig } from "./useConfig";
 */
export function installMockClients() {
  const clients = {
    queryClient: autoMockClient(),
    vaultClient: autoMockClient(),
    lifecycleClient: autoMockClient(),
    configClient: autoMockClient(),
    authClient: autoMockClient(),
    jobClient: autoMockClient(),
  };

  // mock.module replaces the module for all importers in this test file.
  // The path is resolved relative to this file (test/api-mock.ts) →
  // ../src/api/client which matches the hooks' "../client" import.
  mock.module("../src/api/client", () => ({
    ...clients,
    getToken: mock(() => null),
    setToken: mock(() => {}),
    getRefreshToken: mock(() => null),
    setRefreshToken: mock(() => {}),
    refreshAuth: mock(() => Promise.resolve(false)),
  }));

  return clients;
}
