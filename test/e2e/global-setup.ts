/**
 * Global setup: waits for all 3 cluster nodes to be healthy before
 * tests begin. Runs once before any test file.
 */
async function globalSetup() {
  const baseURL = process.env.E2E_BASE_URL ?? "http://localhost:14564";

  // Poll the health endpoint until the cluster is ready.
  // The frontend is served by the same process, so if the API responds,
  // the UI is available too.
  const maxWait = 120_000; // 2 minutes — Docker build + cluster bootstrap
  const interval = 2_000;
  const start = Date.now();

  console.log(`Waiting for cluster at ${baseURL} ...`);

  while (Date.now() - start < maxWait) {
    try {
      const res = await fetch(`${baseURL}/healthz`);
      if (res.ok) {
        console.log(`Cluster healthy after ${((Date.now() - start) / 1000).toFixed(1)}s`);
        return;
      }
    } catch {
      // Not ready yet — retry.
    }
    await new Promise((r) => setTimeout(r, interval));
  }

  throw new Error(
    `Cluster not healthy after ${maxWait / 1000}s — is 'just e2e-up' running?`,
  );
}

export default globalSetup;
