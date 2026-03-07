import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

/** Creates a QueryClient with test-friendly defaults (no retries, no refetching). */
export function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
}

/** Returns a wrapper component for renderHook that provides a fresh QueryClient. */
export function wrapper(qc?: QueryClient) {
  const client = qc ?? createTestQueryClient();
  return function TestWrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}
