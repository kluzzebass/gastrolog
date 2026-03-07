import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ToastProvider } from "../src/components/Toast";
import { HelpProvider } from "../src/hooks/useHelp";

/** Creates a QueryClient with test-friendly defaults (no retries, no refetching). */
export function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: Infinity },
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

/** Returns a wrapper with QueryClient + ToastProvider + HelpProvider for component rendering. */
export function settingsWrapper(qc?: QueryClient) {
  const client = qc ?? createTestQueryClient();
  return function SettingsTestWrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={client}>
        <HelpProvider onOpen={() => {}}>
          <ToastProvider dark={true}>
            {children}
          </ToastProvider>
        </HelpProvider>
      </QueryClientProvider>
    );
  };
}
