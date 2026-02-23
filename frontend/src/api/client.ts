import {
  createPromiseClient,
  ConnectError,
  Code,
  type Interceptor,
} from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { QueryService } from "./gen/gastrolog/v1/query_connect";
import { StoreService } from "./gen/gastrolog/v1/store_connect";
import { LifecycleService } from "./gen/gastrolog/v1/lifecycle_connect";
import { ConfigService } from "./gen/gastrolog/v1/config_connect";
import { AuthService } from "./gen/gastrolog/v1/auth_connect";
import { JobService } from "./gen/gastrolog/v1/job_connect";

// Token management — stored in localStorage, read by the auth interceptor.
const TOKEN_KEY = "gastrolog_token";
const REFRESH_TOKEN_KEY = "gastrolog_refresh_token";
let currentToken: string | null = localStorage.getItem(TOKEN_KEY);

export function getToken(): string | null {
  return currentToken;
}

// Proactive token refresh — schedules a refresh 1 minute before expiry.
let refreshTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleProactiveRefresh(token: string) {
  if (refreshTimer) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
  try {
    const [, payloadB64] = token.split(".");
    if (!payloadB64) return;
    const payload = JSON.parse(atob(payloadB64.replace(/-/g, "+").replace(/_/g, "/")));
    const exp = payload.exp as number | undefined;
    if (!exp) return;
    // Refresh 60 seconds before expiry, but at least 10 seconds from now.
    const msUntilRefresh = Math.max((exp * 1000 - Date.now()) - 60_000, 10_000);
    refreshTimer = setTimeout(() => {
      refreshTimer = null;
      tryRefresh();
    }, msUntilRefresh);
  } catch {
    // Can't decode — fall back to reactive refresh.
  }
}

export function setToken(token: string | null) {
  currentToken = token;
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
    scheduleProactiveRefresh(token);
  } else {
    localStorage.removeItem(TOKEN_KEY);
    if (refreshTimer) {
      clearTimeout(refreshTimer);
      refreshTimer = null;
    }
  }
}

export function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_TOKEN_KEY);
}

export function setRefreshToken(token: string | null) {
  if (token) {
    localStorage.setItem(REFRESH_TOKEN_KEY, token);
  } else {
    localStorage.removeItem(REFRESH_TOKEN_KEY);
  }
}

// Attaches the stored JWT to every outgoing RPC request.
const authInterceptor: Interceptor = (next) => async (req) => {
  if (currentToken) {
    req.header.set("Authorization", `Bearer ${currentToken}`);
  }
  return next(req);
};

// Track whether a refresh is already in-flight to avoid concurrent refreshes.
let refreshPromise: Promise<boolean> | null = null;

async function tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;
  try {
    const res = await authClient.refreshToken({ refreshToken: rt });
    if (res.token) {
      setToken(res.token.token);
    }
    setRefreshToken(res.refreshToken);
    return true;
  } catch {
    return false;
  }
}

/**
 * Attempt to refresh the auth token, deduplicating concurrent calls.
 * Exported for streaming hooks that catch Unauthenticated errors outside
 * the interceptor's reach (errors during async iteration).
 */
export function refreshAuth(): Promise<boolean> {
  if (!refreshPromise) {
    refreshPromise = tryRefresh().finally(() => {
      refreshPromise = null;
    });
  }
  return refreshPromise;
}

// Catches Unauthenticated errors on any RPC (except AuthService) and
// attempts a refresh before redirecting to login.
const unauthInterceptor: Interceptor = (next) => async (req) => {
  try {
    return await next(req);
  } catch (err) {
    if (
      err instanceof ConnectError &&
      err.code === Code.Unauthenticated &&
      !req.service.typeName.endsWith(".AuthService") &&
      !["/login", "/register"].includes(globalThis.location.pathname)
    ) {
      // Try to refresh the token.
      if (!refreshPromise) {
        refreshPromise = tryRefresh().finally(() => {
          refreshPromise = null;
        });
      }
      const refreshed = await refreshPromise;
      if (refreshed) {
        // Retry the original request with the new token.
        req.header.set("Authorization", `Bearer ${currentToken}`);
        return await next(req);
      }
      // Refresh failed — clear everything and redirect.
      setToken(null);
      setRefreshToken(null);
      globalThis.location.href = "/login";
      // Don't re-throw — the redirect is in progress and downstream
      // handlers would show an error toast for a stale auth failure.
      return await new Promise(() => {});
    }
    throw err;
  }
};

// Same origin in both dev and prod. In dev, vite-plugin-http2-proxy
// forwards RPC calls to the backend over HTTP/2 (required for streaming).
const API_BASE_URL = globalThis.location.origin;

const transport = createConnectTransport({
  baseUrl: API_BASE_URL,
  interceptors: [authInterceptor, unauthInterceptor],
});

// Service clients
export const queryClient = createPromiseClient(QueryService, transport);
export const storeClient = createPromiseClient(StoreService, transport);
export const lifecycleClient = createPromiseClient(LifecycleService, transport);
export const configClient = createPromiseClient(ConfigService, transport);
export const authClient = createPromiseClient(AuthService, transport);
export const jobClient = createPromiseClient(JobService, transport);

// Schedule proactive refresh for token loaded from localStorage on startup.
if (currentToken) scheduleProactiveRefresh(currentToken);

// When the tab regains focus, check if the token needs refreshing.
// Browsers throttle setTimeout in background tabs, so the proactive
// refresh timer may have missed its window.
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible" || !currentToken) return;
  try {
    const [, payloadB64] = currentToken.split(".");
    if (!payloadB64) return;
    const payload = JSON.parse(
      atob(payloadB64.replace(/-/g, "+").replace(/_/g, "/")),
    );
    const exp = payload.exp as number | undefined;
    if (!exp) return;
    // Token expires within 2 minutes — refresh now.
    if (exp * 1000 - Date.now() < 120_000) {
      refreshAuth();
    }
  } catch {
    // Can't decode — try refreshing defensively.
    refreshAuth();
  }
});

// Re-export types for convenience
export * from "./gen/gastrolog/v1/query_pb";
export * from "./gen/gastrolog/v1/store_pb";
export * from "./gen/gastrolog/v1/lifecycle_pb";
export * from "./gen/gastrolog/v1/config_pb";
export * from "./gen/gastrolog/v1/auth_pb";
export * from "./gen/gastrolog/v1/job_pb";
