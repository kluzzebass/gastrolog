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

// Token management — stored in localStorage, read by the auth interceptor.
const TOKEN_KEY = "gastrolog_token";
let currentToken: string | null = localStorage.getItem(TOKEN_KEY);

export function getToken(): string | null {
  return currentToken;
}

export function setToken(token: string | null) {
  currentToken = token;
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
  } else {
    localStorage.removeItem(TOKEN_KEY);
  }
}

// Attaches the stored JWT to every outgoing RPC request.
const authInterceptor: Interceptor = (next) => async (req) => {
  if (currentToken) {
    req.header.set("Authorization", `Bearer ${currentToken}`);
  }
  return next(req);
};

// Catches Unauthenticated errors on any RPC (except AuthService) and
// redirects to login, preventing stale-token scenarios from silently failing.
const unauthInterceptor: Interceptor = (next) => async (req) => {
  try {
    return await next(req);
  } catch (err) {
    if (
      err instanceof ConnectError &&
      err.code === Code.Unauthenticated &&
      !req.service.typeName.endsWith(".AuthService")
    ) {
      setToken(null);
      window.location.href = "/login";
    }
    throw err;
  }
};

// Same origin in both dev and prod. In dev, vite-plugin-http2-proxy
// forwards RPC calls to the backend over HTTP/2 (required for streaming).
const API_BASE_URL = window.location.origin;

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

// Re-export types for convenience
export * from "./gen/gastrolog/v1/query_pb";
export * from "./gen/gastrolog/v1/store_pb";
export * from "./gen/gastrolog/v1/lifecycle_pb";
export * from "./gen/gastrolog/v1/config_pb";
export * from "./gen/gastrolog/v1/auth_pb";
