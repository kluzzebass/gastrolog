import { createPromiseClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { QueryService } from "./gen/gastrolog/v1/query_connect";
import { StoreService } from "./gen/gastrolog/v1/store_connect";
import { LifecycleService } from "./gen/gastrolog/v1/lifecycle_connect";
import { ConfigService } from "./gen/gastrolog/v1/config_connect";

// API base URL: In dev mode, Vite proxy forwards /gastrolog.v1.* to the backend.
// In production, assume the backend serves the frontend (same origin).
const API_BASE_URL = window.location.origin;

// Create transport using Connect protocol (works over HTTP/1.1 and HTTP/2)
const transport = createConnectTransport({
  baseUrl: API_BASE_URL,
});

// Service clients
export const queryClient = createPromiseClient(QueryService, transport);
export const storeClient = createPromiseClient(StoreService, transport);
export const lifecycleClient = createPromiseClient(LifecycleService, transport);
export const configClient = createPromiseClient(ConfigService, transport);

// Re-export types for convenience
export * from "./gen/gastrolog/v1/query_pb";
export * from "./gen/gastrolog/v1/store_pb";
export * from "./gen/gastrolog/v1/lifecycle_pb";
export * from "./gen/gastrolog/v1/config_pb";
