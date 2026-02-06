import { createPromiseClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { QueryService } from "./gen/gastrolog/v1/query_connect";
import { StoreService } from "./gen/gastrolog/v1/store_connect";
import { LifecycleService } from "./gen/gastrolog/v1/lifecycle_connect";
import { ConfigService } from "./gen/gastrolog/v1/config_connect";

// Same origin in both dev and prod. In dev, vite-plugin-http2-proxy
// forwards RPC calls to the backend over HTTP/2 (required for streaming).
const API_BASE_URL = window.location.origin;

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
