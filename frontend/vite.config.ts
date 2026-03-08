import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import http2Proxy from "vite-plugin-http2-proxy";
import { createServer, request as httpRequest, type IncomingMessage, type ServerResponse } from "node:http";

// Primary Vite dev server on :3001 → backend :4564.
// Extra proxy servers on :3002-3004 forward assets/HMR to :3001
// and RPCs to their respective backends.
const extraPorts: Record<number, number> = {
  3002: 4574,
  3003: 4584,
  3004: 4594,
};

function multiNodeProxy(): Plugin {
  return {
    name: "multi-node-proxy",
    configureServer(server) {
      server.httpServer?.once("listening", () => {
        for (const [devPort, backendPort] of Object.entries(extraPorts)) {
          const proxy = createServer((req: IncomingMessage, res: ServerResponse) => {
            const isRpc = req.url?.startsWith("/gastrolog.v1.") ?? false;
            const target = isRpc
              ? { host: "localhost", port: backendPort }
              : { host: "localhost", port: 3001 };

            const proxyReq = httpRequest(
              {
                hostname: target.host,
                port: target.port,
                path: req.url,
                method: req.method,
                headers: req.headers,
              },
              (proxyRes) => {
                res.writeHead(proxyRes.statusCode ?? 502, proxyRes.headers);
                proxyRes.pipe(res, { end: true });
              },
            );
            proxyReq.on("error", () => {
              res.writeHead(502);
              res.end("proxy error");
            });
            req.pipe(proxyReq, { end: true });
          });
          proxy.listen(Number(devPort), () => {
            console.log(`  proxy :${devPort} → backend :${backendPort}`);
          });
          server.httpServer?.on("close", () => proxy.close());
        }
      });
    },
  };
}

export default defineConfig({
  plugins: [
    react({
      babel: {
        plugins: ["babel-plugin-react-compiler"],
      },
    }),
    tailwindcss(),
    http2Proxy({
      "^/gastrolog\\.v1\\.": {
        target: "http://localhost:4564",
        timeout: 0,
      },
      "^/api/": {
        target: "http://localhost:4564",
        timeout: 0,
      },
    }),
    multiNodeProxy(),
  ],
  build: {
    chunkSizeWarningLimit: 1500,
    reportCompressedSize: false,
  },
  server: {
    port: 3001,
  },
});
