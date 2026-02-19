import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import http2Proxy from "vite-plugin-http2-proxy";

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
    }),
  ],
  build: {
    chunkSizeWarningLimit: 1500,
    reportCompressedSize: false,
  },
  server: {
    port: 3000,
  },
});
