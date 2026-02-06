import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import http2Proxy from "vite-plugin-http2-proxy";

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    http2Proxy({
      "^/gastrolog\\.v1\\.": {
        target: "http://localhost:8080",
      },
    }),
  ],
  server: {
    port: 3000,
  },
});
