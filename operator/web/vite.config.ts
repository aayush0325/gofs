import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite proxies /api to the operator UI server during local development.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8090",
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
