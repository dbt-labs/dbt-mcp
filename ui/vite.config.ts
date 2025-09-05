import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  // Use relative asset URLs so the app works when served from any base path
  base: "./",
  build: {
    // Emit into the Python package so assets are included in sdist/wheels
    outDir: "../src/dbt_mcp/ui/dist",
    emptyOutDir: true,
    assetsDir: "assets",
  },
});
