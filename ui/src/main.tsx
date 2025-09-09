import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.tsx";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);

// Attempt to gracefully shut down the backend server when the window/tab closes
let shutdownSent = false;
const shutdownServer = () => {
  if (shutdownSent) return;
  shutdownSent = true;
  const url = "/shutdown";
  try {
    if ("sendBeacon" in navigator) {
      const body = new Blob([""], { type: "text/plain" });
      navigator.sendBeacon(url, body);
    } else {
      // Best-effort fallback; keepalive helps during unload
      fetch(url, { method: "POST", keepalive: true }).catch(() => {});
    }
  } catch {
    // noop
  }
};

window.addEventListener("pagehide", shutdownServer);
window.addEventListener("beforeunload", shutdownServer);
