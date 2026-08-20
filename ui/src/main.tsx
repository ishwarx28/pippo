// Mounts the token-driven React shell.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./theme.css";
import { App } from "./app/App";

const root = document.getElementById("root");

if (!root) {
  throw new Error("missing React root");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
