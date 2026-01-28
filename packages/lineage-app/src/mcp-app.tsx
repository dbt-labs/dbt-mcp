import { useApp } from "@modelcontextprotocol/ext-apps/react";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { LineageGraph } from "./components/LineageGraph";
import { NodeDetails } from "./components/NodeDetails";
import type { LineageData, LineageNode } from "./types";
import "./global.css";

function LineageApp() {
  const [lineageData, setLineageData] = useState<LineageData | null>(null);
  const [selectedNode, setSelectedNode] = useState<LineageNode | null>(null);
  const [error, setError] = useState<string | null>(null);

  const { app, error: appError } = useApp({
    appInfo: { name: "dbt Lineage Visualization", version: "0.1.0" },
    capabilities: {},
    onAppCreated: (app) => {
      app.ontoolinput = async (input) => {
        console.info("[Lineage] Received tool input:", input);
      };

      app.ontoolresult = async (result: CallToolResult) => {
        console.info("[Lineage] Received tool result:", result);
        if (result.isError) {
          setError("Failed to load lineage data");
          return;
        }

        try {
          const data = result.structuredContent as unknown as LineageData;
          if (data && data.nodes) {
            setLineageData(data);
            setError(null);
          } else {
            setError("Invalid lineage data format");
          }
        } catch (e) {
          console.error("[Lineage] Error parsing result:", e);
          setError("Failed to parse lineage data");
        }
      };

      app.onerror = (err) => {
        console.error("[Lineage] App error:", err);
        setError(String(err));
      };
    },
  });

  if (appError) {
    return (
      <div className="error-container">
        <strong>Connection Error:</strong> {appError.message}
      </div>
    );
  }

  if (!app) {
    return <div className="loading">Connecting...</div>;
  }

  if (error) {
    return (
      <div className="error-container">
        <strong>Error:</strong> {error}
      </div>
    );
  }

  if (!lineageData) {
    return <div className="loading">Waiting for lineage data...</div>;
  }

  if (lineageData.nodes.length === 0) {
    return (
      <div className="error-container">
        <strong>No lineage data found</strong>
        <p>No nodes found for: {lineageData.targetId}</p>
        <p>The model may not exist or has no lineage connections.</p>
      </div>
    );
  }

  return (
    <div className="app-container">
      <LineageGraph
        data={lineageData}
        onNodeSelect={setSelectedNode}
        selectedNodeId={selectedNode?.uniqueId ?? null}
      />
      {selectedNode && (
        <NodeDetails
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
          app={app}
        />
      )}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <LineageApp />
  </StrictMode>
);
