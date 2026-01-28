import type { App } from "@modelcontextprotocol/ext-apps";
import { useCallback, useState } from "react";
import { RESOURCE_COLORS, type LineageNode } from "../types";

interface NodeDetailsProps {
  node: LineageNode;
  onClose: () => void;
  app: App;
}

export function NodeDetails({ node, onClose, app }: NodeDetailsProps) {
  const [details, setDetails] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const color = RESOURCE_COLORS[node.resourceType] ?? "#64748b";

  const loadDetails = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const toolName = getDetailsToolName(node.resourceType);
      if (!toolName) {
        setError(`No details tool for ${node.resourceType}`);
        return;
      }

      const result = await app.callServerTool({
        name: toolName,
        arguments: { unique_id: node.uniqueId },
      });

      if (result.isError) {
        setError("Failed to load details");
        return;
      }

      const content = result.content?.find((c) => c.type === "text");
      if (content && "text" in content) {
        const parsed = JSON.parse(content.text);
        setDetails(Array.isArray(parsed) ? parsed[0] : parsed);
      }
    } catch (e) {
      console.error("Error loading details:", e);
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [app, node]);

  return (
    <div className="node-details">
      <div className="node-details-header" style={{ backgroundColor: color }}>
        <span className="node-details-type">{node.resourceType}</span>
        <button className="node-details-close" onClick={onClose}>
          &times;
        </button>
      </div>
      <div className="node-details-content">
        <h3>{node.name}</h3>
        <div className="node-details-field">
          <label>Unique ID</label>
          <code>{node.uniqueId}</code>
        </div>
        {node.parentIds && node.parentIds.length > 0 && (
          <div className="node-details-field">
            <label>Dependencies ({node.parentIds.length})</label>
            <ul className="node-details-parents">
              {node.parentIds.map((id) => (
                <li key={id}>
                  <code>{id}</code>
                </li>
              ))}
            </ul>
          </div>
        )}

        {!details && !loading && !error && (
          <button className="load-details-btn" onClick={loadDetails}>
            Load More Details
          </button>
        )}

        {loading && <div className="loading-small">Loading details...</div>}

        {error && <div className="error-small">{error}</div>}

        {details && (
          <div className="node-details-extra">
            <label>Additional Details</label>
            <pre>{JSON.stringify(details, null, 2)}</pre>
          </div>
        )}
      </div>
    </div>
  );
}

function getDetailsToolName(resourceType: string): string | null {
  const toolMap: Record<string, string> = {
    Model: "get_model_details",
    Source: "get_source_details",
    Exposure: "get_exposure_details",
    Seed: "get_seed_details",
    Snapshot: "get_snapshot_details",
    Test: "get_test_details",
    SemanticModel: "get_semantic_model_details",
    Macro: "get_macro_details",
  };
  return toolMap[resourceType] ?? null;
}
