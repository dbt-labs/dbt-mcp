import { Handle, Position } from "@xyflow/react";
import { RESOURCE_COLORS, type ResourceType } from "../types";

interface LineageNodeData {
  label: string;
  resourceType: ResourceType;
  isTarget: boolean;
  isSelected: boolean;
}

interface LineageNodeProps {
  data: LineageNodeData;
}

export function LineageNode({ data }: LineageNodeProps) {
  const color = RESOURCE_COLORS[data.resourceType] ?? "#64748b";
  const isHighlighted = data.isTarget || data.isSelected;

  return (
    <>
      <Handle type="target" position={Position.Left} />
      <div
        className="lineage-node"
        style={{
          backgroundColor: color,
          borderColor: isHighlighted ? "#fff" : color,
          borderWidth: isHighlighted ? 3 : 1,
          boxShadow: isHighlighted
            ? `0 0 10px ${color}, 0 0 20px ${color}40`
            : "none",
        }}
      >
        <div className="lineage-node-type">{data.resourceType}</div>
        <div className="lineage-node-label">{data.label}</div>
      </div>
      <Handle type="source" position={Position.Right} />
    </>
  );
}
