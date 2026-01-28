export type ResourceType =
  | "Source"
  | "Seed"
  | "Model"
  | "Snapshot"
  | "Test"
  | "Exposure"
  | "Metric"
  | "SemanticModel"
  | "SavedQuery";

export interface LineageNode {
  uniqueId: string;
  name: string;
  resourceType: ResourceType;
  parentIds?: string[];
}

export interface LineageData {
  targetId: string;
  nodes: LineageNode[];
}

export const RESOURCE_COLORS: Record<ResourceType, string> = {
  Source: "#10b981", // green
  Seed: "#84cc16", // lime
  Model: "#3b82f6", // blue
  Snapshot: "#8b5cf6", // purple
  Test: "#f97316", // orange
  Exposure: "#ec4899", // pink
  Metric: "#06b6d4", // cyan
  SemanticModel: "#6366f1", // indigo
  SavedQuery: "#a855f7", // violet
};
