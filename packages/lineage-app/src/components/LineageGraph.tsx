import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Edge,
  type Node,
  type NodeMouseHandler,
  type NodeTypes,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import dagre from "dagre";
import { useCallback, useEffect, useMemo } from "react";
import {
  RESOURCE_COLORS,
  type LineageData,
  type LineageNode,
  type ResourceType,
} from "../types";
import { LineageNode as LineageNodeComponent } from "./LineageNode";

interface LineageGraphProps {
  data: LineageData;
  onNodeSelect: (node: LineageNode | null) => void;
  selectedNodeId: string | null;
}

const NODE_WIDTH = 180;
const NODE_HEIGHT = 50;

const nodeTypes: NodeTypes = {
  lineage: LineageNodeComponent,
};

function getLayoutedElements(
  nodes: Node[],
  edges: Edge[]
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "LR", nodesep: 50, ranksep: 100 });

  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = g.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}

function convertToReactFlow(
  data: LineageData,
  selectedNodeId: string | null
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = data.nodes.map((node) => ({
    id: node.uniqueId,
    type: "lineage",
    position: { x: 0, y: 0 },
    data: {
      label: node.name,
      resourceType: node.resourceType,
      isTarget: node.uniqueId === data.targetId,
      isSelected: node.uniqueId === selectedNodeId,
    },
  }));

  const nodeIds = new Set(data.nodes.map((n) => n.uniqueId));
  const edges: Edge[] = [];

  data.nodes.forEach((node) => {
    (node.parentIds ?? []).forEach((parentId) => {
      if (nodeIds.has(parentId)) {
        edges.push({
          id: `${parentId}->${node.uniqueId}`,
          source: parentId,
          target: node.uniqueId,
          animated: false,
          style: { stroke: "#64748b", strokeWidth: 2 },
        });
      }
    });
  });

  return getLayoutedElements(nodes, edges);
}

export function LineageGraph({
  data,
  onNodeSelect,
  selectedNodeId,
}: LineageGraphProps) {
  const { nodes: initialNodes, edges: initialEdges } = useMemo(
    () => convertToReactFlow(data, selectedNodeId),
    [data, selectedNodeId]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    const { nodes: newNodes, edges: newEdges } = convertToReactFlow(
      data,
      selectedNodeId
    );
    setNodes(newNodes);
    setEdges(newEdges);
  }, [data, selectedNodeId, setNodes, setEdges]);

  const onNodeClick: NodeMouseHandler = useCallback(
    (_, node) => {
      const lineageNode = data.nodes.find((n) => n.uniqueId === node.id);
      if (lineageNode) {
        onNodeSelect(lineageNode);
      }
    },
    [data.nodes, onNodeSelect]
  );

  const onPaneClick = useCallback(() => {
    onNodeSelect(null);
  }, [onNodeSelect]);

  const minimapNodeColor = useCallback((node: Node) => {
    const resourceType = node.data?.resourceType as ResourceType | undefined;
    return resourceType ? RESOURCE_COLORS[resourceType] : "#64748b";
  }, []);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onNodeClick={onNodeClick}
      onPaneClick={onPaneClick}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.2 }}
      minZoom={0.1}
      maxZoom={2}
    >
      <Background color="#334155" gap={20} />
      <Controls />
      <MiniMap
        nodeColor={minimapNodeColor}
        maskColor="rgba(0, 0, 0, 0.8)"
        style={{ backgroundColor: "#1e293b" }}
      />
    </ReactFlow>
  );
}
