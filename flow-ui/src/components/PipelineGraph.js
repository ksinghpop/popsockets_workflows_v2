import React, { useEffect, useMemo, useState } from "react";
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  MarkerType,
  applyNodeChanges,
} from "reactflow";
import "reactflow/dist/style.css";
import CustomNode from "./CustomNode";
import { getLayoutedElements } from "../layoutUtils";

export default function PipelineGraph({
  tasks,
  latestRun,
  layoutDirection,
  onNodeClick
}) {
  const nodeTypes = useMemo(() => ({ custom: CustomNode }), []);

  const taskStatusMap = useMemo(() => {
    const map = {};
    if (latestRun?.tasks) {
      latestRun.tasks.forEach((t) => {
        map[t.task_id] = t.status;
      });
    }
    return map;
  }, [latestRun]);

  // Build initial nodes and edges
  const rawNodes = useMemo(
    () =>
      (tasks || []).map((task) => ({
        id: String(task.id),
        type: "custom",
        data: {
          label: task.name,
          functionName: task.function_name,
          status: taskStatusMap[String(task.id)] || ""
        },
        position: { x: 0, y: 0 }
      })),
    [tasks, taskStatusMap]
  );

  const rawEdges = useMemo(
    () =>
      (tasks || []).flatMap((task) =>
        task.dependencies.map((depId) => ({
          id: `${depId}-${task.id}`,
          source: depId,
          target: task.id,
          type: "smoothstep",
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: "#000"
          },
          style: {
            // stroke: "#000",
            strokeWidth: 1
          }
        }))
      ),
    [tasks]
  );

  // Keep nodes in state to preserve positions after dragging
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);

  useEffect(() => {
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      rawNodes,
      rawEdges,
      layoutDirection,
      {
        ranksep: 100,
        nodesep: 50
      }
    );
    setNodes(layoutedNodes);
    setEdges(layoutedEdges);
  }, [rawNodes, rawEdges, layoutDirection]);

  // When user drags nodes, this keeps their new position
  const handleNodesChange = (changes) =>
    setNodes((nds) => applyNodeChanges(changes, nds));

  return (
    <div
      style={{
        width: "100%",
        height: "500px",
        border: "1px solid #ccc",
        marginTop: "1rem"
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        onNodeClick={onNodeClick}
        onNodesChange={handleNodesChange}
      >
        <MiniMap
          nodeColor={(node) => {
            const status = node.data?.status;
            if (status === "success") return "#28a745";
            if (status === "failed") return "#dc3545";
            if (status === "running") return "#ffc107";
            return "#999";
          }}
        />
        <Controls />
        <Background />
      </ReactFlow>
    </div>
  );
}
