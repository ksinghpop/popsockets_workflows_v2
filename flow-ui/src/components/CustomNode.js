import React from "react";
import { Handle, Position } from "reactflow";
import { FaCheckCircle, FaTimesCircle, FaSpinner, FaRegCircle } from "react-icons/fa";

export default function CustomNode({ data }) {
  // Determine background color based on status
  let bgColor = "#f0f0f0";
  let borderColor = "#999";
  let statusIcon = <FaRegCircle color="#999" />;

  if (data.status === "success") {
    bgColor = "#d4edda"; // light green
    borderColor = "#28a745"; // green border
    statusIcon = <FaCheckCircle color="#28a745" />;
  } else if (data.status === "failed") {
    bgColor = "#f8d7da"; // light red
    borderColor = "#dc3545"; // red border
    statusIcon = <FaTimesCircle color="#dc3545" />;
  } else if (data.status === "running") {
    bgColor = "#fff3cd"; // light yellow
    borderColor = "#ffc107"; // yellow border
    statusIcon = <FaSpinner color="#ffc107" />;
  }

  return (
    <div
      style={{
        padding: "10px",
        border: `2px solid ${borderColor}`,
        borderRadius: "6px",
        background: bgColor,
        minWidth: "140px",
        textAlign: "center",
        fontSize: "12px"
      }}
    >
      <div style={{ fontWeight: "bold", marginBottom: "4px" }}>
        {statusIcon} {data.label}
      </div>
      <div
        style={{
          fontSize: "10px",
          color: "#666",
          wordBreak: "break-all"
        }}
      >
        {data.functionName}
      </div>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}
