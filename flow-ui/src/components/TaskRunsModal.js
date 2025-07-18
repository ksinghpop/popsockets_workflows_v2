import React, { useEffect, useState } from "react";
import Modal from "react-modal";
import { FaEye } from "react-icons/fa";

export default function TaskRunsModal({ isOpen, onRequestClose, dagRun }) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const [taskMetadata, setTaskMetadata] = useState([]);
  const [selectedLog, setSelectedLog] = useState(null);

  useEffect(() => {
    if (isOpen && dagRun?.pipeline_id) {
      // Fetch task metadata for names/functions
      fetch(`${API_BASE_URL}/tasks/list/${dagRun.pipeline_id}`)
        .then((res) => res.json())
        .then(setTaskMetadata)
        .catch((err) => console.error("Error fetching task metadata:", err));
    }
  }, [isOpen, dagRun?.pipeline_id]);

  if (!dagRun) return null;

  // Map metadata for quick lookup
  const taskMetaMap = {};
  taskMetadata.forEach((t) => {
    taskMetaMap[t.id] = t;
  });

  return (
    <Modal
      isOpen={isOpen}
      onRequestClose={onRequestClose}
      contentLabel="DAG Run Tasks"
      // style={{
      //   content: {
      //     top: "50%",
      //     left: "50%",
      //     right: "auto",
      //     bottom: "auto",
      //     marginRight: "-50%",
      //     transform: "translate(-50%, -50%)",
      //     maxHeight: "80vh",
      //     width: "800px",
      //   },
      // }}
    >
      <h3>Tasks for Run ID {dagRun.id}</h3>
      <table className="table table-bordered mt-3">
        <thead className="table-light">
          <tr>
            <th>Task Name</th>
            <th>Function Name</th>
            <th style={{ textAlign: "center" }}>Status</th>
            <th>Started At</th>
            <th>Completed At</th>
            <th style={{ textAlign: "center" }}>Logs</th>
          </tr>
        </thead>
        <tbody>
          {dagRun.tasks.map((task) => {
            const meta = taskMetaMap[task.task_id] || {};
            return (
              <tr key={task.task_id}>
                <td>{meta.name || "-"}</td>
                <td>{meta.function_name || "-"}</td>
                <td style={{ textAlign: "center" }}>
                  {task.status === "success" && (
                    <span className="badge bg-success">Success</span>
                  )}
                  {task.status === "failed" && (
                    <span className="badge bg-danger">Failed</span>
                  )}
                  {task.status === "running" && (
                    <span className="badge bg-warning text-dark">Running</span>
                  )}
                </td>
                <td>
                  {task.started_at
                    ? new Date(task.started_at).toLocaleString()
                    : "-"}
                </td>
                <td>
                  {task.completed_at
                    ? new Date(task.completed_at).toLocaleString()
                    : "-"}
                </td>
                <td className="text-center">
                  <button
                    className="btn btn-outline-primary btn-sm"
                    onClick={() =>
                      setSelectedLog(task.logs || "No logs available")
                    }
                    title="View Logs"
                  >
                    <FaEye />
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <div className="text-end">
        <button onClick={onRequestClose} className="btn btn-secondary">
          Close
        </button>
      </div>

      {/* Logs Modal */}
      {selectedLog && (
        <Modal
          isOpen={!!selectedLog}
          onRequestClose={() => setSelectedLog(null)}
          contentLabel="Task Logs"
          // style={{
          //   content: {
          //     top: "50%",
          //     left: "50%",
          //     right: "auto",
          //     bottom: "auto",
          //     marginRight: "-10%",
          //     transform: "translate(-50%, -50%)",
          //     maxHeight: "70vh",
          //     width: "600px",
          //     padding: "20px",
          //   },
          // }}
        >
          <h4>Task Logs</h4>
          <pre
            style={{
              backgroundColor: "#f5f5f5",
              padding: "10px",
              maxHeight: "50vh",
              overflowY: "auto",
              whiteSpace: "pre-wrap",
              border: "1px solid #ccc",
            }}
          >
            {selectedLog}
          </pre>
          <div className="text-end">
            <button
              onClick={() => setSelectedLog(null)}
              className="btn btn-secondary mt-3"
            >
              Close
            </button>
          </div>
        </Modal>
      )}
    </Modal>
  );
}
