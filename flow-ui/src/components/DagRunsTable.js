import React, { useState } from "react";
import { FaTrash } from "react-icons/fa";
import { toast } from "react-toastify";
import TaskRunsModal from "./TaskRunsModal";
import Swal from "sweetalert2";


export default function DagRunsTable({
  allDagRuns,
  refreshDagRuns,
  page,
  setPage,
  perPage,
  setPerPage,
  sortOrder,
  setSortOrder,
  totalPages,
  user
}) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const [selectedRuns, setSelectedRuns] = useState([]);
  const [selectedDagRun, setSelectedDagRun] = useState(null);
  const [modalOpen, setModalOpen] = useState(false);

  const handleCheckboxChange = (runId) => {
    setSelectedRuns((prev) =>
      prev.includes(runId)
        ? prev.filter((id) => id !== runId)
        : [...prev, runId]
    );
  };

  const handleDeleteRun = async (runId) => {
    const result = await Swal.fire({
      title: "Are you sure?",
      text: "This DAG run will be permanently deleted.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#d33",
      cancelButtonColor: "#6c757d",
      confirmButtonText: "Yes, delete it",
    });

    if (!result.isConfirmed) return;

    try {
      const response = await fetch(`${API_BASE_URL}/dag_runs/${runId}`, {
        method: "DELETE",
      });
      if (response.ok) {
        refreshDagRuns();
        toast.success("DAG run deleted successfully.");
      } else {
        toast.error("Failed to delete DAG run.");
      }
    } catch (error) {
      console.error("Delete error:", error);
    }
  };

  const handleBulkDelete = async () => {
    if (user.role === "viewer") {
      Swal.fire({
        icon: "warning",
        title: "Access Denied",
        text: "Viewers cannot delete DAG runs.",
      });
      return;
    }

    if (selectedRuns.length === 0) return;

    const result = await Swal.fire({
      title: "Are you sure?",
      html: `Delete <strong>${selectedRuns.length}</strong> DAG run(s)?`,
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#d33",
      cancelButtonColor: "#6c757d",
      confirmButtonText: "Yes, delete all",
    });

    if (!result.isConfirmed) return;

    try {
      const results = await Promise.all(
        selectedRuns.map((runId) =>
          fetch(`${API_BASE_URL}/dag_runs/${runId}`, { method: "DELETE" })
        )
      );

      const failed = results.filter((r) => !r.ok);
      if (failed.length > 0) {
        toast.error(`Some deletes failed: ${failed.length} of ${results.length}`);
      } else {
        toast.success(`Deleted ${results.length} DAG runs successfully.`);
      }

      setSelectedRuns([]);
      refreshDagRuns();
    } catch (error) {
      console.error("Bulk delete error:", error);
    }
  };

  const allSelectedOnPage =
    allDagRuns?.length > 0 &&
    allDagRuns.every((run) => selectedRuns.includes(run.id));

  return (
    <div className="mt-3">
      {/* Top bar */}
      <div className="d-flex justify-content-between align-items-center mb-2">
        <div>
          <label className="me-2">
            Sort:
            <select
              className="form-select d-inline-block w-auto ms-1"
              value={sortOrder}
              onChange={(e) => {
                setPage(1);
                setSortOrder(e.target.value);
              }}
            >
              <option value="desc">Newest first</option>
              <option value="asc">Oldest first</option>
            </select>
          </label>
          <label className="ms-3">
            Page size:
            <select
              className="form-select d-inline-block w-auto ms-1"
              value={perPage}
              onChange={(e) => {
                setPerPage(Number(e.target.value));
                setPage(1); // ✅ Reset to first page
              }}
            >
              {[5, 10, 15, 20, 25, 50, 100, 150, 200].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </div>
        {selectedRuns.length > 0 && (
          <button
            onClick={handleBulkDelete}
            style={{
              background: "#dc3545",
              color: "#fff",
              border: "none",
              padding: "0.5rem 1rem",
              borderRadius: "4px",
              cursor: "pointer",
            }}
          >
            Delete ({selectedRuns.length})
          </button>
        )}
      </div>

      {/* Table */}
      <table className="table table-bordered table-striped">
        <thead className="table-light">
          <tr>
            <th>
              <input
                type="checkbox"
                checked={allSelectedOnPage}
                onChange={(e) => {
                  if (e.target.checked) {
                    setSelectedRuns((prev) => [
                      ...new Set([...prev, ...allDagRuns.map((r) => r.id)]),
                    ]);
                  } else {
                    setSelectedRuns((prev) =>
                      prev.filter(
                        (id) => !allDagRuns.map((r) => r.id).includes(id)
                      )
                    );
                  }
                }}
                disabled={!allDagRuns || allDagRuns.length === 0}
              />
            </th>
            <th>Run ID</th>
            <th>Started At</th>
            <th>Completed At</th>
            <th style={{ textAlign: "center" }}>Status</th>
            <th style={{ textAlign: "center" }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {allDagRuns && allDagRuns.length > 0 ? (
            allDagRuns.map((run) => (
              <tr key={run.id}>
                <td>
                  <input
                    type="checkbox"
                    checked={selectedRuns.includes(run.id)}
                    onChange={() => handleCheckboxChange(run.id)}
                  />
                </td>
                <td>
                  <span
                    onClick={() => {
                      setSelectedDagRun(run);
                      setModalOpen(true);
                    }}
                    title="View tasks"
                    className="dag-run-id"
                  >
                    {run.id}
                  </span>
                </td>
                <td>{new Date(run.started_at).toLocaleString()}</td>
                <td>
                  {run.completed_at
                    ? new Date(run.completed_at).toLocaleString()
                    : "-"}
                </td>
                <td style={{ textAlign: "center" }}>
                  {run.status === "success" && (
                    <span className="badge bg-success">Success</span>
                  )}
                  {run.status === "failed" && (
                    <span className="badge bg-danger">Failed</span>
                  )}
                  {run.status === "running" && (
                    <span className="badge bg-warning text-dark">Running</span>
                  )}
                </td>
                <td style={{ textAlign: "center" }}>
                  <FaTrash
                    onClick={() => {
                      if (user.role === "viewer") {
                        Swal.fire({
                          icon: "warning",
                          title: "Access Denied",
                          text: "Viewers cannot delete DAG runs.",
                        });
                        return;
                      }
                      handleDeleteRun(run.id);
                    }}
                    style={{ cursor: "pointer", color: "#dc3545" }}
                    title="Delete this run"
                  />
                </td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="6" className="text-center text-muted">
                No DAG runs available.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {/* Pagination */}
      <div className="d-flex justify-content-between align-items-center mt-2">
        <span className="ms-1 text-muted">
          Page {page} of {totalPages}
        </span>
        <div>
          <button
            className="btn btn-outline-primary btn-sm me-2"
            disabled={page === 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            Previous
          </button>
          <button
            className="btn btn-outline-primary btn-sm"
            disabled={page >= totalPages}
            onClick={() => setPage((p) => p + 1)}
          >
            Next
          </button>
        </div>
      </div>

      <TaskRunsModal
        isOpen={modalOpen}
        onRequestClose={() => setModalOpen(false)}
        dagRun={selectedDagRun}
      />
    </div>
  );
}