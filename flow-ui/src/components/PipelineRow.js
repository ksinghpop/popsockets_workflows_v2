import React from "react";
import { Link } from "react-router-dom";
import { toast } from "react-toastify";
import Swal from "sweetalert2";

function PipelineRow( { pipeline, onRefresh, user } ) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const handleDelete = () => {
    Swal.fire({
      title: "Delete Pipeline?",
      text: "Are you sure you want to delete this pipeline? This action cannot be undone.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#d33",
      cancelButtonColor: "#6c757d",
      confirmButtonText: "Yes, delete it",
      reverseButtons: true
    }).then((result) => {
      if (result.isConfirmed) {
        fetch(`${API_BASE_URL}/pipelines/${pipeline.id}`, {
          method: "DELETE"
        })
          .then((res) => {
            if (res.ok) {
              toast.success("Pipeline deleted.");
              onRefresh();
            } else {
              toast.error("Failed to delete pipeline.");
            }
          })
          .catch((err) => {
            console.error("Delete error:", err);
            toast.error("Error deleting pipeline.");
          });
      }
    });
  };

  const handleToggleEnabled = () => {
    fetch(`${API_BASE_URL}/pipelines/${pipeline.id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: pipeline.name,
        description: pipeline.description,
        schedule: pipeline.schedule,
        enabled: !pipeline.enabled
      })
    })
      .then((res) => {
        if (res.ok) {
          toast.success(`Pipeline ${!pipeline.enabled ? "enabled" : "disabled"}.`);
          onRefresh();
        } else {
          toast.error("Failed to update pipeline.");
        }
      })
      .catch((err) => {
        console.error("Toggle error:", err);
        toast.error("Error updating pipeline.");
      });
  };

  return (
    <tr>
      <td className="text-center">
        <div className="form-check form-switch d-flex justify-content-center">
          <input
            className="form-check-input"
            type="checkbox"
            checked={pipeline.enabled}
            onChange={handleToggleEnabled}
          />
        </div>
      </td>
      <td >
        <Link to={`/pipelines/${pipeline.id}`} className="pipeline-name-link">{pipeline.name} </Link>
      </td>
      <td className="text-center">
        {pipeline.schedule ? (
          <span className="badge bg-secondary">
            {pipeline.schedule}
          </span>
        ) : "-"}
      </td>
      <td className="text-center">
        {pipeline.latest_dag_run_status ? (
          pipeline.latest_dag_run_status === "success" ? (
            <span className="badge bg-success">Success</span>
          ) : pipeline.latest_dag_run_status === "failed" ? (
            <span className="badge bg-danger">Failed</span>
          ) : (
            <span className="badge bg-warning text-dark">
              {pipeline.latest_dag_run_status}
            </span>
          )
        ) : "-"}
      </td>
      <td className="text-center">
        {pipeline.total_dag_runs !== undefined ? (
          <span className={`badge ${pipeline.total_dag_runs > 0 ? "bg-secondary" : "bg-light text-dark"}`}>
            {pipeline.total_dag_runs}
          </span>
        ) : "-"}
      </td>
      <td className="text-center">
        {pipeline.failed_dag_runs !== undefined ? (
          <span className={`badge ${pipeline.failed_dag_runs > 0 ? "bg-danger" : "bg-secondary"}`}>
            {pipeline.failed_dag_runs}
          </span>
        ) : "-"}
      </td>
      <td className="text-center">
        {user.role === "viewer" ? (
          <>
            <button
              className="btn btn-sm btn-primary me-2"
              onClick={() =>
                Swal.fire("Access Denied", "Viewers cannot edit pipelines.", "warning")
              }
            >
              Edit
            </button>
            <button
              className="btn btn-sm btn-danger"
              onClick={() =>
                Swal.fire("Access Denied", "Viewers cannot delete pipelines.", "warning")
              }
            >
              Delete
            </button>
          </>
        ) : (
          <>
            <Link
              to={`/pipeline/${pipeline.id}/edit`}
              state={{ fromList: true }}
              className="btn btn-sm btn-primary me-2"
            >
              Edit
            </Link>
            <button
              className="btn btn-sm btn-danger"
              onClick={handleDelete}
            >
              Delete
            </button>
          </>
        )}
      </td>
    </tr>
  );
}

export default PipelineRow;
