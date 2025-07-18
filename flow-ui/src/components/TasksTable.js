import React, { useState, useEffect } from "react";
import { FaTrash, FaEdit, FaPlus } from "react-icons/fa";
import { toast } from "react-toastify";
import Modal from "react-modal";
import Swal from "sweetalert2";

Modal.setAppElement("#root");

export default function TasksTable({ pipelineId, user }) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editingTask, setEditingTask] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [newParam, setNewParam] = useState({ key: "", value: "" });
  const [newDependency, setNewDependency] = useState("");

  const fetchTasks = () => {
    setLoading(true);
    fetch(`${API_BASE_URL}/tasks/list/${pipelineId}`)
      .then((r) => r.json())
      .then((data) => {
        setTasks(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Fetch error:", err);
        setLoading(false);
      });
  };

  useEffect(() => {
    fetchTasks();
  }, [pipelineId]);

  const handleDelete = async (taskId) => {
    const result = await Swal.fire({
      title: "Delete Task?",
      text: "Are you sure you want to delete this task? This action cannot be undone.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#d33",
      cancelButtonColor: "#6c757d",
      confirmButtonText: "Yes, delete it",
      reverseButtons: true,
    });

    if (!result.isConfirmed) return;

    try {
      const res = await fetch(`${API_BASE_URL}/tasks/${taskId}`, {
        method: "DELETE",
      });
      if (res.ok) {
        toast.success("Task deleted.");
        fetchTasks();
      } else {
        toast.error("Failed to delete.");
      }
    } catch (err) {
      console.error(err);
      toast.error("Error deleting task.");
    }
  };

  const handleEdit = (task) => {
    setEditingTask({
      ...task,
      dependencies: task.dependencies || [],
      params: task.params || {},
    });
    setShowForm(true);
    setNewParam({ key: "", value: "" });
    setNewDependency("");
  };

  const handleAdd = () => {
    setEditingTask({
      name: "",
      function_name: "",
      dependencies: [],
      params: {},
      order: 1,
    });
    setShowForm(true);
    setNewParam({ key: "", value: "" });
    setNewDependency("");
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const isUpdate = Boolean(editingTask.id);
    const method = isUpdate ? "PUT" : "POST";
    const url = isUpdate
      ? `${API_BASE_URL}/tasks/${editingTask.id}`
      : `${API_BASE_URL}/tasks/create/${pipelineId}`;

    const payload = {
      name: editingTask.name,
      function_name: editingTask.function_name,
      params: editingTask.params,
      dependencies: editingTask.dependencies,
      order: editingTask.order,
      ...(isUpdate ? { pipeline_id: pipelineId } : {}),
    };

    try {
      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        toast.success(`Task ${isUpdate ? "updated" : "created"}.`);
        fetchTasks();
        setShowForm(false);
      } else {
        toast.error(`Failed to ${isUpdate ? "update" : "create"} task.`);
      }
    } catch (err) {
      console.error(err);
      toast.error("Error saving task.");
    }
  };

  if (loading) return <p>Loading...</p>;

  return (
    <div className="mt-3">
      {user.role === "viewer" ? (
        <button
          className="btn btn-success mb-3"
          onClick={() =>
            Swal.fire("Access Denied", "Viewers cannot add tasks.", "warning")
          }
        >
          <FaPlus className="me-1" />
          Add Task
        </button>
      ) : (
        <button
          className="btn btn-success mb-3"
          onClick={handleAdd}
        >
          <FaPlus className="me-1" />
          Add Task
        </button>
      )}

      <table className="table table-bordered table-striped">
        <thead className="table-light">
          <tr>
            <th>Name</th>
            <th>Function</th>
            <th style={{ textAlign: "center" }}>Order</th>
            <th>Dependencies</th>
            <th style={{ textAlign: "center" }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.id}>
              <td>{t.name}</td>
              <td>{t.function_name}</td>
              <td style={{ textAlign: "center" }}>{t.order}</td>
              <td>
                {t.dependencies.length > 0
                  ? t.dependencies.join(", ")
                  : "-"}
              </td>
              <td style={{ textAlign: "center" }}>
                {user.role === "viewer" ? (
                  <>
                    <FaEdit
                      style={{ cursor: "pointer", color: "#0d6efd", marginRight: "0.5rem" }}
                      onClick={() =>
                        Swal.fire("Access Denied", "Viewers cannot edit tasks.", "warning")
                      }
                      title="Edit"
                    />
                    <FaTrash
                      style={{ cursor: "pointer", color: "#dc3545" }}
                      onClick={() =>
                        Swal.fire("Access Denied", "Viewers cannot delete tasks.", "warning")
                      }
                      title="Delete"
                    />
                  </>
                ) : (
                  <>
                    <FaEdit
                      style={{ cursor: "pointer", color: "#0d6efd", marginRight: "0.5rem" }}
                      onClick={() => handleEdit(t)}
                      title="Edit"
                    />
                    <FaTrash
                      style={{ cursor: "pointer", color: "#dc3545" }}
                      onClick={() => handleDelete(t.id)}
                      title="Delete"
                    />
                  </>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Modal for Add/Edit */}
      <Modal
        isOpen={showForm}
        onRequestClose={() => setShowForm(false)}
        contentLabel="Task Form"
        style={{
          content: {
            top: "50%",
            left: "50%",
            right: "auto",
            bottom: "auto",
            marginRight: "-50%",
            transform: "translate(-50%, -50%)",
            width: "500px",
          },
        }}
      >
        <h5>{editingTask?.id ? "Edit Task" : "Add Task"}</h5>
        <form onSubmit={handleSubmit}>
          <div className="mb-2">
            <label className="form-label">Name</label>
            <input
              className="form-control"
              value={editingTask?.name || ""}
              onChange={(e) =>
                setEditingTask({ ...editingTask, name: e.target.value })
              }
              required
            />
          </div>
          <div className="mb-2">
            <label className="form-label">Function Name</label>
            <input
              className="form-control"
              value={editingTask?.function_name || ""}
              onChange={(e) =>
                setEditingTask({ ...editingTask, function_name: e.target.value })
              }
              required
            />
          </div>
          <div className="mb-2">
            <label className="form-label">Order</label>
            <input
              type="number"
              className="form-control"
              value={editingTask?.order || 1}
              onChange={(e) =>
                setEditingTask({ ...editingTask, order: Number(e.target.value) })
              }
            />
          </div>
          <div className="mb-2">
            <label className="form-label">Dependencies</label>
            {editingTask?.dependencies?.map((dep, idx) => (
              <div key={idx} className="input-group mb-1">
                <input
                  className="form-control"
                  value={dep}
                  onChange={(e) => {
                    const deps = [...editingTask.dependencies];
                    deps[idx] = e.target.value;
                    setEditingTask({ ...editingTask, dependencies: deps });
                  }}
                />
                <button
                  type="button"
                  className="btn btn-outline-danger"
                  onClick={() => {
                    const deps = editingTask.dependencies.filter((_, i) => i !== idx);
                    setEditingTask({ ...editingTask, dependencies: deps });
                  }}
                >
                  ×
                </button>
              </div>
            ))}
            <div className="input-group mb-2">
              <input
                className="form-control"
                placeholder="Task ID"
                value={newDependency}
                onChange={(e) => setNewDependency(e.target.value)}
              />
              <button
                type="button"
                className="btn btn-outline-primary"
                onClick={() => {
                  if (newDependency.trim()) {
                    setEditingTask({
                      ...editingTask,
                      dependencies: [...editingTask.dependencies, newDependency.trim()],
                    });
                    setNewDependency("");
                  }
                }}
              >
                + Add
              </button>
            </div>
          </div>
          <div className="mb-2">
            <label className="form-label">Parameters</label>
            {Object.entries(editingTask?.params || {}).map(([key, val], idx) => (
              <div key={idx} className="input-group mb-1">
                <input
                  className="form-control"
                  placeholder="Key"
                  value={key}
                  disabled
                />
                <input
                  className="form-control"
                  placeholder="Value"
                  value={val}
                  onChange={(e) => {
                    const newParams = { ...editingTask.params, [key]: e.target.value };
                    setEditingTask({ ...editingTask, params: newParams });
                  }}
                />
                <button
                  type="button"
                  className="btn btn-outline-danger"
                  onClick={() => {
                    const { [key]: _, ...rest } = editingTask.params;
                    setEditingTask({ ...editingTask, params: rest });
                  }}
                >
                  ×
                </button>
              </div>
            ))}
            <div className="input-group mb-2">
              <input
                className="form-control"
                placeholder="New Param Key"
                value={newParam.key}
                onChange={(e) => setNewParam({ ...newParam, key: e.target.value })}
              />
              <input
                className="form-control"
                placeholder="New Param Value"
                value={newParam.value}
                onChange={(e) => setNewParam({ ...newParam, value: e.target.value })}
              />
              <button
                type="button"
                className="btn btn-outline-primary"
                onClick={() => {
                  if (newParam.key.trim()) {
                    setEditingTask({
                      ...editingTask,
                      params: {
                        ...editingTask.params,
                        [newParam.key.trim()]: newParam.value,
                      },
                    });
                    setNewParam({ key: "", value: "" });
                  }
                }}
              >
                + Add
              </button>
            </div>
          </div>
          <div className="d-flex justify-content-between mt-3">
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setShowForm(false)}
            >
              Cancel
            </button>
            <button type="submit" className="btn btn-primary">
              Save
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
