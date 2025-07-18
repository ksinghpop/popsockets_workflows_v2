import React, { useEffect, useState } from "react";
import { useParams, Link, useNavigate, useLocation } from "react-router-dom";
import PipelineGraph from "./PipelineGraph";
import DagRunsTable from "./DagRunsTable";
import TaskLogsModal from "./TaskLogsModal";
import TasksTable from "./TasksTable";
import Modal from "react-modal";
import { toast } from "react-toastify";
import { FaArrowLeft } from "react-icons/fa";
import Swal from "sweetalert2";

Modal.setAppElement("#root");

function PipelineDetail({ user }) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [pipeline, setPipeline] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [latestRun, setLatestRun] = useState(null);
  const [allDagRuns, setAllDagRuns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [layoutDirection, setLayoutDirection] = useState("TB");
  const [selectedTask, setSelectedTask] = useState(null);
  const [modalIsOpen, setModalIsOpen] = useState(false);
  // const [activeTab, setActiveTab] = useState("graph");
  const [activeTab, setActiveTab] = useState(localStorage.getItem("pipelineActiveTab") || "graph");
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(50);
  const [sortOrder, setSortOrder] = useState("desc");
  const [dagRunsIntervalId, setDagRunsIntervalId] = useState(null);
  const [totalPages, setTotalPages] = useState(1);
  
  const refreshAllData = () => {
    const query = new URLSearchParams({
      page: String(page),
      per_page: String(perPage),
      sort: sortOrder
    });

    fetch(`${API_BASE_URL}/dag_runs/${id}?${query}`)
      .then((response) => response.json())
      .then((data) => {
        setAllDagRuns(data.items);
        setTotalPages(data.total_pages || 1); // ✅ Important
      })
      .catch((error) => console.error("Error fetching all DAG runs:", error));

    fetch(`${API_BASE_URL}/dag_runs/${id}/latest`)
      .then((response) => response.json())
      .then((data) => {
        setLatestRun(data?.id ? data : null);
      })
      .catch((error) => console.error("Error fetching latest run:", error));
  };

  const handleTabClick = (tabName) => {
    setActiveTab(tabName);
    localStorage.setItem("pipelineActiveTab", tabName);

    // Clear any existing interval
    if (dagRunsIntervalId) {
      clearInterval(dagRunsIntervalId);
      setDagRunsIntervalId(null);
    }

    // Refresh based on tab
    if (tabName === "graph" || tabName === "tasks") {
      // Re-fetch tasks and latest run
      fetch(`${API_BASE_URL}/tasks/list/${id}`)
        .then((res) => res.json())
        .then(setTasks)
        .catch((err) => console.error("Tasks fetch error:", err));

      fetch(`${API_BASE_URL}/dag_runs/${id}/latest`)
        .then((res) => res.json())
        .then((data) => setLatestRun(data?.id ? data : null))
        .catch((err) => console.error("Latest DAG run fetch error:", err));
    }

    if (tabName === "history") {
      // Fetch immediately
      refreshAllData();
      // Start polling every 5 seconds
      const intervalId = setInterval(() => {
        refreshAllData();
      }, 1000);
      setDagRunsIntervalId(intervalId);
    }
  };

  // Fetch pipeline details
  useEffect(() => {
    fetch(`${API_BASE_URL}/pipelines/${id}`)
      .then((res) => res.json())
      .then(setPipeline)
      .catch((err) => console.error("Pipeline fetch error:", err));
  }, [id, location.key]);

  // Fetch tasks
  useEffect(() => {
    fetch(`${API_BASE_URL}/tasks/list/${id}`)
      .then((res) => res.json())
      .then((data) => {
        setTasks(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Tasks fetch error:", err);
        setLoading(false);
      });
  }, [id, location.key]);

  // Fetch latest DAG run
  useEffect(() => {
    fetch(`${API_BASE_URL}/dag_runs/${id}/latest`)
      .then((res) => res.json())
      .then((data) => setLatestRun(data?.id ? data : null))
      .catch((err) => console.error("Latest DAG run fetch error:", err));
  }, [id, location.key]);

  // ✅ Fetch all DAG runs for the pipeline
  useEffect(() => {
    if (activeTab === "history") {
      const timeoutId = setTimeout(() => {
        refreshAllData();
      }, 200);

      return () => clearTimeout(timeoutId);
    }
  }, [id, page, perPage, sortOrder, activeTab, location.key]);

  // Trigger pipeline run
  const handleRunPipeline = () => {
    fetch(`${API_BASE_URL}/pipelines/${id}/run`, { method: "POST" })
      .then((res) => {
        if (res.ok) {
          // Re-fetch latest run
          fetch(`${API_BASE_URL}/dag_runs/${id}/latest`)
            .then((r) => r.json())
            .then((d) => setLatestRun(d?.id ? d : null));
          refreshAllData();
          toast.success("Pipeline triggered successfully!");
        } else {
          toast.error("Failed to trigger pipeline.");
        }
      })
      .catch((err) => {
        console.error("Pipeline run error:", err);
        toast.error("Error triggering pipeline.");
      });
  };

  // Handle node click
  const handleNodeClick = (event, node) => {
    if (!latestRun) return;
    const taskLog = latestRun.tasks.find((t) => t.task_id === node.id);
    setSelectedTask({
      name: node.data.label,
      logs: taskLog ? taskLog.logs : "No logs available."
    });
    setModalIsOpen(true);
  };

  useEffect(() => {
    return () => {
      // Clean up on unmount
      if (dagRunsIntervalId) {
        clearInterval(dagRunsIntervalId);
      }
    };
  }, [dagRunsIntervalId]);

  if (loading) return <p>Loading...</p>;
  if (!pipeline) return <p>Pipeline not found.</p>;


  return (
    <div>
      <Link to="/" className="btn btn-outline-secondary mb-3">
        <FaArrowLeft className="me-2" />
        Back
      </Link>
      <h2 style={{color:"#0d0d64ff"}} >Pipeline Detail</h2>

      <div className="card mt-3" style={{ width: "100%", backgroundColor: "#f8f9fa" }}>
        <div className="card-body">
          <h5 className="card-title text-primary">{pipeline.name}</h5>
          <p className="mb-2">
            <strong>Description:</strong><br />
            {pipeline.description}
          </p>
          <p className="mb-2">
            <strong>Schedule:</strong><br />
            <code>{pipeline.schedule}</code>
          </p>
          <p className="mb-0">
            <strong>Enabled:</strong><br />
            {pipeline.enabled ? (
              <span className="badge bg-success">Enabled</span>
            ) : (
              <span className="badge bg-secondary">Disabled</span>
            )}
          </p>
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", margin: "0.5rem 0" }}>
        {user.role === "viewer" ? (
          <button
            className="btn btn-primary me-2"
            onClick={() =>
              Swal.fire("Access Denied", "Viewers cannot run pipelines.", "warning")
            }
          >
            Run Pipeline
          </button>
        ) : (
          <button className="btn btn-primary me-2" onClick={handleRunPipeline}>
            Run Pipeline
          </button>
        )}
        <button
          className="btn btn-secondary"
          onClick={() => {
            if (user.role === "viewer") {
              Swal.fire({
                icon: "warning",
                title: "Access Denied",
                text: "Viewers cannot edit pipelines.",
              });
              return;
            }
            navigate(`/pipeline/${pipeline.id}/edit`);
          }}
        >
          Edit Pipeline
        </button>
      </div>

      <ul className="nav nav-tabs mt-3">
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === "graph" ? "active" : ""}`}
            onClick={() => handleTabClick("graph")}
          >
            Graph
          </button>
        </li>
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === "tasks" ? "active" : ""}`}
            onClick={() => handleTabClick("tasks")}
          >
            Tasks
          </button>
        </li>
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === "history" ? "active" : ""}`}
            onClick={() => handleTabClick("history")}
          >
            Dag Runs
          </button>
        </li>
      </ul>

      {activeTab === "graph" && (
        <>
          <div className="mt-3">
            <label className="form-label">
              <strong>Layout Direction:</strong>
            </label>
            <select
              className="form-select"
              id="layoutDirection"
              value={layoutDirection}
              onChange={(e) => setLayoutDirection(e.target.value)}
            >
              <option value="TB">Vertical (Top to Bottom)</option>
              <option value="LR">Horizontal (Left to Right)</option>
            </select>
          </div>
          <PipelineGraph
            tasks={tasks}
            latestRun={latestRun}
            layoutDirection={layoutDirection}
            onNodeClick={handleNodeClick}
          />
        </>
      )}

      {activeTab === "history" && <DagRunsTable 
                                      allDagRuns={allDagRuns}
                                      refreshDagRuns={refreshAllData}
                                      page={page}
                                      setPage={setPage}
                                      perPage={perPage}
                                      setPerPage={setPerPage}
                                      sortOrder={sortOrder}
                                      setSortOrder={setSortOrder}
                                      totalPages={totalPages}
                                      user={user}
                                    />
                                  }

      <TaskLogsModal
        isOpen={modalIsOpen}
        onRequestClose={() => setModalIsOpen(false)}
        task={selectedTask}
      />

      <div style={{ display: activeTab === "tasks" ? "block" : "none" }}>
        <TasksTable pipelineId={id} user={user} />
      </div>
    </div>
  );
}

export default PipelineDetail;
