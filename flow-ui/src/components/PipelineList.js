import React, { useEffect, useState } from "react";
import PipelineTable from "./PipelineTable";
import { toast } from "react-toastify";
import { Link } from "react-router-dom";
import Swal from "sweetalert2";

function PipelineList({ user }) {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const [pipelines, setPipelines] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(10);
  const [sortOrder, setSortOrder] = useState("desc");
  const [totalPages, setTotalPages] = useState(1);

  const fetchPipelines = () => {
    setLoading(true);
    const query = new URLSearchParams({
      page: String(page),
      per_page: String(perPage),
      sort: sortOrder
    });
    fetch(`${API_BASE_URL}/pipelines?${query}`)
      .then((response) => response.json())
      .then((data) => {
        setPipelines(data.items);
        setTotalPages(data.total_pages);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Fetch error:", error);
        toast.error("Error fetching pipelines.");
        setLoading(false);
      });
  };

  useEffect(() => {
    fetchPipelines();
  }, [page, perPage, sortOrder]);

  return (
    <div>
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h2>Pipelines</h2>
        {user.role === "viewer" ? (
        <button
          className="btn btn-success"
          onClick={() =>
            Swal.fire("Access Denied", "Viewers cannot create pipelines.", "warning")
          }
        >
          + New Pipeline
        </button>
      ) : (
        <Link to="/pipeline/new" className="btn btn-success">
          + New Pipeline
        </Link>
      )}
      </div>
      {loading ? (
        <p>Loading...</p>
      ) : (
        <>
          <PipelineTable
            pipelines={pipelines}
            onRefresh={fetchPipelines}
            user={user}
          />
          <div className="d-flex justify-content-between mt-3">
            <div>
              <button
                className="btn btn-secondary me-2"
                disabled={page === 1}
                onClick={() => setPage((p) => p - 1)}
              >
                Previous
              </button>
              <button
                className="btn btn-secondary"
                disabled={page === totalPages}
                onClick={() => setPage((p) => p + 1)}
              >
                Next
              </button>
            </div>
            <div>
              <label>
                Items per page:
                <select
                  className="form-select d-inline-block w-auto ms-2"
                  value={perPage}
                  onChange={(e) => setPerPage(Number(e.target.value))}
                >
                  <option value={5}>5</option>
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                  <option value={150}>150</option>
                </select>
              </label>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default PipelineList;
