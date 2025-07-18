import React, { useState, useEffect } from "react";
import { useParams, useNavigate, useLocation, Link } from "react-router-dom";
import { FaArrowLeft } from "react-icons/fa";

function EditPipeline() {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();

  const [pipeline, setPipeline] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/pipelines/${id}`)
      .then((response) => response.json())
      .then((data) => {
        setPipeline(data);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error fetching pipeline:", error);
        setLoading(false);
      });
  }, [id]);

  const handleSubmit = (e) => {
    e.preventDefault();
    fetch(`${API_BASE_URL}/pipelines/${id}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: pipeline.name,
        description: pipeline.description,
        schedule: pipeline.schedule,
        enabled: pipeline.enabled,
      }),
    })
      .then((response) => {
        if (response.ok) {
          if (location.state?.fromList) {
            navigate("/");
          } else {
            navigate(`/pipelines/${id}`);
          }
        } else {
          alert("Failed to update pipeline.");
        }
      })
      .catch((error) => {
        console.error("Error updating pipeline:", error);
        alert("Error updating pipeline.");
      });
  };

  if (loading) return <p>Loading...</p>;
  if (!pipeline) return <p>Pipeline not found.</p>;

  return (
    <div className="container mt-4">
      <div className="d-flex align-items-center mb-3">
        {location.state?.fromList ? (
          <Link to="/" className="btn btn-outline-secondary me-2">
            <FaArrowLeft className="me-1" />
            Back to List
          </Link>
        ) : (
          <Link to={`/pipelines/${id}`} className="btn btn-outline-secondary me-2">
            <FaArrowLeft className="me-1" />
            Back to Detail
          </Link>
        )}
        <h2 className="mb-0" style={{ color: "#022542ff" }}>Edit Pipeline</h2>
      </div>

      <div className="card shadow-sm" style={{ color: "#b94803ff" }}>
        <div className="card-body" style={{ backgroundColor: "#f8f9fa" }}>
          <form onSubmit={handleSubmit}>
            <div className="mb-3">
              <label className="form-label">
                Name
                <input
                  type="text"
                  className="form-control"
                  value={pipeline.name}
                  onChange={(e) => setPipeline({ ...pipeline, name: e.target.value })}
                  required
                  style={{ resize: "both", minWidth: "400px" }}
                />
              </label>
            </div>

            <div className="mb-3">
              <label className="form-label">
                Description
                <textarea
                  className="form-control"
                  rows="3"
                  value={pipeline.description}
                  onChange={(e) =>
                    setPipeline({ ...pipeline, description: e.target.value })
                  }
                  style={{ resize: "both", minWidth: "400px" }}
                />
              </label>
            </div>

            <div className="mb-3">
              <label className="form-label">
                Schedule (Cron Expression)
                <input
                  type="text"
                  className="form-control"
                  value={pipeline.schedule}
                  onChange={(e) =>
                    setPipeline({ ...pipeline, schedule: e.target.value })
                  }
                  required
                  style={{ resize: "both", minWidth: "400px" }}
                />
              </label>
            </div>

            <div className="form-check mb-3">
              <input
                className="form-check-input"
                type="checkbox"
                id="enabledCheckbox"
                checked={pipeline.enabled}
                onChange={(e) =>
                  setPipeline({ ...pipeline, enabled: e.target.checked })
                }
              />
              <label className="form-check-label" htmlFor="enabledCheckbox">
                Enabled
              </label>
            </div>

            <button type="submit" className="btn btn-success">
              Save Changes
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default EditPipeline;
