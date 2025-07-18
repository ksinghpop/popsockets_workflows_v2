import React, { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { toast } from "react-toastify";
import { FaArrowLeft } from "react-icons/fa";

function NewPipeline() {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;
  const navigate = useNavigate();
  const [pipeline, setPipeline] = useState({
    name: "",
    description: "",
    schedule: "",
    enabled: true,
  });

  const handleSubmit = (e) => {
    e.preventDefault();

    fetch(`${API_BASE_URL}/pipelines`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(pipeline),
    })
      .then((res) => res.json())
      .then((data) => {
        if (data.id) {
          toast.success("Pipeline created.");
          navigate("/");
        } else {
          toast.error("Failed to create pipeline.");
        }
      })
      .catch((err) => {
        console.error("Error creating pipeline:", err);
        toast.error("Error creating pipeline.");
      });
  };

  return (
    <div className="container mt-4">
      <div className="d-flex align-items-center mb-3">
        <Link to="/" className="btn btn-outline-secondary me-2">
          <FaArrowLeft className="me-1" />
          Back
        </Link>
        <h2 className="mb-0" style={{ color: "#022542ff" }}>
          New Pipeline
        </h2>
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
                  onChange={(e) =>
                    setPipeline({ ...pipeline, name: e.target.value })
                  }
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
              Create Pipeline
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default NewPipeline;
