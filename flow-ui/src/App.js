import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Routes, Route, Link, Navigate } from "react-router-dom";
import PipelineList from "./components/PipelineList";
import PipelineDetail from "./components/PipelineDetail";
import EditPipeline from "./components/EditPipeline";
import NewPipeline from "./components/NewPipeline";
import LoginPage from "./components/LoginPage";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import "bootstrap/dist/css/bootstrap.min.css";
import logo from "./assets/Popsockets_Logo.jpg";
import "./App.css";
import UserManagement from "./components/UserManagement";

function App() {
  const [user, setUser] = useState(null);
  const [loadingUser, setLoadingUser] = useState(true);

  useEffect(() => {
    fetch(`${process.env.REACT_APP_API_BASE_URL}/auth/me`, {
      credentials: "include",
    })
      .then(async (res) => {
        if (res.ok) {
          const data = await res.json();
          setUser(data.user);
        }
      })
      .catch(() => {})
      .finally(() => setLoadingUser(false));
  }, []);

  const handleLogout = async () => {
    try {
      const res = await fetch(`${process.env.REACT_APP_API_BASE_URL}/auth/logout`, {
        method: "POST",
        credentials: "include",
      });
      if (res.ok) {
        setUser(null);
        toast.success("Logged out successfully.");
      } else {
        toast.error("Logout failed.");
      }
    } catch (err) {
      console.error(err);
      toast.error("Error logging out.");
    }
  };

  if (loadingUser) return <p className="text-center mt-5">Loading session...</p>;

  return (
    <Router>
      <div>
        {/* Navigation Bar */}
        <nav
          className="navbar navbar-expand-lg navbar-dark"
          style={{ backgroundColor: "#030303ff" }}
        >
          <div className="container-fluid">
            <Link className="navbar-brand" to="/">
              <img
                src={logo}
                alt="Logo"
                height="25"
                style={{ verticalAlign: "middle" }}
              />
            </Link>
            <button
              className="navbar-toggler"
              type="button"
              data-bs-toggle="collapse"
              data-bs-target="#navbarNav"
              aria-controls="navbarNav"
              aria-expanded="false"
              aria-label="Toggle navigation"
            >
              <span className="navbar-toggler-icon"></span>
            </button>
            <div className="collapse navbar-collapse" id="navbarNav">
              <ul className="navbar-nav me-auto">
                {user && (
                  <>
                    <li className="nav-item">
                      <Link className="nav-link text-white" to="/">Home</Link>
                    </li>
                    <li className="nav-item">
                      <Link className="nav-link text-white" to="/user-management">User Management</Link>
                    </li>
                    <li className="nav-item">
                      <Link className="nav-link text-white" to="/environment">Environment</Link>
                    </li>
                  </>
                )}
              </ul>
              <ul className="navbar-nav ms-auto align-items-center">
                {user ? (
                  <>
                    <li className="nav-item me-3">
                      <span
                        className="nav-link"
                        style={{
                          color: "#fff",
                          fontWeight: "500",
                          fontSize: "0.95rem"
                        }}
                      >
                        👤 {user.username}{" "}
                        <span style={{ opacity: 0.7 }}>({user.role})</span>
                      </span>
                    </li>
                    <li className="nav-item">
                      <button
                        className="btn btn-light btn-sm"
                        style={{
                          backgroundColor: "#fff",
                          color: "#000",
                          fontWeight: "500"
                        }}
                        onClick={handleLogout}
                      >
                        Logout
                      </button>
                    </li>
                  </>
                ) : (
                  <li className="nav-item">
                    <Link className="btn btn-sm btn-light" to="/login">
                      Login
                    </Link>
                  </li>
                )}
              </ul>
            </div>
          </div>
        </nav>

        {/* Main Content */}
        <div style={{ padding: "2rem" }}>
          <Routes>
            <Route path="/login" element={<LoginPage setUser={setUser} />} />
            <Route
              path="/"
              element={user ? <PipelineList user={user} /> : <Navigate to="/login" />}
            />
            <Route
              path="/pipelines/:id"
              element={user ? <PipelineDetail user={user} /> : <Navigate to="/login" />}
            />
            <Route
              path="/pipeline/new"
              element={
                user && user.role !== "viewer" ? (
                  <NewPipeline user={user} />
                ) : (
                  <Navigate to="/" />
                )
              }
            />
            <Route
              path="/pipeline/:id/edit"
              element={
                user && user.role !== "viewer" ? (
                  <EditPipeline user={user} />
                ) : (
                  <Navigate to="/" />
                )
              }
            />
            <Route
              path="/user-management"
              element={
                user && user.role === "admin" ? (
                  <UserManagement user={user} />
                ) : (
                  <h2>Access Denied</h2>
                )
              }
            />
            <Route
              path="/environment"
              element={
                user && user.role === "admin" ? (
                  <h2>Environment Page</h2>
                ) : (
                  <h2>Access Denied</h2>
                )
              }
            />
          </Routes>
        </div>

        <ToastContainer position="top-right" autoClose={3000} />
      </div>
    </Router>
    
  );
}

export default App;
