import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { toast } from "react-toastify";

export default function LoginPage({ setUser }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();

    try {
      const response = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/auth/login`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({ username, password }),
        }
      );

      console.log("Response status:", response.status);
      const text = await response.text();
      console.log("Response body:", text);

      if (!response.ok) {
        toast.error("Invalid credentials");
        return;
      }

      await new Promise((resolve) => setTimeout(resolve, 100));

      const meRes = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/auth/me`,
        {
          credentials: "include",
        }
      );

      if (meRes.ok) {
        const meData = await meRes.json();
        console.log("Session user data:", meData);
        setUser(meData.user);
        toast.success(`Welcome ${meData.user.username}`);
        navigate("/");
      } else {
        toast.error("Login succeeded but could not load user data.");
      }
    } catch (err) {
      console.error("Login error", err);
      toast.error("Error logging in");
    }
  };

  return (
    <div
      className="container mt-5 p-4"
      style={{
        maxWidth: "400px",
        border: "1px solid #ddd",
        borderRadius: "8px",
        backgroundColor: "#f9f9f9",
        boxShadow: "0 2px 8px rgba(0,0,0,0.05)"
      }}
    >
      <h3
        className="mb-4 text-center"
        style={{
          fontWeight: "500",
          color: "#333"
        }}
      >
        Login
      </h3>
      <form onSubmit={handleSubmit}>
        <div className="mb-3">
          <label className="form-label">Username</label>
          <input
            className="form-control"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
          />
        </div>
        <div className="mb-3">
          <label className="form-label">Password</label>
          <input
            type="password"
            className="form-control"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </div>
        <button
          className="btn w-100"
          style={{
            backgroundColor: "#000",
            color: "#fff",
            fontWeight: "500"
          }}
          type="submit"
        >
          Login
        </button>
      </form>
    </div>
  );
}
