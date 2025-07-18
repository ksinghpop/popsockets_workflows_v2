// src/components/UserFormModal.js
import React, { useState, useEffect } from "react";
import { Modal, Button, Form } from "react-bootstrap";

export default function UserFormModal({ show, onHide, onSubmit, initialData, isResetPassword }) {
  const [username, setUsername] = useState("");
  const [role, setRole] = useState("member");
  const [password, setPassword] = useState("");

  useEffect(() => {
    if (initialData) {
      setUsername(initialData.username || "");
      setRole(initialData.role || "member");
    }
    setPassword("");
  }, [initialData]);

  const handleSubmit = () => {
    onSubmit({ username, role, password });
  };

  return (
    <Modal show={show} onHide={onHide} centered>
      <Modal.Header closeButton>
        <Modal.Title>
          {isResetPassword ? "Reset Password" : initialData ? "Edit User" : "Add User"}
        </Modal.Title>
      </Modal.Header>
      <Modal.Body>
        {!isResetPassword && (
          <>
            <Form.Group className="mb-3">
              <Form.Label>Username</Form.Label>
              <Form.Control
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                disabled={!!initialData}
              />
            </Form.Group>
            <Form.Group className="mb-3">
              <Form.Label>Role</Form.Label>
              <Form.Select value={role} onChange={(e) => setRole(e.target.value)}>
                <option value="admin">Admin</option>
                <option value="member">Member</option>
                <option value="viewer">Viewer</option>
              </Form.Select>
            </Form.Group>
          </>
        )}
        <Form.Group className="mb-3">
          <Form.Label>{isResetPassword ? "New Password" : "Password"}</Form.Label>
          <Form.Control
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </Form.Group>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={onHide}>
          Cancel
        </Button>
        <Button variant="primary" onClick={handleSubmit}>
          {isResetPassword ? "Reset Password" : "Save"}
        </Button>
      </Modal.Footer>
    </Modal>
  );
}
