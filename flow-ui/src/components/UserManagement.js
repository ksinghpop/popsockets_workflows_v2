// src/components/UserManagement.js
import React, { useEffect, useState } from "react";
import { toast } from "react-toastify";
import Swal from "sweetalert2";
import Modal from "react-modal";

Modal.setAppElement("#root");

export default function UserManagement({ user }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState("add"); // add | edit | reset
  const [selectedUser, setSelectedUser] = useState(null);
  const [formData, setFormData] = useState({
    username: "",
    role: "member",
    password: "",
  });

  const fetchUsers = () => {
    setLoading(true);
    fetch(`${process.env.REACT_APP_API_BASE_URL}/auth/users`, {
      credentials: "include",
    })
      .then((res) => res.json())
      .then(setUsers)
      .catch((err) => console.error("Fetch error:", err))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const openModal = (mode, userItem = null) => {
    setModalMode(mode);
    setSelectedUser(userItem);
    if (mode === "edit") {
      setFormData({
        username: userItem.username,
        role: userItem.role,
        password: "",
      });
    } else {
      setFormData({
        username: "",
        role: "member",
        password: "",
      });
    }
    setModalOpen(true);
  };

  const handleDelete = async (userId) => {
    const result = await Swal.fire({
      title: "Delete User?",
      text: "This action cannot be undone.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#d33",
      cancelButtonColor: "#6c757d",
      confirmButtonText: "Yes, delete",
    });

    if (!result.isConfirmed) return;

    try {
      const res = await fetch(
        `${process.env.REACT_APP_API_BASE_URL}/auth/users/${userId}`,
        {
          method: "DELETE",
          credentials: "include",
        }
      );
      if (res.ok) {
        toast.success("User deleted.");
        fetchUsers();
      } else {
        toast.error("Failed to delete user.");
      }
    } catch (err) {
      console.error("Delete error:", err);
      toast.error("Error deleting user.");
    }
  };

  const handleModalSubmit = async (e) => {
    e.preventDefault();

    try {
      if (modalMode === "add") {
        const res = await fetch(
          `${process.env.REACT_APP_API_BASE_URL}/auth/register`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "include",
            body: JSON.stringify({
              username: formData.username,
              password: formData.password,
              role: formData.role,
            }),
          }
        );
        if (res.ok) {
          toast.success("User created.");
          setModalOpen(false);
          fetchUsers();
        } else {
          toast.error("Failed to create user.");
        }
      }

      if (modalMode === "edit") {
        const res = await fetch(
          `${process.env.REACT_APP_API_BASE_URL}/auth/users/${selectedUser._id}`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            credentials: "include",
            body: JSON.stringify({
              username: formData.username,
              role: formData.role,
            }),
          }
        );
        if (res.ok) {
          toast.success("User updated.");
          setModalOpen(false);
          fetchUsers();
        } else {
          toast.error("Failed to update user.");
        }
      }

      if (modalMode === "reset") {
        const res = await fetch(
          `${process.env.REACT_APP_API_BASE_URL}/auth/users/update_password`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "include",
            body: JSON.stringify({
              user_id: selectedUser._id,
              new_password: formData.password,
            }),
          }
        );
        if (res.ok) {
          toast.success("Password reset.");
          setModalOpen(false);
        } else {
          toast.error("Failed to reset password.");
        }
      }
    } catch (err) {
      console.error("Submit error:", err);
      toast.error("Error saving changes.");
    }
  };

  if (loading) return <p>Loading users...</p>;

  return (
    <div className="mt-3">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h2 className="mb-0">User Management</h2>
        <button
          className="btn btn-success"
          onClick={() => openModal("add")}
        >
          + Add New User
        </button>
      </div>
      <table className="table table-bordered table-striped">
        <thead className="table-dark">
          <tr>
            <th>Username</th>
            <th>Role</th>
            <th className="text-center">Actions</th>
          </tr>
        </thead>
        <tbody>
          {users.length > 0 ? (
            users.map((u) => (
              <tr key={u._id}>
                <td>{u.username}</td>
                <td>{u.role}</td>
                <td className="text-center">
                  <button
                    className="btn btn-sm btn-primary me-2"
                    onClick={() => openModal("edit", u)}
                  >
                    Edit
                  </button>
                  <button
                    className="btn btn-sm btn-warning me-2"
                    onClick={() => openModal("reset", u)}
                  >
                    Reset Password
                  </button>
                  <button
                    className="btn btn-sm btn-danger"
                    onClick={() => handleDelete(u._id)}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="3" className="text-center text-muted">
                No users found.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <Modal
        isOpen={modalOpen}
        onRequestClose={() => setModalOpen(false)}
        contentLabel="User Form"
        style={{
          content: {
            top: "50%",
            left: "50%",
            right: "auto",
            bottom: "auto",
            marginRight: "-50%",
            transform: "translate(-50%, -50%)",
            width: "400px",
          },
        }}
      >
        <h5>
          {modalMode === "add" && "Add User"}
          {modalMode === "edit" && "Edit User"}
          {modalMode === "reset" && "Reset Password"}
        </h5>
        <form onSubmit={handleModalSubmit}>
          {(modalMode === "add" || modalMode === "edit") && (
            <>
              <div className="mb-2">
                <label className="form-label">Username</label>
                <input
                  className="form-control"
                  value={formData.username}
                  onChange={(e) =>
                    setFormData({ ...formData, username: e.target.value })
                  }
                  required
                />
              </div>
              <div className="mb-2">
                <label className="form-label">Role</label>
                <select
                  className="form-select"
                  value={formData.role}
                  onChange={(e) =>
                    setFormData({ ...formData, role: e.target.value })
                  }
                >
                  <option value="admin">Admin</option>
                  <option value="member">Member</option>
                  <option value="viewer">Viewer</option>
                </select>
              </div>
            </>
          )}
          {(modalMode === "add" || modalMode === "reset") && (
            <div className="mb-2">
              <label className="form-label">Password</label>
              <input
                type="password"
                className="form-control"
                value={formData.password}
                onChange={(e) =>
                  setFormData({ ...formData, password: e.target.value })
                }
                required
              />
            </div>
          )}
          <div className="d-flex justify-content-between mt-3">
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setModalOpen(false)}
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
