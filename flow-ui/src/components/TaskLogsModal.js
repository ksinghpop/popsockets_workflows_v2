import React from "react";
import Modal from "react-modal";

export default function TaskLogsModal({ isOpen, onRequestClose, task }) {
  return (
    <Modal
      isOpen={isOpen}
      onRequestClose={onRequestClose}
      contentLabel="Task Logs"
      style={{
        content: {
          top: "50%",
          left: "50%",
          right: "auto",
          bottom: "auto",
          marginRight: "-50%",
          transform: "translate(-50%, -50%)",
          maxHeight: "70vh",
          width: "500px"
        }
      }}
    >
      <h3>{task?.name}</h3>
      <pre
        style={{
          background: "#f5f5f5",
          padding: "1rem",
          border: "1px solid #ddd",
          maxHeight: "50vh",
          overflowY: "auto"
        }}
      >
        {task?.logs}
      </pre>
      <button
        onClick={onRequestClose}
        className="btn btn-primary mt-3"
      >
        Close
      </button>
    </Modal>
  );
}
