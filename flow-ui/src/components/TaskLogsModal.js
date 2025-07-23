import React, { useEffect, useState } from 'react';
import { Modal, Button, Typography } from 'antd';
import axios from 'axios';

const { Paragraph } = Typography;

const TaskLogsModal = ({ visible, onClose, runId, taskId }) => {
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    let interval;
    const fetchLogs = async () => {
      if (!runId || !taskId) return;
      try {
        const response = await axios.get(`/dag_runs/logs/${runId}/${taskId}`);
        setLogs(response.data.logs);
      } catch (err) {
        setLogs(prev => [...prev, '⚠️ Failed to load logs']);
      }
    };

    if (visible) {
      fetchLogs(); // initial
      interval = setInterval(fetchLogs, 5000); // poll every 5s
    }

    return () => clearInterval(interval);
  }, [visible, runId, taskId]);

  return (
    <Modal
      title={`Logs for Task ${taskId}`}
      visible={visible}
      onCancel={onClose}
      footer={<Button onClick={onClose}>Close</Button>}
      width={800}
    >
      <div style={{ maxHeight: '60vh', overflowY: 'scroll', backgroundColor: '#111', color: '#0f0', padding: 10 }}>
        {logs.length > 0 ? (
          logs.map((line, idx) => <Paragraph key={idx} style={{ marginBottom: 0 }}>{line}</Paragraph>)
        ) : (
          <Paragraph type="secondary">Waiting for logs...</Paragraph>
        )}
      </div>
    </Modal>
  );
};

export default TaskLogsModal;