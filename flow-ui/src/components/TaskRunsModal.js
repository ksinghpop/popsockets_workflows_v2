import React, { useState } from 'react';
import { Modal, Table } from 'antd';
import TaskLogsModal from './TaskLogsModal';

const TaskRunsModal = ({ visible, onClose, taskRuns, runId }) => {
  const [selectedTaskId, setSelectedTaskId] = useState(null);
  const [logVisible, setLogVisible] = useState(false);

  const handleRowClick = (record) => {
    setSelectedTaskId(record.task_id);
    setLogVisible(true);
  };

  const columns = [
    { title: 'Task Name', dataIndex: 'name', key: 'name' },
    { title: 'Status', dataIndex: 'status', key: 'status' },
    { title: 'Started At', dataIndex: 'started_at', key: 'started_at' },
    { title: 'Completed At', dataIndex: 'completed_at', key: 'completed_at' },
  ];

  return (
    <>
      <Modal
        title="Task Runs"
        visible={visible}
        onCancel={onClose}
        footer={null}
        width={800}
      >
        <Table
          dataSource={taskRuns}
          columns={columns}
          rowKey="task_id"
          onRow={(record) => ({
            onClick: () => handleRowClick(record)
          })}
        />
      </Modal>
      <TaskLogsModal
        visible={logVisible}
        onClose={() => setLogVisible(false)}
        runId={runId}
        taskId={selectedTaskId}
      />
    </>
  );
};

export default TaskRunsModal;