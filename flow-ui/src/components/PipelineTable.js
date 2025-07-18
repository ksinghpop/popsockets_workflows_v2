import React from "react";
import PipelineRow from "./PipelineRow";

function PipelineTable({ pipelines, onRefresh, user }) {
  return (
    <table className="table table-bordered table-striped mt-3">
      <thead className="table-dark">
        <tr>
          <th></th>
          <th>Name</th>
          <th className="text-center">Schedule</th>
          <th className="text-center">Latest Dag Run Status</th>
          <th className="text-center">Total Dag Runs</th>
          <th className="text-center">Failed Runs</th>
          <th className="text-center">Actions</th>
        </tr>
      </thead>
      <tbody>
        {pipelines.map((pipeline) => (
          <PipelineRow
            key={pipeline.id}
            pipeline={pipeline}
            onRefresh={onRefresh}
            user={user}
          />
        ))}
      </tbody>
    </table>
  );
}

export default PipelineTable;
