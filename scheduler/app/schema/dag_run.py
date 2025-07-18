from pydantic import BaseModel, Field
from typing import List, Optional, Literal
import datetime as dt

class TaskRun(BaseModel):
    task_id: str
    status: str = Literal["running", "success", "failed"]
    started_at: dt.datetime
    completed_at: Optional[dt.datetime]
    logs: Optional[str]

class DagRunOut(BaseModel):
    id: str = Field(..., description="MongoDB ObjectId as string")
    pipeline_id: str
    started_at: dt.datetime
    completed_at: Optional[dt.datetime]
    status: str
    tasks: List[TaskRun]
