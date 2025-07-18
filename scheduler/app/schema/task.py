from pydantic import BaseModel
from typing import Optional, List, Dict
from bson import ObjectId

class TaskBase(BaseModel):
    name: str
    function_name: str
    params: Dict
    dependencies: List[str] = []
    order: int
    pipeline_id: str

class TaskCreate(BaseModel):
    name: str
    function_name: str
    params: Dict
    dependencies: List[str] = []
    order: int

class TaskUpdate(BaseModel):
    name: Optional[str] = None
    function_name: Optional[str] = None
    params: Optional[Dict] = None
    dependencies: Optional[List[str]] = None
    order: Optional[int] = None
    pipeline_id: Optional[str] = None

class TaskDetail(BaseModel):
    id: str
    name: str
    function_name: str
    params: Dict
    dependencies: List[str]
    order: int
    pipeline_id: str
    created_at: str
    updated_at: str