from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime

class PipelineBase(BaseModel):
    name: str
    description: Optional[str] = None
    schedule: str
    enabled: bool = True

class PipelineCreate(PipelineBase):
    pass

class Pipeline(PipelineBase):
    id: str
    created_at: datetime
    updated_at: datetime

class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    schedule: Optional[str] = None
    enabled: Optional[bool] = None

class PipelineDetail(BaseModel):
    id: str
    name: str
    description: Optional[str]
    schedule: str
    enabled: bool
    created_at: datetime
    updated_at: datetime