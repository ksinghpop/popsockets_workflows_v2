from fastapi import APIRouter, HTTPException, Path
from app.db import db
from bson import ObjectId
from datetime import datetime, timezone
from app.schema.task import TaskCreate, TaskUpdate, TaskDetail

router = APIRouter()

@router.post("/create/{pipeline_id}")
async def create_task(pipeline_id: str, task: TaskCreate):
    now = datetime.now(timezone.utc)
    doc = task.model_dump()
    doc["pipeline_id"] = ObjectId(pipeline_id)
    if "dependencies" in doc:
        doc["dependencies"] = [ObjectId(dep) for dep in doc["dependencies"]]
    doc["created_at"] = now
    doc["updated_at"] = now

    result = db.tasks.insert_one(doc)
    return {"id": str(result.inserted_id), "message": "Task created"}

@router.get("/list/{pipeline_id}",response_model=list[TaskDetail])
async def list_tasks(pipeline_id: str):
    tasks = []
    for doc in db.tasks.find({"pipeline_id": ObjectId(pipeline_id)}):
        dependencies_str_list = [str(dep) for dep in doc.get("dependencies", [])]
        tasks.append(
            TaskDetail(
                id=str(doc["_id"]),
                name=doc["name"],
                function_name=doc["function_name"],
                params=doc["params"],
                dependencies=dependencies_str_list,
                order=doc["order"],
                pipeline_id=str(doc["pipeline_id"]),
                created_at=doc["created_at"].isoformat(),
                updated_at=doc["updated_at"].isoformat()
            )
        )
    return tasks

@router.get("/{task_id}")
async def get_task(task_id: str = Path(...)):
    doc = db.tasks.find_one({"_id": ObjectId(task_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Task not found")
    dependencies_str_list = [str(dep) for dep in doc.get("dependencies", [])]
    data = TaskDetail(
                id=str(doc["_id"]),
                name=doc["name"],
                function_name=doc["function_name"],
                params=doc["params"],
                dependencies=dependencies_str_list,
                order=doc["order"],
                pipeline_id=str(doc["pipeline_id"]),
                created_at=doc["created_at"].isoformat(),
                updated_at=doc["updated_at"].isoformat()
            )
    return data

@router.put("/{task_id}")
async def update_task(task_id: str, task_update: TaskUpdate):
    update_data = {k: v for k, v in task_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    if "pipeline_id" in update_data:
        update_data["pipeline_id"] = ObjectId(update_data["pipeline_id"])

    if "dependencies" in update_data:
        update_data["dependencies"] = [ObjectId(dep) for dep in update_data["dependencies"]]
    
    
    update_data["updated_at"] = datetime.now(timezone.utc)

    result = db.tasks.update_one(
        {"_id": ObjectId(task_id)},
        {"$set": update_data}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")

    return {"message": "Task updated"}

@router.delete("/{task_id}")
async def delete_task(task_id: str):
    result = db.tasks.delete_one({"_id": ObjectId(task_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task deleted"}
