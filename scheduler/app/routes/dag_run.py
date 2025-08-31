from fastapi import APIRouter, HTTPException, Path, Query
from typing import List
from bson import ObjectId
from app.db import db
from app.schema.dag_run import DagRunOut, TaskRun
from typing import Literal, Dict, Any
from pymongo import ASCENDING, DESCENDING
from math import ceil

router = APIRouter()

@router.get("/", response_model=Dict[str, Any])
async def list_dag_runs(
                page: int = Query(1, ge=1, description="Page number (starts from 1)"),
                per_page: int = Query(50, ge=1, le=1000, description="Items per page"),
                sort: Literal["asc", "desc"] = Query("desc", description="Sort by started_at: 'asc' or 'desc'")
):
    # Determine sort direction
    sort_order = ASCENDING if sort == "asc" else DESCENDING

    # Count total documents
    total_records = db.dag_runs.count_documents({})
    total_pages = ceil(total_records / per_page) if total_records > 0 else 1

    runs = []
    for doc in db.dag_runs.find().sort("started_at", sort_order).skip((page - 1) * per_page).limit(per_page):
        runs.append(DagRunOut(
            id=str(doc["_id"]),
            pipeline_id=str(doc["pipeline_id"]),
            started_at=doc["started_at"],
            completed_at=doc.get("completed_at"),
            status=doc["status"],
            tasks=[
                TaskRun(
                    task_id=str(t["task_id"]),
                    status=t["status"],
                    started_at=t["started_at"],
                    completed_at=t.get("completed_at"),
                    logs=t.get("logs")
                )
                for t in doc.get("tasks", [])
            ]
        ))
    return {
            "items": runs,
            "total_records": total_records,
            "total_pages": total_pages,
            "page": page,
            "per_page": per_page
            }

@router.get("/{pipeline_id}", response_model=Dict[str, Any])
async def get_all_dag_runs_by_pipeline(
                pipeline_id: str,
                page: int = Query(1, ge=1, description="Page number (starts from 1)"),
                per_page: int = Query(50, ge=1, le=1000, description="Items per page"),
                sort: Literal["asc", "desc"] = Query("desc", description="Sort by started_at: 'asc' or 'desc'")
                ):
    # Determine sort direction
    sort_order = ASCENDING if sort == "asc" else DESCENDING
    
    # Count total documents
    total_records = db.dag_runs.count_documents({"pipeline_id": ObjectId(pipeline_id)})
    total_pages = ceil(total_records / per_page) if total_records > 0 else 1

    # Query paginated results
    docs = db.dag_runs.find({"pipeline_id": ObjectId(pipeline_id)}).sort("started_at", sort_order).skip((page - 1) * per_page).limit(per_page)
    
    if not docs:
        raise HTTPException(status_code=404, detail="No runs found for this pipeline")
    results = []
    for doc in docs:
        results.append(
            DagRunOut(
                id=str(doc["_id"]),
                pipeline_id=str(doc["pipeline_id"]),
                started_at=doc["started_at"],
                completed_at=doc.get("completed_at"),
                status=doc["status"],
                tasks=[
                    TaskRun(
                        task_id=str(t["task_id"]),
                        status=t["status"],
                        started_at=t["started_at"],
                        completed_at=t.get("completed_at"),
                        logs=t.get("logs")
                    )
                    for t in doc.get("tasks", [])
                ]
            ))
    return {
            "items": results,
            "total_records": total_records,
            "total_pages": total_pages,
            "page": page,
            "per_page": per_page
            }

@router.get("/{pipeline_id}/latest", response_model=DagRunOut)
async def get_latest_dag_run(pipeline_id: str):
    doc = db.dag_runs.find_one(
        {"pipeline_id": ObjectId(pipeline_id)},
        sort=[("started_at", -1)]
    )
    if not doc:
        raise HTTPException(status_code=404, detail="No runs found for this pipeline")
    return DagRunOut(
        id=str(doc["_id"]),
        pipeline_id=str(doc["pipeline_id"]),
        started_at=doc["started_at"],
        completed_at=doc.get("completed_at"),
        status=doc["status"],
        tasks=[
            TaskRun(
                task_id=str(t["task_id"]),
                status=t["status"],
                started_at=t["started_at"],
                completed_at=t.get("completed_at"),
                logs=t.get("logs")
            )
            for t in doc.get("tasks", [])
        ]
    )

@router.get("/{dag_run_id}", response_model=DagRunOut)
async def get_dag_run(dag_run_id: str = Path(..., description="Run ObjectId")):
    doc = db.dag_runs.find_one({"_id": ObjectId(dag_run_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Run not found")
    return DagRunOut(
        id=str(doc["_id"]),
        pipeline_id=str(doc["pipeline_id"]),
        started_at=doc["started_at"],
        completed_at=doc.get("completed_at"),
        status=doc["status"],
        tasks=[
            TaskRun(
                task_id=str(t["task_id"]),
                status=t["status"],
                started_at=t["started_at"],
                completed_at=t.get("completed_at"),
                logs=t.get("logs")
            )
            for t in doc.get("tasks", [])
        ]
    )

@router.delete("/{dag_run_id}")
async def delete_dag_run(dag_run_id: str = Path(..., description="Run ObjectId")):
    result = db.dag_runs.delete_one({"_id": ObjectId(dag_run_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"message": "Dag Run deleted"}


@router.get("/logs/{run_id}/{task_id}")
def get_task_logs(run_id: str, task_id: str):
    # Try both ObjectId and string for compatibility
    try:
        run_oid = ObjectId(run_id)
        task_oid = ObjectId(task_id)
    except Exception:
        run_oid = run_id
        task_oid = task_id
    cursor = db.logs.find({
        "$or": [
            {"run_id": run_oid, "task_id": task_oid},
            {"run_id": run_id, "task_id": task_id}
        ]
    }).sort("timestamp", 1)
    return {"logs": [doc["log"] for doc in cursor]}