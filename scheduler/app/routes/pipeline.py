from fastapi import APIRouter, HTTPException, Path, Query, BackgroundTasks
from app.schema.pipeline import PipelineCreate, PipelineUpdate, PipelineDetail
from app.db import db
from app.helpers import run_pipeline, parse_cron
import datetime as dt
from bson import ObjectId
from app.scheduler import scheduler
from typing import Literal, Dict, Any
from pymongo import ASCENDING, DESCENDING
from math import ceil

router = APIRouter()

@router.get("/", response_model=Dict[str, Any])
async def list_pipelines(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    per_page: int = Query(50, ge=1, le=1000, description="Items per page"),
    sort: Literal["asc", "desc"] = Query("desc", description="Sort by updated_at: 'asc' or 'desc'")
):
    # Determine sort direction
    sort_order = ASCENDING if sort == "asc" else DESCENDING

    # Count total documents
    total_records = db.pipelines.count_documents({})
    total_pages = ceil(total_records / per_page) if total_records > 0 else 1

    # Fetch paginated pipelines
    pipeline_docs = list(
        db.pipelines
        .find()
        .sort("updated_at", sort_order)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    # Extract pipeline ObjectIds
    pipeline_ids = [doc["_id"] for doc in pipeline_docs]

    # Aggregate dag_runs per pipeline
    dag_runs = list(
        db.dag_runs.aggregate([
            {"$match": {"pipeline_id": {"$in": pipeline_ids}}},
            {"$sort": {"started_at": -1}},
            {
                "$group": {
                    "_id": "$pipeline_id",
                    "total_runs": {"$sum": 1},
                    "failed_runs": {
                        "$sum": {
                            "$cond": [{"$eq": ["$status", "failed"]}, 1, 0]
                        }
                    },
                    "latest_status": {"$first": "$status"}
                }
            }
        ])
    )

    # Build lookup map
    dag_runs_map = {r["_id"]: r for r in dag_runs}

    # Build response items
    pipelines = []
    for doc in pipeline_docs:
        stats = dag_runs_map.get(doc["_id"], {})
        pipelines.append({
            "id": str(doc["_id"]),
            "name": doc["name"],
            "description": doc.get("description"),
            "schedule": doc["schedule"],
            "enabled": doc["enabled"],
            "created_at": doc["created_at"],
            "updated_at": doc["updated_at"],
            "total_dag_runs": stats.get("total_runs", 0),
            "failed_dag_runs": stats.get("failed_runs", 0),
            "latest_dag_run_status": stats.get("latest_status")
        })

    return {
        "items": pipelines,
        "total_records": total_records,
        "total_pages": total_pages,
        "page": page,
        "per_page": per_page
    }

@router.get("/{pipeline_id}",response_model=PipelineDetail)
async def get_pipeline(pipeline_id: str = Path(...)):
    doc = db.pipelines.find_one({"_id": ObjectId(pipeline_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    record = PipelineDetail(
            id=str(doc["_id"]),
            name=doc["name"],
            description=doc.get("description"),
            schedule=doc["schedule"],
            enabled=doc["enabled"],
            created_at=doc["created_at"],
            updated_at=doc["updated_at"]
        )
    return record

@router.put("/{pipeline_id}")
async def update_pipeline(pipeline_id: str, pipeline_update: PipelineUpdate):
    update_data = {k: v for k, v in pipeline_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    update_data["updated_at"] = dt.datetime.now(dt.timezone.utc)

    result = db.pipelines.update_one(
        {"_id": ObjectId(pipeline_id)},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    try:
        scheduler.remove_job(pipeline_id)
    except:
        pass

    # Handle scheduler update
    doc = db.pipelines.find_one({"_id": ObjectId(pipeline_id)})
    if doc and doc["enabled"]:
        scheduler.add_job(
            func=run_pipeline,
            trigger="cron",
            id=pipeline_id,
            name=doc["name"],
            max_instances=1,
            coalesce=True,  # combine missed runs into one
            misfire_grace_time=300,  # allow 5 minutes grace
            **parse_cron(doc["schedule"]),
            kwargs={"pipeline_id": pipeline_id}
        )

    return {"message": "Pipeline updated"}


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    result = db.pipelines.delete_one({"_id": ObjectId(pipeline_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Remove job from scheduler
    try:
        scheduler.remove_job(pipeline_id)
    except:
        pass

    # Optionally delete tasks and runs
    db.tasks.delete_many({"pipeline_id": ObjectId(pipeline_id)})
    db.dag_runs.delete_many({"pipeline_id": ObjectId(pipeline_id)})

    return {"message": "Pipeline and related data deleted"}


@router.post("/")
async def create_pipeline(pipeline: PipelineCreate):
    now = dt.datetime.now(dt.timezone.utc)
    doc = pipeline.model_dump()
    doc["created_at"] = now
    doc["updated_at"] = now
    result = db.pipelines.insert_one(doc)

    pipeline_id = str(result.inserted_id)

    if pipeline.enabled:
        # Schedule job
        scheduler.add_job(
            func=run_pipeline,
            trigger="cron",
            id=pipeline_id,
            name=pipeline.name,
            max_instances=1,
            coalesce=True,  # combine missed runs into one
            misfire_grace_time=300,  # allow 5 minutes grace
            **parse_cron(pipeline.schedule),
            kwargs={"pipeline_id": pipeline_id}
        )

    return {"id": pipeline_id, "message": "Pipeline created"}

@router.post("/{pipeline_id}/run")
async def run_pipeline_manually(pipeline_id: str, background_tasks: BackgroundTasks):
    """
    Manually trigger a pipeline run immediately.
    """
    # Validate that the pipeline exists
    doc = db.pipelines.find_one({"_id": ObjectId(pipeline_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Call the run_pipeline function (synchronously)
    # In production, you might want to run this in the background
    background_tasks.add_task(run_pipeline, pipeline_id)

    return {"message": "Pipeline manual execution triggered"}