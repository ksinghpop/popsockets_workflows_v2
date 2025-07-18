import importlib
from datetime import datetime
import datetime as dt
from bson import ObjectId
from scheduler.app.db import db

def resolve_function(function_name: str):
    """
    Given 'module_name.function_name', import and return the function object.
    Example:
        function_name = "app.my_tasks.my_function"
    """
    import importlib

    module_name, func_name = function_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    return func

def run_pipeline(pipeline_id: str):
    """
    Executes the pipeline tasks in order, respecting dependencies.
    """
    import traceback

    # Create a dag_runs record
    started_at = dt.datetime.now(dt.timezone.utc)
    dag_run = {
        "pipeline_id": ObjectId(pipeline_id),
        "started_at": started_at,
        "status": "running",
        "tasks": []
    }
    result = db.dag_runs.insert_one(dag_run)
    run_id = result.inserted_id

    # Fetch tasks
    tasks = list(db.tasks.find({"pipeline_id": ObjectId(pipeline_id)}))

    # Build dependency map
    task_map = {str(t["_id"]): t for t in tasks}
    completed_tasks = set()

    def can_run(task):
        return all(str(dep) in completed_tasks for dep in task.get("dependencies", []))

    while len(completed_tasks) < len(tasks):
        ran_any = False
        for t_id, task in task_map.items():
            if t_id in completed_tasks:
                continue
            if not can_run(task):
                continue

            # Run this task
            print(f"Running task: {task['name']}")
            task_status = {
                "task_id": ObjectId(t_id),
                "status": "running",
                "started_at": dt.datetime.now(dt.timezone.utc),
                "logs": ""
            }
            try:
                # Dynamically import and run the function
                func = resolve_function(task["function_name"])
                result = func(**task["params"])
                task_status["status"] = "success"
                task_status["logs"] = result['logs']
            except Exception as e:
                task_status["status"] = "failed"
                task_status["logs"] = traceback.format_exc()
                # Mark pipeline as failed
                db.dag_runs.update_one(
                    {"_id": run_id},
                    {"$set": {"status": "failed", "completed_at": dt.datetime.now(dt.timezone.utc)}}
                )
                return

            task_status["completed_at"] = dt.datetime.now(dt.timezone.utc)
            db.dag_runs.update_one(
                {"_id": run_id},
                {"$push": {"tasks": task_status}}
            )

            completed_tasks.add(t_id)
            ran_any = True

        if not ran_any:
            raise Exception("Circular dependency detected or no runnable tasks.")

    # Mark pipeline as successful
    db.dag_runs.update_one(
        {"_id": run_id},
        {"$set": {"status": "success", "completed_at": dt.datetime.now(dt.timezone.utc)}}
    )
