import importlib
import datetime as dt
from bson import ObjectId
from app.db import db
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import io
import sys
import functools
import os
from dotenv import load_dotenv
load_dotenv()

ETL_MODULE_PREFIX = os.environ.get("ETL_MODULE_PREFIX", "popsockets_etl")

def parse_cron(cron_expr):
    """
    Convert cron string '0 6 * * *' to dict:
    {'minute': '0', 'hour': '6', 'day': '*', 'month': '*', 'day_of_week': '*'}
    """
    parts = cron_expr.split()
    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4]
    }

def resolve_function(function_name: str):
    """
    function_name: e.g. tasks.sample_task.extract_data
    """
    # module_name, func_name = function_name.rsplit(".", 1)
    # module_name_prefix = "app.tasks."  # Adjust this if your modules are in a different package
    # module = importlib.import_module(f"{module_name_prefix}{module_name}")
    # func = getattr(module, func_name)
    # return func
    parts = function_name.rsplit(".", 1)
    module_part = parts[0]
    func_name = parts[1]
    module_name = f"{ETL_MODULE_PREFIX}.{module_part}"

    module = importlib.import_module(module_name)
    return getattr(module, func_name)

async def run_pipeline(pipeline_id: str, max_retries: int = int(os.environ.get("DAG_RUN_RETIRES", 3))):
    """
    Executes the pipeline tasks in order, with parallel execution and retries.
    """
    # Create dag_run record
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

    # Prepare task tracking
    task_map = {str(t["_id"]): t for t in tasks}
    completed_tasks = set()
    retries_left = {str(t["_id"]): max_retries for t in tasks}

    def can_run(task):
        return all(str(dep) in completed_tasks for dep in task.get("dependencies", []))

    def run_single_task(task_id: str, task):
        """
        This function runs a single task and returns its status dictionary.
        """
        task_status = {
            "task_id": ObjectId(task_id),
            "status": "running",
            "started_at": dt.datetime.now(dt.timezone.utc),
            "logs": ""
        }
        try:
            func = resolve_function(task["function_name"])
            wrapped_func = capture_logs(func)
            result = wrapped_func(**task["params"])
            task_status["status"] = "success"
            task_status["logs"] = str(result['logs'])
        except Exception:
            task_status["status"] = "failed"
            task_status["logs"] = traceback.format_exc()
        task_status["completed_at"] = dt.datetime.now(dt.timezone.utc)
        return task_status

    # Main execution loop
    with ThreadPoolExecutor() as executor:
        while len(completed_tasks) < len(tasks):
            runnable_tasks = [
                (t_id, task)
                for t_id, task in task_map.items()
                if t_id not in completed_tasks and can_run(task)
            ]

            if not runnable_tasks:
                raise Exception("Circular dependency detected or no runnable tasks.")

            # Submit all runnable tasks in parallel
            futures = {
                executor.submit(run_single_task, t_id, task): t_id
                for t_id, task in runnable_tasks
            }

            for future in as_completed(futures):
                t_id = futures[future]
                task_status = future.result()

                if task_status["status"] == "success":
                    # Mark task completed
                    completed_tasks.add(t_id)
                    db.dag_runs.update_one(
                        {"_id": run_id},
                        {"$push": {"tasks": task_status}}
                    )
                else:
                    # Decrement retry count
                    print("==============================",flush=True)
                    print(f"TID: {t_id}",flush=True)
                    print("==============================",flush=True)
                    retries_left[t_id] -= 1
                    if retries_left[t_id] > 0:
                        print(f"Task {task_map[t_id]['name']} failed. Retrying... Remaining retries: {retries_left[t_id]}")
                    else:
                        # Mark pipeline failed
                        db.dag_runs.update_one(
                            {"_id": run_id},
                            {"$push": {"tasks": task_status},
                             "$set": {"status": "failed", "completed_at": dt.datetime.now(dt.timezone.utc)}}
                        )
                        return  # Exit pipeline

    # Mark pipeline successful
    db.dag_runs.update_one(
        {"_id": run_id},
        {"$set": {"status": "success", "completed_at": dt.datetime.now(dt.timezone.utc)}}
    )


def capture_logs(func):
    """
    Decorator that captures stdout prints and returns them with the result.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        buffer = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = buffer
        try:
            result = func(*args, **kwargs)
            output = buffer.getvalue()
            return {
                "result": result,
                "logs": output
            }
        finally:
            sys.stdout = old_stdout
    return wrapper