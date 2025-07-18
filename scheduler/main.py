from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.scheduler import scheduler
from app.routes.pipeline import router as pipeline_router
from app.routes.tasks import router as tasks_router
from app.routes.dag_run import router as dag_run_router
from app.routes.auth import router as auth_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    scheduler.start()
    yield
    # Shutdown logic
    scheduler.shutdown()

app = FastAPI(title="Python DAG Scheduler", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://45.32.67.201:3000"
        ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(pipeline_router, prefix="/pipelines",tags=["Pipelines"])
app.include_router(tasks_router, prefix="/tasks",tags=["Tasks"])
app.include_router(dag_run_router, prefix="/dag_runs",tags=["DAG Runs"])