from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from dotenv import load_dotenv
import os
from pymongo import MongoClient
# import logging

# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("apscheduler").setLevel(logging.DEBUG)

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# Separate PyMongo client for APScheduler
sync_client = MongoClient(MONGO_URI)
jobstore = MongoDBJobStore(database=DB_NAME, collection='apscheduler_jobs', client=sync_client)

scheduler = AsyncIOScheduler()
scheduler.add_jobstore(jobstore)
