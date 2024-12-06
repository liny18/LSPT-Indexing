# app/main.py
from fastapi import FastAPI
from app.api import router
from app.db import Database
import logging
import contextlib

# Initialize Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create an instance of the Database class
db = Database()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to databases
    try:
        logger.info("Connecting to databases...")
        db.connect_to_databases()
        app.state.db = db  # Attach the db instance to app.state for global access
        logger.info("Connected to databases.")
    except Exception as e:
        logger.error(f"Failed to connect to databases on startup: {e}")
        raise e
    try:
        yield
    finally:
        # Shutdown: Close database connections
        try:
            logger.info("Closing database connections...")
            db.close_database_connections()
            logger.info("Database connections closed.")
        except Exception as e:
            logger.error(f"Failed to close database connections on shutdown: {e}")


app = FastAPI(
    title="Search Engine Indexing API",
    version="1.0.0",
    lifespan=lifespan,
)

# Include API router
app.include_router(router, prefix="/index", tags=["Indexing"])
