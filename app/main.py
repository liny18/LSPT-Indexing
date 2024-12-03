# app/main.py
from fastapi import FastAPI
from app.api import router
from app.db import connect_to_databases, close_database_connections


async def lifespan(app: FastAPI):
    # Startup: Connect to both databases
    connect_to_databases()
    yield
    # Shutdown: Close database connections
    close_database_connections()


app = FastAPI(
    title="Search Engine Indexing API",
    description="API for managing and querying the search engine indexes.",
    version="1.0.0",
    lifespan=lifespan,
)

# Include API router
app.include_router(router, prefix="/index", tags=["Indexing"])
