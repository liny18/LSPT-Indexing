# app/api.py
from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
from typing import Dict
from app.services import (
    add_document_to_index,
    update_document_in_index,
    delete_document_from_index,
    search_documents,
    get_document_metadata,
    get_total_doc_statistics,
)
from datetime import datetime
import logging

router = APIRouter()


# Pydantic Models
class PingIndexRequest(BaseModel):
    document_id: str
    operation: str  # "add", "update", "delete"
    timestamp: datetime


class DocumentMetadata(BaseModel):
    document_id: str
    total_terms: int
    metadata: Dict


@router.post("/ping")
async def ping_index(request: Request, ping_request: PingIndexRequest):
    operation = ping_request.operation.lower()
    document_id = ping_request.document_id

    try:
        if operation == "add":
            add_document_to_index(request, document_id)
            op_past = "added"
        elif operation == "update":
            update_document_in_index(request, document_id)
            op_past = "updated"
        elif operation == "delete":
            delete_document_from_index(request, document_id)
            op_past = "deleted"
        else:
            raise HTTPException(status_code=400, detail="Invalid operation type")
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error in ping_index: {e}")
        raise HTTPException(status_code=500, detail="Server error")

    return {"message": f"Document {op_past} successfully"}


@router.get("/search")
async def search_index(
    request: Request, term: str = Query(..., description="Search term")
):
    try:
        results = search_documents(request, term)
        return {"documents": results}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error in search_index: {e}")
        raise HTTPException(status_code=500, detail="Server error")


@router.get("/metadata/{document_id}")
async def metadata(request: Request, document_id: str):
    try:
        metadata = get_document_metadata(request, document_id)
        if not metadata:
            raise HTTPException(status_code=404, detail="Document not found")
        return metadata
    except Exception as e:
        logging.error(f"Error in metadata endpoint: {e}")
        raise HTTPException(status_code=500, detail="Server error")


@router.get("/doc-stats")
async def doc_stats(request: Request):
    try:
        stats = get_total_doc_statistics(request)
        return stats
    except Exception as e:
        logging.error(f"Error in doc_stats endpoint: {e}")
        raise HTTPException(status_code=500, detail="Server error")


# Test Endpoint to Verify Index DB Connection
# @router.get("/test-connection")
# async def test_connection(request: Request):
#     db = request.app.state.db
#     try:
#         # Attempt to list collections in the Indexing Database
#         collections = db.index_db.list_collection_names()
#         return {"status": "success", "collections": collections}
#     except Exception as e:
#         logging.error(f"Connection Test Failed: {e}")
#         raise HTTPException(
#             status_code=500, detail="Failed to connect to Indexing Database"
#         )
