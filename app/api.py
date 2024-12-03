# app/api.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from app.services import (
    add_document_to_index,
    update_document_in_index,
    delete_document_from_index,
    search_documents,
    get_document_metadata,
    get_average_document_length,
)
from datetime import datetime

router = APIRouter()


# Pydantic Models
class PingIndexRequest(BaseModel):
    document_id: str
    operation: str  # "add", "update", "delete"
    timestamp: datetime


@router.post("/ping")
def ping_index(request: PingIndexRequest):
    operation = request.operation.lower()
    document_id = request.document_id

    try:
        if operation == "add":
            add_document_to_index(document_id)
        elif operation == "update":
            update_document_in_index(document_id)
        elif operation == "delete":
            delete_document_from_index(document_id)
        else:
            raise HTTPException(status_code=400, detail="Invalid operation type")
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Server error")

    return {"message": f"Document {operation}d successfully"}


@router.get("/search")
def search_index(terms: List[str], proximity: bool = False, phrase_match: bool = False):
    results = search_documents(terms, proximity, phrase_match)
    return {"documents": results}


@router.get("/metadata/{document_id}")
def metadata(document_id: str):
    metadata = get_document_metadata(document_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Document not found")
    return metadata


@router.get("/average-document-length")
def average_length():
    avg_length = get_average_document_length()
    return {"average_length": avg_length}
