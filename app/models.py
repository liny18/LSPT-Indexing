# app/models.py
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime


class PingIndexRequest(BaseModel):
    document_id: str
    operation: str  # "add", "update", "delete"
    timestamp: datetime


class SearchOptions(BaseModel):
    proximity: bool
    phrase_match: bool


class SearchRequest(BaseModel):
    terms: List[str]
    options: SearchOptions


class DocumentMetadata(BaseModel):
    document_id: str
    total_terms: int
    metadata: Dict
