# app/services.py
from fastapi import Request
from app.utils import extract_terms
from app.mocks import fetch_document_content_mock, fetch_document_metadata_mock
from pymongo import UpdateOne
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


def get_db(request: Request):
    return request.app.state.db


def add_document_to_index(request: Request, document_id: str):
    db = get_db(request)

    # Fetch document content and metadata
    if db.transformed_docs_col is not None:
        try:
            document = db.transformed_docs_col.find_one({"_id": document_id})
            if not document:
                raise ValueError("Document not found.")
            document_content = document.get("text", "")
            document_metadata = {
                "url": document.get("url", ""),
                "type": document.get("type", ""),
                "text_length": document.get(
                    "text_length", len(document_content.split())
                ),
            }
        except Exception as e:
            logger.error(f"Error fetching document: {e}")
            raise ValueError("Failed to fetch document.")
    else:
        # Handle mock data
        document_content = fetch_document_content_mock(document_id)
        document_metadata = fetch_document_metadata_mock(document_id)
        if not document_content or not document_metadata:
            raise ValueError("Document not found in mock data.")

    if db.forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document already exists.")

    terms = extract_terms(document_content)
    logger.info(f"Extracted terms for document {document_id}: {terms}")

    term_info = {}
    for position, term in enumerate(terms):
        term_info.setdefault(term, {"frequency": 0, "positions": []})
        term_info[term]["frequency"] += 1
        term_info[term]["positions"].append(position)

        # Update inverted index
        db.inverted_index_col.update_one(
            {"term": term},
            {"$set": {f"documents.{document_id}": term_info[term]}},
            upsert=True,
        )
        logger.debug(f"Indexed term '{term}' for document '{document_id}'.")

    # Update forward index
    db.forward_index_col.insert_one(
        {
            "document_id": document_id,
            "terms": term_info,
            "metadata": document_metadata,
            "total_terms": document_metadata.get("text_length", len(terms)),
        }
    )
    logger.info(f"Added document {document_id} to forward index.")

    # Update statistics using text_length
    doc_stats = db.doc_stats_col.find_one({})
    text_length = document_metadata.get("text_length", len(terms))
    if doc_stats:
        new_doc_count = doc_stats.get("docCount", 0) + 1
        new_total_length = (
            doc_stats.get("avgDocLength", 0.0) * doc_stats.get("docCount", 0)
            + text_length
        )
        new_avg_length = new_total_length / new_doc_count if new_doc_count > 0 else 0.0

        db.doc_stats_col.update_one(
            {},
            {
                "$set": {
                    "docCount": new_doc_count,
                    "avgDocLength": new_avg_length,
                    "last_updated": datetime.now(timezone.utc),
                }
            },
        )
        logger.info(
            f"Updated doc_stats_col: docCount={new_doc_count}, avgDocLength={new_avg_length}"
        )
    else:
        # Initialize statistics
        db.doc_stats_col.insert_one(
            {
                "docCount": 1,
                "avgDocLength": text_length,
                "last_updated": datetime.now(timezone.utc),
            }
        )
        logger.info("Initialized doc_stats_col with first document.")

    logger.info(f"Added document {document_id} successfully.")


def update_document_in_index(request: Request, document_id: str):
    db = get_db(request)
    if not db.forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document does not exist.")

    # Fetch existing document's term count for recalculating average
    existing_doc = db.forward_index_col.find_one({"document_id": document_id})
    existing_total_terms = existing_doc.get("total_terms", 0)

    # Delete existing document from indexes without adjusting stats
    delete_document_from_index(request, document_id, adjust_stats=False)

    # Add updated document
    add_document_to_index(request, document_id)

    # Adjust doc_stats_col based on the change in document length
    new_doc = db.forward_index_col.find_one({"document_id": document_id})
    new_total_terms = new_doc.get("total_terms", 0)

    doc_stats = db.doc_stats_col.find_one({})
    if doc_stats:
        total_length = (
            doc_stats.get("avgDocLength", 0.0) * doc_stats.get("docCount", 0)
            - existing_total_terms
            + new_total_terms
        )
        new_avg_length = (
            total_length / doc_stats.get("docCount", 1)
            if doc_stats.get("docCount", 1) > 0
            else 0.0
        )

        db.doc_stats_col.update_one(
            {},
            {
                "$set": {
                    "avgDocLength": new_avg_length,
                    "last_updated": datetime.now(timezone.utc),
                }
            },
        )
        logger.info(f"Adjusted doc_stats_col: avgDocLength={new_avg_length}")
    logger.info(f"Updated document {document_id} successfully.")


def delete_document_from_index(
    request: Request, document_id: str, adjust_stats: bool = True
):
    db = get_db(request)
    if not db.forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document does not exist.")

    # Remove from inverted index
    document_terms = db.forward_index_col.find_one({"document_id": document_id})[
        "terms"
    ]
    bulk_operations = []
    for term in document_terms:
        bulk_operations.append(
            UpdateOne({"term": term}, {"$unset": {f"documents.{document_id}": ""}})
        )
    if bulk_operations:
        db.inverted_index_col.bulk_write(bulk_operations)
        logger.info(f"Removed document {document_id} from inverted index.")

    # Remove term entries with no documents
    db.inverted_index_col.delete_many({"documents": {"$size": 0}})
    logger.debug("Cleaned up inverted index.")

    # Fetch total_terms before deletion for recalculating average
    total_terms = db.forward_index_col.find_one({"document_id": document_id}).get(
        "total_terms", 0
    )

    # Remove from forward index
    db.forward_index_col.delete_one({"document_id": document_id})
    logger.info(f"Removed document {document_id} from forward index.")

    # Update statistics using text_length
    if adjust_stats:
        doc_stats = db.doc_stats_col.find_one({})
        if doc_stats:
            new_doc_count = doc_stats.get("docCount", 1) - 1
            new_doc_count = max(new_doc_count, 0)
            if new_doc_count > 0:
                new_total_length = (
                    doc_stats.get("avgDocLength", 0.0) * doc_stats.get("docCount", 0)
                    - total_terms
                )
                new_avg_length = new_total_length / new_doc_count
            else:
                new_avg_length = 0.0

            db.doc_stats_col.update_one(
                {},
                {
                    "$set": {
                        "docCount": new_doc_count,
                        "avgDocLength": new_avg_length,
                        "last_updated": datetime.now(timezone.utc),
                    }
                },
            )
            logger.info(
                f"Updated doc_stats_col: docCount={new_doc_count}, avgDocLength={new_avg_length}"
            )
        else:
            # Initialize statistics if not present
            db.doc_stats_col.insert_one(
                {
                    "docCount": 0,
                    "avgDocLength": 0.0,
                    "last_updated": datetime.now(timezone.utc),
                }
            )
            logger.info("Initialized doc_stats_col with zero documents.")

    logger.info(f"Deleted document {document_id} successfully.")


def search_documents(request: Request, term: str):
    """
    Search for documents containing the specified term.
    """
    db = get_db(request)
    if not term:
        return {}

    # Retrieve documents for the term
    entry = db.inverted_index_col.find_one({"term": term})
    if entry and "documents" in entry:
        matching_docs = set(entry["documents"].keys())
    else:
        matching_docs = set()

    logger.debug(f"Matching documents for term '{term}': {matching_docs}")

    # Compile results
    result = {}
    for doc in matching_docs:
        forward_entry = db.forward_index_col.find_one({"document_id": doc})
        if forward_entry:
            term_data = entry["documents"].get(doc)
            if term_data:
                result[doc] = {
                    "metadata": forward_entry.get("metadata", {}),
                    "terms": {term: term_data},
                }
    logger.debug(f"Search results: {result}")
    return result


def get_document_metadata(request: Request, document_id: str):
    db = get_db(request)
    forward_entry = db.forward_index_col.find_one({"document_id": document_id})
    if not forward_entry:
        return None
    return {
        "document_id": forward_entry["document_id"],
        "total_terms": forward_entry["total_terms"],
        "metadata": forward_entry.get("metadata", {}),
    }


def get_total_doc_statistics(request: Request):
    db = get_db(request)
    doc_stats = db.doc_stats_col.find_one({})
    if not doc_stats:
        return {"avgDocLength": 0.0, "docCount": 0}
    return {
        "avgDocLength": doc_stats.get("avgDocLength", 0.0),
        "docCount": doc_stats.get("docCount", 0),
    }


# Helper functions remain the same
def fetch_document_content(db, document_id: str) -> str:
    # Fetch from Document Data Store's transformed_documents collection
    doc = db.transformed_docs_col.find_one({"_id": document_id})
    if not doc:
        raise ValueError("Transformed document not found")
    return doc.get("text", "")


def fetch_document_metadata(db, document_id: str) -> dict:
    # Fetch from Document Data Store's transformed_documents collection
    doc = db.transformed_docs_col.find_one({"_id": document_id})
    if not doc:
        raise ValueError("Document metadata not found")
    return {
        "url": doc.get("url", ""),
        "type": doc.get("type", ""),
        "text_length": doc.get("text_length", len(doc.get("text", "").split())),
    }
