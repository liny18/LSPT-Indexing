# app/services.py
from app.db import (
    forward_index_col,
    inverted_index_col,
    transformed_docs_col,
    doc_stats_col,
)
from app.utils import extract_terms
from pymongo import UpdateOne
from datetime import datetime
import re


def add_document_to_index(document_id: str):
    # Fetch document content and metadata from the Document Data Store
    document_content = fetch_document_content(document_id)
    document_metadata = fetch_document_metadata(document_id)

    if forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document already exists")

    terms = extract_terms(document_content)
    term_info = {}
    for position, term in enumerate(terms):
        term_info.setdefault(term, {"frequency": 0, "positions": []})
        term_info[term]["frequency"] += 1
        term_info[term]["positions"].append(position)

        # Update inverted index
        inverted_index_col.update_one(
            {"term": term},
            {"$set": {f"documents.{document_id}": term_info[term]}},
            upsert=True,
        )

    # Update forward index with metadata
    forward_index_col.insert_one(
        {
            "document_id": document_id,
            "terms": term_info,
            "metadata": document_metadata,
            "total_terms": len(terms),
        }
    )

    # Update doc_stats_col
    doc_stats = doc_stats_col.find_one({})
    if doc_stats:
        new_doc_count = doc_stats.get("docCount", 0) + 1
        new_total_length = doc_stats.get("avgDocLength", 0.0) * doc_stats.get(
            "docCount", 0
        ) + len(terms)
        new_avg_length = new_total_length / new_doc_count if new_doc_count > 0 else 0.0

        doc_stats_col.update_one(
            {},
            {
                "$set": {
                    "docCount": new_doc_count,
                    "avgDocLength": new_avg_length,
                    "last_updated": datetime.utcnow(),
                }
            },
        )
    else:
        # Initialize doc_stats_col if not present
        doc_stats_col.insert_one(
            {
                "docCount": 1,
                "avgDocLength": len(terms),
                "last_updated": datetime.utcnow(),
            }
        )


def update_document_in_index(document_id: str):
    if not forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document does not exist")

    # Fetch existing document's term count for recalculating average
    existing_doc = forward_index_col.find_one({"document_id": document_id})
    existing_total_terms = existing_doc.get("total_terms", 0)

    # Delete existing document from indexes
    delete_document_from_index(document_id, adjust_stats=False)

    # Add updated document
    add_document_to_index(document_id)

    # Adjust doc_stats_col based on the change in document length
    doc_stats = doc_stats_col.find_one({})
    if doc_stats:
        new_doc_count = doc_stats.get("docCount", 0)
        total_length = doc_stats.get("avgDocLength", 0.0) * new_doc_count
        total_length = (
            total_length
            - existing_total_terms
            + forward_index_col.find_one({"document_id": document_id}).get(
                "total_terms", 0
            )
        )
        new_avg_length = total_length / new_doc_count if new_doc_count > 0 else 0.0

        doc_stats_col.update_one(
            {},
            {
                "$set": {
                    "avgDocLength": new_avg_length,
                    "last_updated": datetime.utcnow(),
                }
            },
        )


def delete_document_from_index(document_id: str, adjust_stats: bool = True):
    if not forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document does not exist")

    # Remove from inverted index
    document_terms = forward_index_col.find_one({"document_id": document_id})["terms"]
    bulk_operations = []
    for term in document_terms:
        bulk_operations.append(
            UpdateOne({"term": term}, {"$unset": {f"documents.{document_id}": ""}})
        )
    if bulk_operations:
        inverted_index_col.bulk_write(bulk_operations)

    # Remove term entries with no documents
    inverted_index_col.delete_many({"documents": {"$size": 0}})

    # Fetch total_terms before deletion for recalculating average
    total_terms = forward_index_col.find_one({"document_id": document_id}).get(
        "total_terms", 0
    )

    # Remove from forward index
    forward_index_col.delete_one({"document_id": document_id})

    if adjust_stats:
        # Update doc_stats_col
        doc_stats = doc_stats_col.find_one({})
        if doc_stats:
            new_doc_count = doc_stats.get("docCount", 1) - 1
            if new_doc_count < 0:
                new_doc_count = 0
            total_length = (
                doc_stats.get("avgDocLength", 0.0) * doc_stats.get("docCount", 0)
                - total_terms
            )
            new_avg_length = total_length / new_doc_count if new_doc_count > 0 else 0.0

            doc_stats_col.update_one(
                {},
                {
                    "$set": {
                        "docCount": new_doc_count,
                        "avgDocLength": new_avg_length,
                        "last_updated": datetime.utcnow(),
                    }
                },
            )
        else:
            # If doc_stats_col is missing, initialize it
            doc_stats_col.insert_one(
                {"docCount": 0, "avgDocLength": 0.0, "last_updated": datetime.utcnow()}
            )


def search_documents(terms: list):
    if not terms:
        return {}

    # Retrieve documents for each term
    doc_sets = []
    for term in terms:
        entry = inverted_index_col.find_one({"term": term})
        if entry:
            doc_sets.append(set(entry["documents"].keys()))
        else:
            doc_sets.append(set())

    # Intersection of document sets
    matching_docs = set.intersection(*doc_sets) if doc_sets else set()

    # Compile results
    result = {}
    for doc in matching_docs:
        forward_entry = forward_index_col.find_one({"document_id": doc})
        result[doc] = {"metadata": forward_entry.get("metadata", {}), "terms": {}}
        for term in terms:
            term_data = inverted_index_col.find_one({"term": term})["documents"][doc]
            result[doc]["terms"][term] = term_data
    return result


def get_document_metadata(document_id: str):
    forward_entry = forward_index_col.find_one({"document_id": document_id})
    if not forward_entry:
        return None
    return {
        "document_id": forward_entry["document_id"],
        "total_terms": forward_entry["total_terms"],
        "metadata": forward_entry.get("metadata", {}),
    }


def get_total_doc_statistics():
    doc_stats = doc_stats_col.find_one({})
    if not doc_stats:
        return {"avgDocLength": 0.0, "docCount": 0}
    return {
        "avgDocLength": doc_stats.get("avgDocLength", 0.0),
        "docCount": doc_stats.get("docCount", 0),
    }


# Helper functions
def fetch_document_content(document_id: str) -> str:
    # Fetch from Document Data Store's transformed_documents collection
    doc = transformed_docs_col.find_one({"document_id": document_id})
    if not doc:
        raise ValueError("Transformed document not found")
    return doc.get("content", "")


def fetch_document_metadata(document_id: str) -> dict:
    # Fetch from Document Data Store's transformed_documents collection
    doc = transformed_docs_col.find_one({"document_id": document_id})
    if not doc:
        raise ValueError("Document metadata not found")
    return doc.get("metadata", {})
