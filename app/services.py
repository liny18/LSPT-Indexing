# app/services.py
from app.db import (
    forward_index_col,
    inverted_index_col,
    metadata_store_col,
    average_length_col,
    transformed_docs_col,
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

    # Update forward index
    forward_index_col.insert_one({"document_id": document_id, "terms": term_info})

    # Update metadata
    metadata_store_col.insert_one(
        {
            "document_id": document_id,
            "total_terms": len(terms),
            "metadata": document_metadata,
        }
    )

    # Recalculate average document length
    recalculate_average_document_length()


def update_document_in_index(document_id: str):
    if not forward_index_col.find_one({"document_id": document_id}):
        raise ValueError("Document does not exist")

    # Delete existing document from indexes
    delete_document_from_index(document_id)

    # Add updated document
    add_document_to_index(document_id)


def delete_document_from_index(document_id: str):
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

    # Remove from forward index and metadata store
    forward_index_col.delete_one({"document_id": document_id})
    metadata_store_col.delete_one({"document_id": document_id})

    # Recalculate average document length
    recalculate_average_document_length()


def search_documents(terms: list, proximity: bool, phrase_match: bool):
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

    # Apply proximity or phrase match filters
    if proximity or phrase_match:
        filtered_docs = set()
        for doc in matching_docs:
            positions_lists = [
                inverted_index_col.find_one({"term": term})["documents"][doc][
                    "positions"
                ]
                for term in terms
            ]
            if proximity and check_proximity(positions_lists):
                filtered_docs.add(doc)
            elif phrase_match and check_phrase_match(positions_lists):
                filtered_docs.add(doc)
        matching_docs = filtered_docs

    # Compile results
    result = {}
    for doc in matching_docs:
        result[doc] = {}
        for term in terms:
            term_data = inverted_index_col.find_one({"term": term})["documents"][doc]
            result[doc][term] = term_data
    return result


def get_document_metadata(document_id: str):
    metadata = metadata_store_col.find_one({"document_id": document_id})
    if not metadata:
        return None
    return {
        "document_id": metadata["document_id"],
        "total_terms": metadata["total_terms"],
        "metadata": metadata["metadata"],
    }


def get_total_doc_statistics():
    total_docs = metadata_store_col.count_documents({})
    
    avg_doc_length_entry = average_length_col.find_one({})
    avg_doc_length = avg_doc_length_entry["average_length"] if avg_doc_length_entry else 0.0

    return {
        "avgDocLength": avg_doc_length,
        "docCount": total_docs
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


def recalculate_average_document_length():
    total_docs = metadata_store_col.count_documents({})
    if total_docs == 0:
        average = 0.0
    else:
        pipeline = [{"$group": {"_id": None, "total_length": {"$sum": "$total_terms"}}}]
        result = list(metadata_store_col.aggregate(pipeline))
        total_length = result[0]["total_length"] if result else 0
        average = total_length / total_docs
    # Update average document length
    average_length_col.update_one(
        {},
        {"$set": {"average_length": average, "last_updated": datetime.utcnow()}},
        upsert=True,
    )


def check_proximity(positions_lists, max_distance=5):
    """Check if terms appear within a certain proximity in the document."""
    first_positions = positions_lists[0]
    for pos in first_positions:
        if all(
            any(abs(p - pos) <= max_distance for p in positions)
            for positions in positions_lists[1:]
        ):
            return True
    return False


def check_phrase_match(positions_lists):
    """Check if terms appear consecutively as a phrase in the document."""
    first_positions = positions_lists[0]
    for pos in first_positions:
        if all((pos + i) in positions_lists[i] for i in range(1, len(positions_lists))):
            return True
    return False
