# populate_indexes.py
import os
from pymongo import MongoClient
from datetime import datetime, timezone
import logging
import re
from typing import List, Dict

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample Documents
sample_documents = [
    {
        "_id": "doc001",
        "url": "https://example.com/doc001",
        "text_length": 9,
        "text": "FastAPI makes building APIs easy and fun.",
        "type": "html",
    },
    {
        "_id": "doc002",
        "url": "https://example.com/doc002",
        "text_length": 11,
        "text": "MongoDB is a powerful NoSQL database for modern applications.",
        "type": "pdf",
    },
    {
        "_id": "doc003",
        "url": "https://example.com/doc003",
        "text_length": 8,
        "text": "Python is a versatile programming language.",
        "type": "txt",
    },
]


def extract_terms(text: str) -> List[str]:
    """Tokenizes and normalizes the input text."""
    tokens = re.findall(r"\b\w+\b", text.lower())
    return tokens


def populate_forward_index(db, document):
    document_id = document["_id"]
    terms = extract_terms(document["text"])
    term_info = {}
    for position, term in enumerate(terms):
        term_info.setdefault(term, {"frequency": 0, "positions": []})
        term_info[term]["frequency"] += 1
        term_info[term]["positions"].append(position)

    forward_entry = {
        "document_id": document_id,
        "terms": term_info,
        "metadata": {
            "url": document.get("url", ""),
            "type": document.get("type", ""),
            "text_length": document.get("text_length", len(terms)),
        },
        "total_terms": document.get("text_length", len(terms)),
    }

    db.forward_index_col.insert_one(forward_entry)
    logger.info(f"Inserted into forward_index: {document_id}")


def populate_inverted_index(db, document):
    document_id = document["_id"]
    terms = extract_terms(document["text"])
    for position, term in enumerate(terms):
        # Fetch existing document data for the term
        existing_term = db.inverted_index_col.find_one({"term": term})
        if existing_term and "documents" in existing_term:
            existing_doc_data = existing_term["documents"].get(document_id, {})
            new_frequency = existing_doc_data.get("frequency", 0) + 1
            new_positions = existing_doc_data.get("positions", []) + [position]
        else:
            new_frequency = 1
            new_positions = [position]

        # Update inverted index
        db.inverted_index_col.update_one(
            {"term": term},
            {
                "$set": {
                    f"documents.{document_id}": {
                        "frequency": new_frequency,
                        "positions": new_positions,
                    }
                }
            },
            upsert=True,
        )
    logger.info(f"Updated inverted_index for document: {document_id}")


def update_statistics(db, document):
    text_length = document.get("text_length", len(extract_terms(document["text"])))
    doc_stats = db.doc_stats_col.find_one({})

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
    else:
        # Initialize statistics
        db.doc_stats_col.insert_one(
            {
                "docCount": 1,
                "avgDocLength": text_length,
                "last_updated": datetime.now(timezone.utc),
            }
        )
    logger.info("Updated doc_stats_col")


def main():
    # MongoDB Configuration
    MONGO_URI = os.getenv("INDEX_DB_URI", "mongodb://localhost:27017/")
    INDEX_DB_NAME = os.getenv("INDEX_DATABASE_NAME", "indexing_db")

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[INDEX_DB_NAME]

    # Ensure collections exist and reference the correct variables
    # These variables should match those defined in the Database class
    # forward_index_col, inverted_index_col, doc_stats_col
    forward_index_col = db.forward_index
    inverted_index_col = db.inverted_index
    doc_stats_col = db.doc_stats

    # Assign the collection variables to match the Database class
    db.forward_index_col = forward_index_col
    db.inverted_index_col = inverted_index_col
    db.doc_stats_col = doc_stats_col

    # Clear existing data (optional)
    forward_index_col.delete_many({})
    inverted_index_col.delete_many({})
    doc_stats_col.delete_many({})
    logger.info("Cleared existing data in index collections.")

    # Populate Indexes with Sample Documents
    for doc in sample_documents:
        populate_forward_index(db, doc)
        populate_inverted_index(db, doc)
        update_statistics(db, doc)

    logger.info("Completed populating index collections.")

    # Close the connection
    client.close()


if __name__ == "__main__":
    main()
