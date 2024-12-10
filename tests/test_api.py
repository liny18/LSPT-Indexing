# tests/test_api.py
from fastapi.testclient import TestClient
from app.main import app
from app.db import Database
from app.mocks import fetch_document_content_mock, fetch_document_metadata_mock
import pytest
from datetime import datetime, timezone

client = TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    # Setup: Connect to databases
    db = Database()
    db.connect_to_databases()
    app.state.db = db  # Attach the db instance to app.state for testing

    # Insert a sample transformed document if Document Data Store is available
    if db.transformed_docs_col is not None:
        db.transformed_docs_col.insert_one(
            {
                "document_id": "doc123",
                "content": "This is a sample document for testing the indexing component.",
                "metadata": {
                    "title": "Test Document",
                    "author": "Tester",
                    "date": "2024-11-20",
                },
            }
        )
    else:
        pass  # Since mock data is handled in services.py

    yield

    # Teardown: Remove test data and close connections
    if db.transformed_docs_col is not None:
        db.transformed_docs_col.delete_one({"document_id": "doc123"})
    db.forward_index_col.delete_one({"document_id": "doc123"})
    db.inverted_index_col.delete_many({"documents.doc123": {"$exists": True}})
    db.inverted_index_col.delete_many(
        {"documents": {"$size": 0}}
    )  # Remove entries with no documents
    db.doc_stats_col.update_one(
        {},
        {
            "$set": {
                "docCount": 0,
                "avgDocLength": 0.0,
                "last_updated": datetime.now(timezone.utc),
            }
        },
    )
    db.close_database_connections()


def test_ping_add():
    response = client.post(
        "/index/ping",
        json={
            "document_id": "doc123",
            "operation": "add",
            "timestamp": "2024-11-20T10:00:00Z",
        },
    )
    assert response.status_code == 200
    assert response.json()["message"] == "Document added successfully"

    # Verify in forward_index collection
    forward_entry = app.state.db.forward_index_col.find_one({"document_id": "doc123"})
    assert forward_entry is not None
    assert forward_entry["metadata"]["title"] == "Test Document"

    # Verify doc_stats_col
    doc_stats = app.state.db.doc_stats_col.find_one({})
    assert doc_stats is not None
    assert doc_stats["docCount"] == 1
    assert doc_stats["avgDocLength"] == len(
        "This is a sample document for testing the indexing component.".split()
    )


def test_search():
    response = client.get("/index/search", params={"term": "sample"})
    if response.status_code != 200:
        print("Response Status Code:", response.status_code)
        print("Response JSON:", response.json())
    assert response.status_code == 200
    assert "documents" in response.json()
    assert "doc123" in response.json()["documents"]
    assert (
        "sample" in response.json()["documents"]["doc123"]["terms"]
    ), "Term 'sample' not found in search results."
    assert response.json()["documents"]["doc123"]["terms"]["sample"]["frequency"] == 1
    assert response.json()["documents"]["doc123"]["terms"]["sample"]["positions"] == [3]


def test_inverted_index():
    db = app.state.db
    term = "sample"
    inverted_entry = db.inverted_index_col.find_one({"term": term})
    assert inverted_entry is not None, f"Inverted index missing term: {term}"
    assert "doc123" in inverted_entry.get(
        "documents", {}
    ), f"Document doc123 missing for term: {term}"
    term_data = inverted_entry["documents"]["doc123"]
    assert term_data["frequency"] == 1, f"Incorrect frequency for term: {term}"
    assert term_data["positions"] == [3], f"Incorrect positions for term: {term}"


def test_forward_index():
    db = app.state.db
    document_id = "doc123"
    forward_entry = db.forward_index_col.find_one({"document_id": document_id})
    assert forward_entry is not None, f"Forward index missing document: {document_id}"
    assert forward_entry["metadata"]["title"] == "Test Document"
    assert forward_entry["total_terms"] == 10


def test_ping_delete():
    response = client.post(
        "/index/ping",
        json={
            "document_id": "doc123",
            "operation": "delete",
            "timestamp": "2024-11-20T10:10:00Z",
        },
    )
    assert response.status_code == 200
    assert response.json()["message"] == "Document deleted successfully"

    # Verify removal from forward_index collection
    forward_entry = app.state.db.forward_index_col.find_one({"document_id": "doc123"})
    assert forward_entry is None

    # Verify doc_stats_col
    doc_stats = app.state.db.doc_stats_col.find_one({})
    assert doc_stats is not None
    assert doc_stats["docCount"] == 0
    assert doc_stats["avgDocLength"] == 0.0


# def test_connection():
#     response = client.get("/index/test-connection")
#     assert response.status_code == 200
#     data = response.json()
#     assert data["status"] == "success"
#     assert "collections" in data
#     assert "forward_index" in data["collections"]
#     assert "inverted_index" in data["collections"]
#     assert "doc_stats" in data["collections"]
