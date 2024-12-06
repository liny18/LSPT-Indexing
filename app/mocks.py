# app/mocks.py
from typing import Optional, Dict

# Mock document data
MOCK_DOCUMENTS = {
    "doc123": {
        "content": "This is a sample document for testing the indexing component.",
        "metadata": {
            "title": "Test Document",
            "author": "Tester",
            "date": "2024-11-20",
        },
    },
}


def fetch_document_content_mock(document_id: str) -> Optional[str]:
    document = MOCK_DOCUMENTS.get(document_id)
    if document:
        return document["content"]
    return None


def fetch_document_metadata_mock(document_id: str) -> Optional[Dict]:
    document = MOCK_DOCUMENTS.get(document_id)
    if document:
        return document["metadata"]
    return None
