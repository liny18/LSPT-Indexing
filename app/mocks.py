# app/mocks.py
from typing import Optional, Dict

# Mock document data
MOCK_DOCUMENTS = {
    "doc123": {
        "_id": "doc123",
        "url": "https://example.com/doc123",
        "text_length": 13,
        "text": "This is a sample document for testing the indexing component.",
        "type": "pdf",
    },
}


def fetch_document_content_mock(document_id: str) -> Optional[str]:
    document = MOCK_DOCUMENTS.get(document_id)
    if document:
        return document["text"]
    return None


def fetch_document_metadata_mock(document_id: str) -> Optional[Dict]:
    document = MOCK_DOCUMENTS.get(document_id)
    if document:
        return {
            "url": document.get("url", ""),
            "type": document.get("type", ""),
            "text_length": document.get(
                "text_length", len(document.get("text", "").split())
            ),
        }
    return None
