from .models import add_to_forward_index, add_to_reverse_index, get_reverse_index_terms, get_document_metadata

def process_ping(document_id, operation, timestamp, terms=None):
    if operation == "add" or operation == "update":
        add_to_forward_index(document_id, terms)
        add_to_reverse_index(terms, document_id)
    elif operation == "delete":
        forward_index.delete_one({"document_id": document_id})
        reverse_index.update_many({}, {"$pull": {"documents": {"$exists": document_id}}})

def search_terms(terms):
    results = get_reverse_index_terms(terms)
    return results

def fetch_metadata(document_id):
    return get_document_metadata(document_id)