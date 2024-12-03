from config import get_db

db = get_db()

# Collections
forward_index = db['forward_index']
reverse_index = db['reverse_index']

def add_to_forward_index(document_id, terms):
    forward_index.update_one(
        {"document_id": document_id},
        {"$set": {"terms": terms}},
        upsert=True
    )

def add_to_reverse_index(terms, document_id):
    for term, details in terms.items():
        reverse_index.update_one(
            {"term": term},
            {"$push": {"documents": {document_id: details}}},
            upsert=True
        )

def get_reverse_index_terms(terms):
    return reverse_index.find({"term": {"$in": terms}})

def get_document_metadata(document_id):
    return forward_index.find_one({"document_id": document_id}, {"_id": 0})
