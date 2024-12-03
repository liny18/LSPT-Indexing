# app/db.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Indexing Component MongoDB Configuration
INDEX_DB_URI = os.getenv("INDEX_DB_URI")
INDEX_DATABASE_NAME = os.getenv("INDEX_DATABASE_NAME")

# Document Data Store MongoDB Configuration
DOC_STORE_DB_URI = os.getenv("DOC_STORE_DB_URI")
DOC_STORE_DATABASE_NAME = os.getenv("DOC_STORE_DATABASE_NAME")

# Global Variables for MongoDB Clients and Databases
index_client = None
index_db = None
forward_index_col = None
inverted_index_col = None
doc_stats_col = None

doc_store_client = None
doc_store_db = None
transformed_docs_col = None


def connect_to_databases():
    global index_client, index_db
    global forward_index_col, inverted_index_col, doc_stats_col
    global doc_store_client, doc_store_db, transformed_docs_col

    # Connect to Indexing Component MongoDB
    index_client = MongoClient(INDEX_DB_URI)
    index_db = index_client[INDEX_DATABASE_NAME]
    forward_index_col = index_db["forward_index"]
    inverted_index_col = index_db["inverted_index"]
    doc_stats_col = index_db["total_doc_stats"]

    # Connect to Document Data Store MongoDB
    doc_store_client = MongoClient(DOC_STORE_DB_URI)
    doc_store_db = doc_store_client[DOC_STORE_DATABASE_NAME]
    transformed_docs_col = doc_store_db["transformed_documents"]


def close_database_connections():
    if index_client:
        index_client.close()
    if doc_store_client:
        doc_store_client.close()
