# app/db.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import logging
from pymongo.errors import ConnectionFailure

load_dotenv()

# Initialize Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class Database:
    def __init__(self):
        self.index_client = None
        self.index_db = None
        self.forward_index_col = None
        self.inverted_index_col = None
        self.doc_stats_col = None

        self.doc_store_client = None
        self.doc_store_db = None
        self.transformed_docs_col = None  # Collection in Document Data Store

    def connect_to_databases(self):
        try:
            # Indexing Component MongoDB Configuration
            INDEX_DB_URI = os.getenv("INDEX_DB_URI")
            INDEX_DATABASE_NAME = os.getenv("INDEX_DATABASE_NAME")

            # Document Data Store MongoDB Configuration
            DOC_STORE_DB_URI = os.getenv("DOC_STORE_DB_URI")
            DOC_STORE_DATABASE_NAME = os.getenv("DOC_STORE_DATABASE_NAME")

            # Connect to Indexing Component MongoDB
            self.index_client = MongoClient(INDEX_DB_URI)
            self.index_db = self.index_client[INDEX_DATABASE_NAME]
            self.forward_index_col = self.index_db["forward_index"]
            self.inverted_index_col = self.index_db["inverted_index"]
            self.doc_stats_col = self.index_db["doc_stats"]

            # Initialize doc_stats_col if empty
            if self.doc_stats_col.count_documents({}) == 0:
                self.doc_stats_col.insert_one(
                    {
                        "docCount": 0,
                        "avgDocLength": 0.0,
                        "last_updated": datetime.now(timezone.utc),
                    }
                )
                logger.info("Initialized doc_stats_col with default values.")
            else:
                logger.info("doc_stats_col already initialized.")

            logger.info("Successfully connected to the Indexing Database.")

        except Exception as e:
            logger.error(f"Error connecting to the Indexing Database: {e}")
            raise e

        # Attempt to connect to Document Data Store

        try:
            self.doc_store_client = MongoClient(
                DOC_STORE_DB_URI, serverSelectionTimeoutMS=5000
            )
            self.doc_store_db = self.doc_store_client[DOC_STORE_DATABASE_NAME]
            self.transformed_docs_col = self.doc_store_db["transformed_documents"]
            # Force connection by pinging the server
            self.doc_store_client.admin.command("ping")
            logger.info("Successfully connected to the Document Data Store Database.")
        except ConnectionFailure as e:
            logger.warning(
                f"Could not connect to the Document Data Store Database: {e}"
            )
            self.transformed_docs_col = None  # Set to None if connection fails

    def close_database_connections(self):
        try:
            if self.index_client:
                self.index_client.close()
            if self.doc_store_client:
                self.doc_store_client.close()
            logger.info("Closed all database connections.")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
            raise e
