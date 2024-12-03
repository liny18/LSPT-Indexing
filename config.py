from pymongo import MongoClient

class Config:
    MONGO_URI = "mongodb://localhost:27017/indexDB"

def get_db():
    client = MongoClient(Config.MONGO_URI)
    db = client['indexDB']
    return db