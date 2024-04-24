from pymongo import MongoClient

def createConnection():
    path = "mongodb://localhost:27017/"
    mongo_client = MongoClient(path)
    return mongo_client