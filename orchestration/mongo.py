from json import load
from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "stockmarket_db"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def load_reddit(limit=5000):
    cursor = db.reddit_features_15m.find().sort("timestamp", -1).limit(limit)
    return pd.DataFrame(list(cursor))


def load_stock(limit=5000):
    cursor = db.stock_features.find().sort("timestamp", -1).limit(limit)
    return pd.DataFrame(list(cursor))
