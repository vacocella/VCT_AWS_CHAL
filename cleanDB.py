import os
from sqlalchemy import create_engine
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL cleanup
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
with engine.connect() as conn:
    conn.execute("TRUNCATE TABLE players, teams RESTART IDENTITY CASCADE;")
    print("PostgreSQL tables truncated.")

# MongoDB cleanup
MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client['valorantdb']
mongo_db.matches.delete_many({})
mongo_db.maps.delete_many({})
print("MongoDB collections cleared.")

# Close connections
mongo_client.close()
engine.dispose()
