from dotenv import load_dotenv
import os

load_dotenv()

MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "Analytics"
COLLECTION_NAME = "dau"

