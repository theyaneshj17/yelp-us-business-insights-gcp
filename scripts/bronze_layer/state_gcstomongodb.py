import csv
from pymongo import MongoClient
from google.cloud import storage

# === CONFIG ===
GCS_BUCKET = "sp25-thjaya-yelp-data"
GCS_FILE = "data-map-state-abbreviations.csv"
LOCAL_FILE = "/tmp/data-map-state-abbreviations.csv"

MONGO_URI = "mongodb://localhost:27017"
BRONZE_DB = "bronze_db"
STATE_COLLECTION = "state_mapping"

# === STEP 1: Download CSV from GCS ===
print("Downloading state mapping CSV from GCS...")
client = storage.Client()
bucket = client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_FILE)
blob.download_to_filename(LOCAL_FILE)
print(f"Downloaded to {LOCAL_FILE}")

# === STEP 2: Load into MongoDB ===
mongo_client = MongoClient(MONGO_URI)
state_col = mongo_client[BRONZE_DB][STATE_COLLECTION]

if state_col.estimated_document_count() > 0:
    print("State mapping collection exists. Dropping...")
    state_col.drop()

print("Parsing CSV and inserting into MongoDB...")
with open(LOCAL_FILE, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    data = list(reader)

state_col.insert_many(data)
print(f"Inserted {len(data)} state records into {BRONZE_DB}.{STATE_COLLECTION}")
