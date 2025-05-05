import json
from pymongo import MongoClient
from google.cloud import storage

# === CONFIG ===
GCS_BUCKET = "sp25-thjaya-yelp-data"
GCS_FILE = "yelp_academic_dataset_business.json"  # file is in root, not in /bronze/
LOCAL_FILE = "/tmp/yelp_academic_dataset_business.json"

MONGO_URI = "mongodb://localhost:27017"
RAW_DB = "yelp"
RAW_COLLECTION = "business"
BRONZE_DB = "bronze_db"
BRONZE_COLLECTION = "business_filtered"

# === STEP 1: GCS → Local VM ===
print("Downloading raw business JSON from GCS...")
client = storage.Client()
bucket = client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_FILE)
blob.download_to_filename(LOCAL_FILE)
print(f"Downloaded to {LOCAL_FILE}")

# === STEP 2: Load Raw Data → MongoDB (yelp.business) ===
mongo_client = MongoClient(MONGO_URI)
raw_col = mongo_client[RAW_DB][RAW_COLLECTION]

if raw_col.estimated_document_count() > 0:
    print("Raw collection exists. Dropping...")
    raw_col.drop()

print("Loading raw data into yelp.business...")
with open(LOCAL_FILE, "r", encoding="utf-8") as f:
    docs = [json.loads(line) for line in f]  # NDJSON

raw_col.insert_many(docs)
print(f"Inserted {len(docs)} records into {RAW_DB}.{RAW_COLLECTION}")

# === STEP 3: Filter Open Businesses → bronze_db.business_filtered ===
bronze_col = mongo_client[BRONZE_DB][BRONZE_COLLECTION]

if bronze_col.estimated_document_count() > 0:
    print("Bronze collection exists. Truncating...")
    bronze_col.delete_many({})

query = {"is_open": 1}
projection = {
    "_id": 0,
    "business_id": 1,
    "name": 1,
    "address": 1,
    "city": 1,
    "state": 1,
    "postal_code": 1,       
    "latitude": 1,
    "longitude": 1,
    "stars": 1,
    "review_count": 1,
    "attributes": 1,
    "categories": 1,
    "hours": 1
}

print("Filtering and writing open businesses to bronze...")
count = 0
for doc in raw_col.find(query, projection):
    bronze_col.insert_one(doc)
    count += 1

print(f"Filtered and inserted {count} open businesses into {BRONZE_DB}.{BRONZE_COLLECTION}")
