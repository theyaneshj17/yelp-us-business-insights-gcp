import json
from pymongo import MongoClient
from google.cloud import storage

# === CONFIG ===
GCS_BUCKET = "sp25-thjaya-yelp-data"
GCS_FILE = "yelp_academic_dataset_review.json"
LOCAL_FILE = "/tmp/yelp_academic_dataset_review.json"

MONGO_URI = "mongodb://localhost:27017"
RAW_DB = "yelp"
RAW_COLLECTION = "review"
BRONZE_DB = "bronze_db"
BRONZE_COLLECTION = "review_filtered"

# === STEP 1: GCS → Local VM ===
print("Downloading raw review JSON from GCS...")
client = storage.Client()
bucket = client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_FILE)
blob.download_to_filename(LOCAL_FILE)
print(f"Downloaded to {LOCAL_FILE}")

# === STEP 2: Load Raw Data → MongoDB (yelp.review) ===
mongo_client = MongoClient(MONGO_URI)
raw_col = mongo_client[RAW_DB][RAW_COLLECTION]

if raw_col.estimated_document_count() > 0:
    print("Raw review collection exists. Dropping...")
    raw_col.drop()

print("Loading raw review data into yelp.review...")
with open(LOCAL_FILE, "r", encoding="utf-8") as f:
    batch = []
    for i, line in enumerate(f):
        batch.append(json.loads(line))
        if len(batch) == 10000:
            raw_col.insert_many(batch)
            batch.clear()
    if batch:
        raw_col.insert_many(batch)
print(f"Inserted all review records into {RAW_DB}.{RAW_COLLECTION}")

# === STEP 3: Filter reviews > 2020 → bronze_db.review_filtered ===
bronze_col = mongo_client[BRONZE_DB][BRONZE_COLLECTION]

if bronze_col.estimated_document_count() > 0:
    print("Bronze review collection exists. Truncating...")
    bronze_col.delete_many({})

query = {"date": {"$gt": "2020-01-01"}}
projection = {
    "_id": 0,
    "review_id": 1,
    "user_id": 1,
    "business_id": 1,
    "stars": 1,
    "date": 1,
    "text": 1,
    "useful": 1,
    "funny": 1,
    "cool": 1
}

print("Filtering reviews after 2020-01-01 and writing to bronze (batched insert)...")
cursor = raw_col.find(query, projection)
batch = []
count = 0
for doc in cursor:
    batch.append(doc)
    if len(batch) == 10000:
        bronze_col.insert_many(batch)
        count += len(batch)
        batch.clear()
if batch:
    bronze_col.insert_many(batch)
    count += len(batch)

print(f"Filtered and inserted {count} reviews into {BRONZE_DB}.{BRONZE_COLLECTION}")
